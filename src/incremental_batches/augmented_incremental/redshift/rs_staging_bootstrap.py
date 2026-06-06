"""
rs_staging_bootstrap.py — self-bootstrapping Redshift staging schema.

Pure Python module (NOT a notebook). Mirrors `bq_staging_bootstrap.py` and
`sf_staging_bootstrap.py`. Entry point is called by `setup_rs.py` within the
same notebook execution (NOT via dbutils.notebook.run — see
[[feedback-no-dbutils-notebook-run]]):

  ensure_staging_environment(conn, *, database, target_schema, src_catalog,
                              src_schema, parquet_root, volume_root,
                              s3_volume_prefix, iam_role, aws_region,
                              spark, dbutils, secret_scope, parallel=4) -> dict
    Idempotent. Checks Redshift `{database}.{target_schema}` exists with all
    22 expected staging tables. If anything is missing, seeds only the
    missing ones in parallel via Delta → parquet (UC external volume on S3)
    → COPY into Redshift → row-count parity check.

Per-table layout (DISTKEY / SORTKEY / DISTSTYLE) declared at CREATE time —
mirrors the strategy in setup_rs.py's per-run CTAS layouts (PORT_NOTES.md
"DISTKEY / SORTKEY strategy" section).

Type mapping: Spark's `DataType.simpleString()` keys (bigint/smallint/
tinyint/int/etc.) NOT internal Python names — Redshift COPY FROM PARQUET
requires exact type match.
"""
from __future__ import annotations

import concurrent.futures as _cf
import time as _time

# Canonical 22 staging tables (same set as bq_staging_bootstrap.STAGING_TABLES,
# sorted biggest-first so the ThreadPoolExecutor kicks off long-pole tables first).
STAGING_TABLES: tuple[str, ...] = (
    "bronzedailymarket", "factmarkethistory",
    "factwatches",
    "dimtrade",
    "factholdings", "factcashbalances",
    "cashtransactionhistorical",
    "financial", "companyyeareps",
    "dimaccount", "dimcustomer",
    "currentaccountbalances", "dimbroker",
    "dimsecurity", "dimcompany",
    "dimtime", "dimdate",
    "taxrate", "industry", "tradetype",
    "statustype", "batchdate",
)

# Per-table Redshift layout. Spec format: (distribution_spec, sortkey_cols).
# distribution_spec: "ALL" | "EVEN" | "KEY(col)"
# Keep in sync with setup_rs.py's TABLE_LAYOUTS — both point at the same
# physical tables (staging is the CTAS source for the per-run schema).
TABLE_LAYOUTS: dict[str, tuple[str, tuple[str, ...]]] = {
    "factmarkethistory":          ("KEY(sk_securityid)", ("sk_dateid", "sk_securityid", "sk_companyid")),
    "factwatches":                ("KEY(sk_customerid)", ("sk_dateid_dateremoved", "sk_customerid", "sk_securityid")),
    "factholdings":               ("KEY(sk_customerid)", ("sk_dateid", "sk_customerid", "sk_securityid")),
    "factcashbalances":           ("KEY(sk_customerid)", ("sk_dateid", "sk_customerid")),
    "dimtrade":                   ("KEY(sk_securityid)", ("sk_closedateid", "sk_brokerid", "sk_securityid")),
    "dimcustomer":                ("KEY(customerid)",    ("enddate", "customerid")),
    "dimaccount":                 ("KEY(accountid)",     ("enddate", "accountid")),
    "currentaccountbalances":     ("ALL",                ("customerid",)),
    "dimbroker":                  ("ALL",                ("brokerid",)),
    "dimsecurity":                ("ALL",                ("symbol",)),
    "dimcompany":                 ("ALL",                ("companyid",)),
    "dimdate":                    ("ALL",                ("sk_dateid",)),
    "dimtime":                    ("ALL",                ("sk_timeid",)),
    "taxrate":                    ("ALL",                ()),
    "industry":                   ("ALL",                ()),
    "tradetype":                  ("ALL",                ()),
    "statustype":                 ("ALL",                ()),
    "batchdate":                  ("ALL",                ()),
    "bronzedailymarket":          ("KEY(dm_s_symb)",     ("dm_date",)),
    "financial":                  ("KEY(sk_companyid)",  ("fi_year", "fi_qtr")),
    "companyyeareps":             ("KEY(sk_companyid)",  ("fi_year",)),
    "cashtransactionhistorical":  ("KEY(accountid)",     ("event_dt", "ct_dts")),
}
_missing_layouts = set(STAGING_TABLES) - set(TABLE_LAYOUTS)
assert not _missing_layouts, f"TABLE_LAYOUTS missing: {_missing_layouts}"


# Redshift COPY FROM PARQUET requires EXACT type match. Map keys MUST match
# Spark `DataType.simpleString()` output (SQL-flavored: bigint/int/smallint/
# tinyint), NOT internal Python type names.
_TYPE_MAP: dict[str, str] = {
    "boolean":    "BOOLEAN",
    "tinyint":    "SMALLINT",            # Redshift has no TINYINT
    "smallint":   "SMALLINT",
    "int":        "INTEGER",
    "bigint":     "BIGINT",
    "float":      "REAL",                # parquet FLOAT (32-bit) → REAL/FLOAT4
    "double":     "DOUBLE PRECISION",    # parquet DOUBLE (64-bit) → FLOAT8
    "date":       "DATE",
    "timestamp":  "TIMESTAMP",
    "binary":     "VARBYTE",
}


def _dist_clause(spec: str) -> str:
    if spec == "ALL":  return "DISTSTYLE ALL"
    if spec == "EVEN": return "DISTSTYLE EVEN"
    if spec.startswith("KEY("):
        col = spec[len("KEY("):-1]
        # `DISTKEY(col)` alone implies `DISTSTYLE KEY` — cleaner than
        # writing both. Writing `DISTSTYLE KEY KEY(col)` is a syntax error.
        return f"DISTKEY({col})"
    raise ValueError(f"unknown distribution_spec: {spec}")


def _ddl_for(table: str, target_schema: str, schema_fields) -> str:
    """Build CREATE TABLE DDL from Spark schema + per-table layout."""
    dist_spec, sortkey_cols = TABLE_LAYOUTS[table]
    cols = []
    for f in schema_fields:
        t = f.dataType.simpleString()
        if t.startswith("decimal"):
            cols.append(f'"{f.name}" {t.upper()}')
        elif t == "string":
            cols.append(f'"{f.name}" VARCHAR(MAX)')
        else:
            cols.append(f'"{f.name}" {_TYPE_MAP.get(t, "VARCHAR(MAX)")}')
    cols_sql = ",\n  ".join(cols)
    sort_sql = f"SORTKEY({', '.join(sortkey_cols)})" if sortkey_cols else ""
    return f'''
        CREATE TABLE "{target_schema}"."{table}" (
          {cols_sql}
        )
        {_dist_clause(dist_spec)}
        {sort_sql}
    '''.strip()


def _seed_one(table: str, *, database: str, target_schema: str,
              src_catalog: str, src_schema: str,
              parquet_root: str, volume_root: str, s3_volume_prefix: str,
              iam_role: str, aws_region: str,
              spark, dbutils, secret_scope: str) -> dict:
    """Seed one staging table: Delta → parquet → COPY → row-count check.

    Each call opens its own psycopg2 connection (psycopg2 connections are
    NOT thread-safe to share, so per-thread is the rule). Raises on any
    failure — caller catches and aggregates.

    NOTE: do NOT do relative imports here. This module is loaded via
    sys.path insertion (not as a package), so `from . import X` fails
    immediately with "attempted relative import with no known parent
    package". Use absolute imports only.
    """
    import psycopg2

    def _get(key, default=None):
        try:
            return dbutils.secrets.get(scope=secret_scope, key=key)
        except Exception:
            return default

    log = [f"[{table}] starting"]
    t0 = _time.time()
    src_fq = f"{src_catalog}.{src_schema}.{table}"
    pq_path = f"{parquet_root}/{table}"

    # Defensive wipe — guard against stale parquet from a cancelled prior run.
    try: dbutils.fs.rm(pq_path, recurse=True)
    except Exception: pass

    df = spark.read.table(src_fq)
    delta_rows = df.count()
    log.append(f"[{table}] delta_rows={delta_rows:,}")
    df.write.mode("overwrite").parquet(pq_path)

    # Spark on UC volumes leaves `_committed_*` and `_SUCCESS` markers
    # alongside part files. Redshift COPY treats them as parquet and fails
    # with "Spectrum Scan Error / invalid version number". Defense in depth:
    #   (1) delete the markers, (2) use `/part-` COPY URI prefix so any
    #   leftover markers can't match.
    try:
        for entry in dbutils.fs.ls(pq_path):
            if entry.name.rstrip("/").startswith("_"):
                dbutils.fs.rm(entry.path, recurse=True)
    except Exception as _e:
        log.append(f"[{table}] WARN: marker cleanup failed: {_e}")

    s3_uri = pq_path.replace(volume_root, s3_volume_prefix.rstrip("/") + "/") + "/part-"

    conn = psycopg2.connect(
        host=_get("host"), port=int(_get("port", "5439")),
        user=_get("user"), password=_get("password"),
        dbname=database, sslmode="require", connect_timeout=30,
    )
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{target_schema}"."{table}"')
            cur.execute(_ddl_for(table, target_schema, df.schema.fields))
            cur.execute(f"""
                COPY "{target_schema}"."{table}"
                FROM '{s3_uri}'
                IAM_ROLE '{iam_role}'
                FORMAT AS PARQUET
                REGION '{aws_region}'
            """)
            cur.execute(f'SELECT COUNT(*) FROM "{target_schema}"."{table}"')
            rs_rows = cur.fetchone()[0]
    finally:
        conn.close()

    if rs_rows != delta_rows:
        raise RuntimeError(
            f"row-count mismatch for {table}: delta={delta_rows:,}, redshift={rs_rows:,}"
        )
    elapsed = _time.time() - t0
    log.append(f"[{table}] done in {elapsed:.1f}s — {rs_rows:,} rows  OK")
    return {"table": table, "delta_rows": delta_rows, "rs_rows": rs_rows,
            "elapsed": elapsed, "log": "\n".join(log)}


def ensure_staging_environment(conn, *,
                                database: str,
                                target_schema: str,
                                src_catalog: str,
                                src_schema: str,
                                parquet_root: str,
                                volume_root: str,
                                s3_volume_prefix: str,
                                iam_role: str,
                                aws_region: str,
                                spark,
                                dbutils,
                                secret_scope: str,
                                parallel: int = 4) -> dict:
    """Idempotent Redshift staging bootstrap. Mirrors
    `bq_staging_bootstrap.ensure_staging_environment` and
    `sf_staging_bootstrap.ensure_staging_environment`.

    Args:
        conn: live psycopg2 connection (autocommit OK either way).
        database: Redshift database name (e.g. "dev").
        target_schema: per-SF staging schema (e.g. "tpcdi_staging_sf10").
        src_catalog / src_schema: Databricks-side source
          (e.g. "main" / "tpcdi_incremental_staging_{sf}").
        parquet_root: UC volume path for the per-table parquet staging step.
        volume_root: UC volume root used for s3_uri rewrite.
        s3_volume_prefix: s3:// prefix that maps 1:1 to volume_root.
        iam_role: IAM role ARN attached to the workgroup, for COPY.
        aws_region: COPY's REGION clause.
        spark: SparkSession.
        dbutils: notebook dbutils for fs.ls/fs.rm/secrets.
        secret_scope: scope name to load psycopg2 creds in worker threads.
        parallel: ThreadPoolExecutor max_workers (default 4).

    Returns:
        dict: {skipped: bool, n_seeded: int, elapsed_s: float, missing: list}.

    Raises:
        RuntimeError if any seeded table fails to load with matching row counts.
    """
    # 1) Ensure schema exists.
    with conn.cursor() as cur:
        cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{target_schema}"')

    # 2) Check what's already present.
    with conn.cursor() as cur:
        cur.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = %s",
            (target_schema,),
        )
        present = {r[0].lower() for r in cur.fetchall()}
    missing = [t for t in STAGING_TABLES if t.lower() not in present]

    if not missing:
        msg = f"[bootstrap] {database}.{target_schema} already has all {len(STAGING_TABLES)} staging tables — skipping"
        print(msg)
        return {"skipped": True, "n_seeded": 0, "elapsed_s": 0.0, "missing": []}

    # 3) Seed only the missing tables in parallel.
    print(f"[bootstrap] seeding {len(missing)} of {len(STAGING_TABLES)} staging tables: {missing}")
    t_start = _time.time()
    results: list[dict] = []
    failures: list[tuple[str, str]] = []

    with _cf.ThreadPoolExecutor(max_workers=parallel) as ex:
        futures = {
            ex.submit(
                _seed_one, t,
                database=database,
                target_schema=target_schema,
                src_catalog=src_catalog,
                src_schema=src_schema,
                parquet_root=parquet_root,
                volume_root=volume_root,
                s3_volume_prefix=s3_volume_prefix,
                iam_role=iam_role,
                aws_region=aws_region,
                spark=spark,
                dbutils=dbutils,
                secret_scope=secret_scope,
            ): t for t in missing
        }
        for fut in _cf.as_completed(futures):
            t = futures[fut]
            try:
                r = fut.result()
                print(r["log"])
                results.append(r)
            except Exception as e:
                msg = f"[{t}] [FAIL] {type(e).__name__}: {e}"
                print(msg)
                failures.append((t, str(e)))

    elapsed_s = _time.time() - t_start
    print(f"\n[bootstrap] seeded {len(results)} tables, {len(failures)} failures in {elapsed_s:.1f}s")

    # 4) Re-verify after seed.
    with conn.cursor() as cur:
        cur.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = %s",
            (target_schema,),
        )
        present_after = {r[0].lower() for r in cur.fetchall()}
    still_missing = [t for t in STAGING_TABLES if t.lower() not in present_after]
    if still_missing or failures:
        raise RuntimeError(
            f"Seed incomplete. Failures: {failures}. Still missing: {still_missing}"
        )

    return {"skipped": False, "n_seeded": len(results),
            "elapsed_s": elapsed_s, "missing": missing}
