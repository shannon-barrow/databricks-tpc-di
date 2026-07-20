"""Self-bootstrapping Redshift staging schema (pure Python module, imported
by setup_rs.py — mirrors bq_/sf_staging_bootstrap.py).

`ensure_staging_environment(...)` is idempotent: it seeds only the staging
tables that are missing or empty, each via Delta -> parquet (UC external
volume on S3) -> COPY -> row-count parity check.

Tables are sorted biggest-first so the thread pool starts the long-pole
loads first. Layouts (DISTKEY/SORTKEY) match setup_rs.py's TABLE_LAYOUTS.
"""
from __future__ import annotations

import concurrent.futures as _cf
import time as _time

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

# (distribution_spec, sortkey_cols) per table. Keep in sync with setup_rs.py's
# TABLE_LAYOUTS — staging is the CTAS source for the per-run schema.
TABLE_LAYOUTS: dict[str, tuple[str, tuple[str, ...]]] = {
    "factmarkethistory":          ("KEY(sk_securityid)", ("sk_dateid", "sk_securityid", "sk_companyid")),
    "factwatches":                ("KEY(sk_customerid)", ("sk_dateid_dateremoved", "sk_customerid", "sk_securityid")),
    "factholdings":               ("KEY(sk_customerid)", ("sk_dateid", "sk_customerid", "sk_securityid")),
    "factcashbalances":           ("KEY(sk_customerid)", ("sk_dateid", "sk_customerid")),
    "dimtrade":                   ("KEY(sk_securityid)", ("sk_closedateid", "sk_brokerid", "sk_securityid")),
    "dimcustomer":                ("KEY(customerid)",    ("enddate", "customerid")),
    "dimaccount":                 ("KEY(accountid)",     ("enddate", "accountid")),
    "currentaccountbalances":     ("ALL",                ("accountid",)),
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
    "companyyeareps":             ("KEY(sk_companyid)",  ("qtr_start_date",)),
    "cashtransactionhistorical":  ("KEY(accountid)",     ("event_dt", "ct_dts")),
}
_missing_layouts = set(STAGING_TABLES) - set(TABLE_LAYOUTS)
assert not _missing_layouts, f"TABLE_LAYOUTS missing: {_missing_layouts}"


# COPY FROM PARQUET needs an exact type match, so keys are Spark
# DataType.simpleString() values (SQL-flavored), not Python type names.
_TYPE_MAP: dict[str, str] = {
    "boolean":    "BOOLEAN",
    "tinyint":    "SMALLINT",            # Redshift has no TINYINT
    "smallint":   "SMALLINT",
    "int":        "INTEGER",
    "bigint":     "BIGINT",
    "float":      "REAL",                # parquet FLOAT (32-bit) -> REAL
    "double":     "DOUBLE PRECISION",    # parquet DOUBLE (64-bit) -> FLOAT8
    "date":       "DATE",
    "timestamp":  "TIMESTAMP",
    "binary":     "VARBYTE",
}


def _dist_clause(spec: str) -> str:
    if spec == "ALL":  return "DISTSTYLE ALL"
    if spec == "EVEN": return "DISTSTYLE EVEN"
    if spec.startswith("KEY("):
        col = spec[len("KEY("):-1]
        return f"DISTKEY({col})"  # implies DISTSTYLE KEY
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
    """Seed one staging table: Delta -> parquet -> COPY -> row-count check.
    Opens its own psycopg2 connection (they aren't thread-safe to share).
    Raises on failure; the caller aggregates.
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

    # Spark leaves `_committed_*` / `_SUCCESS` markers next to the part files;
    # COPY would try to read them as parquet and fail. Delete them, and point
    # COPY at the `/part-` prefix so any stragglers can't match.
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
        keepalives=1, keepalives_idle=30,
        keepalives_interval=10, keepalives_count=3,
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
            # Stamp the row count in a TABLE COMMENT so a later run can detect
            # a partial/empty seed without re-reading Delta.
            cur.execute(
                f"COMMENT ON TABLE \"{target_schema}\".\"{table}\" "
                f"IS 'tpcdi_staging_seed: rows={rs_rows}'"
            )
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
                                parallel: int = 8) -> dict:
    """Idempotent Redshift staging bootstrap (mirrors the bq_/sf_ equivalents).

    Args:
        conn: live psycopg2 connection.
        database: Redshift database name (e.g. "dev").
        target_schema: per-SF staging schema (e.g. "tpcdi_staging_sf10").
        src_catalog / src_schema: Databricks source (catalog / schema).
        parquet_root: UC volume path for the per-table parquet staging step.
        volume_root: UC volume root, rewritten to s3_volume_prefix for COPY.
        s3_volume_prefix: s3:// prefix that maps 1:1 to volume_root.
        iam_role: IAM role ARN for COPY.
        aws_region: COPY REGION clause.
        spark / dbutils: notebook handles.
        secret_scope: scope for psycopg2 creds in worker threads.
        parallel: thread-pool size.

    Returns:
        {skipped, n_seeded, elapsed_s, missing}.

    Raises:
        RuntimeError if any seeded table's row count doesn't match its source.
    """
    with conn.cursor() as cur:
        cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{target_schema}"')

    with conn.cursor() as cur:
        cur.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = %s",
            (target_schema,),
        )
        present = {r[0].lower() for r in cur.fetchall()}
    # A table counts as seeded only if its actual row count matches the count
    # _seed_one stamped in its TABLE COMMENT — existence alone isn't enough,
    # since a cancelled run can leave a CREATE'd-but-never-COPY'd empty shell.
    # Tables without the comment marker (legacy seeds) fall back to `> 0`.
    import re as _re
    nonempty = set()
    with conn.cursor() as cur:
        for t in [s for s in STAGING_TABLES if s.lower() in present]:
            cur.execute(
                "SELECT obj_description(c.oid) "
                "FROM pg_class c JOIN pg_namespace n ON c.relnamespace=n.oid "
                "WHERE n.nspname=%s AND c.relname=%s",
                (target_schema, t.lower()),
            )
            row = cur.fetchone()
            comment = row[0] if row else None
            expected = None
            if comment:
                m = _re.search(r"tpcdi_staging_seed: rows=(\d+)", comment)
                if m:
                    expected = int(m.group(1))
            cur.execute(f'SELECT COUNT(*) FROM "{target_schema}"."{t}"')
            actual = cur.fetchone()[0]
            if expected is not None:
                if actual == expected and actual > 0:
                    nonempty.add(t.lower())
                else:
                    print(f"[bootstrap] {t}: count mismatch (actual={actual:,}, expected={expected:,}) — will re-seed")
            else:
                # Legacy seed without comment marker — fall back to >0.
                if actual > 0:
                    nonempty.add(t.lower())
                else:
                    print(f"[bootstrap] {t}: present but ZERO rows — will re-seed")
    missing = [t for t in STAGING_TABLES if t.lower() not in nonempty]

    if not missing:
        msg = f"[bootstrap] {database}.{target_schema} already has all {len(STAGING_TABLES)} staging tables — skipping"
        print(msg)
        return {"skipped": True, "n_seeded": 0, "elapsed_s": 0.0, "missing": []}

    # Seed only the missing tables, in parallel.
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

    # Re-verify on a fresh connection: the caller's `conn` has been idle for
    # the whole (up to ~1hr) seed and Redshift Serverless would have dropped
    # its SSL socket by now.
    import psycopg2 as _psy
    def _get_secret(k, default=None):
        try: return dbutils.secrets.get(scope=secret_scope, key=k)
        except Exception: return default
    verify_conn = _psy.connect(
        host=_get_secret("host"),
        port=int(_get_secret("port", "5439")),
        user=_get_secret("user"),
        password=_get_secret("password"),
        dbname=database, sslmode="require", connect_timeout=30,
        keepalives=1, keepalives_idle=30,
        keepalives_interval=10, keepalives_count=3,
    )
    verify_conn.autocommit = True
    try:
        with verify_conn.cursor() as cur:
            cur.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = %s",
                (target_schema,),
            )
            present_after = {r[0].lower() for r in cur.fetchall()}
    finally:
        verify_conn.close()
    still_missing = [t for t in STAGING_TABLES if t.lower() not in present_after]
    if still_missing or failures:
        raise RuntimeError(
            f"Seed incomplete. Failures: {failures}. Still missing: {still_missing}"
        )

    return {"skipped": False, "n_seeded": len(results),
            "elapsed_s": elapsed_s, "missing": missing}
