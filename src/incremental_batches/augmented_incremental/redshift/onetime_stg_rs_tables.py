# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "psycopg2-binary",
# ]
# ///
# One-time, idempotent Redshift staging bootstrap. Loads
# `main.tpcdi_incremental_staging_{sf}` (Databricks Delta) → S3 parquet →
# Redshift `tpcdi_staging_sf{sf}` schema with DISTKEY/SORTKEY declared at
# CREATE time. Mirrors `bq_staging_bootstrap.py` but writes to Redshift
# instead of BigQuery.
#
# Run once per scale_factor. Self-skips when all 22 staging tables already
# exist with non-zero rowcount. Idempotent. Failures raise.
#
# Architecture decisions documented in
# `incremental_batches/augmented_incremental/redshift/PORT_NOTES.md`
# (see "Per-batch staging clone (carries over)" and "No-clone workaround"
# sections).

# COMMAND ----------

dbutils.widgets.text("database",        "dev", "Redshift database")
dbutils.widgets.dropdown("scale_factor","10",  ["10","100","1000","5000","10000","20000"])
dbutils.widgets.text("src_catalog",     "main", "Databricks UC catalog with Delta staging")
dbutils.widgets.text("secret_scope",    "tpcdi_redshift")
dbutils.widgets.text("tpcdi_directory", "/Volumes/main/tpcdi_raw_data/tpcdi_volume/")
dbutils.widgets.text("s3_volume_prefix","s3://tpcds-datasets/shannon_tpcdi/",
                     "S3 prefix matching the UC volume backing")
dbutils.widgets.text("aws_region",      "us-west-2")
dbutils.widgets.text("parallel",        "6", "How many tables to load concurrently")

database        = dbutils.widgets.get("database")
scale_factor    = dbutils.widgets.get("scale_factor")
src_catalog     = dbutils.widgets.get("src_catalog")
secret_scope    = dbutils.widgets.get("secret_scope")
tpcdi_directory = dbutils.widgets.get("tpcdi_directory").rstrip("/") + "/"
s3_prefix       = dbutils.widgets.get("s3_volume_prefix").rstrip("/")
aws_region      = dbutils.widgets.get("aws_region")
parallel        = int(dbutils.widgets.get("parallel"))

src_schema     = f"tpcdi_incremental_staging_{scale_factor}"
target_schema  = f"tpcdi_staging_sf{scale_factor}".lower()
parquet_root   = f"{tpcdi_directory}staging_parquet_rs/sf={scale_factor}"
print(f"src     = {src_catalog}.{src_schema}")
print(f"target  = {database}.{target_schema}")
print(f"parquet = {parquet_root}")

# COMMAND ----------

# MAGIC %run ./_rs_conn

# COMMAND ----------

# Canonical 22-table list — must match STAGING_TABLES in bq_staging_bootstrap.py.
# Sorted biggest-first so the ThreadPoolExecutor kicks off the long-pole
# tables first.
STAGING_TABLES = (
    "bronzedailymarket", "factmarkethistory",        # ~5.35B rows
    "factwatches",                                    # ~2.85B
    "dimtrade",                                       # ~2.10B
    "factholdings", "factcashbalances",               # ~1.94B
    "cashtransactionhistorical",                      # ~1.94B
    "financial", "companyyeareps",                    # ~950M
    "dimaccount", "dimcustomer",                      # 102M, 39M
    "currentaccountbalances", "dimbroker",            # ~30M
    "dimsecurity", "dimcompany",                      # 16M, 10M
    "dimtime", "dimdate",                             # 86K, 26K
    "taxrate", "industry", "tradetype",               # <500
    "statustype", "batchdate",                        # tiny
)

# Redshift-specific layout per table. Same strategy as setup_rs.py's
# TABLE_LAYOUTS — staging tables match the layouts the per-run CTAS will
# reuse. Keep these in sync with setup_rs.py.
TABLE_LAYOUTS = {
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
_missing = set(STAGING_TABLES) - set(TABLE_LAYOUTS)
assert not _missing, f"TABLE_LAYOUTS missing: {_missing}"

# COMMAND ----------

# 1. CREATE SCHEMA IF NOT EXISTS (idempotent).
conn = rs_connect(database=database, secret_scope=secret_scope,
                  query_group={"task": "onetime_stg_rs_tables", "scale_factor": scale_factor})
iam_role = rs_iam_role(secret_scope=secret_scope)
with conn.cursor() as cur:
    cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{target_schema}"')
    # Check which tables already exist + have rows
    cur.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = %s",
        (target_schema,),
    )
    present = {r[0].lower() for r in cur.fetchall()}
missing = [t for t in STAGING_TABLES if t.lower() not in present]
if not missing:
    print(f"[skip] all 22 staging tables already present in {database}.{target_schema}")
    conn.close()
    dbutils.notebook.exit("skipped — already bootstrapped")

print(f"[bootstrap] seeding {len(missing)} of {len(STAGING_TABLES)} tables: {missing}")

# COMMAND ----------

# 2. For each missing table: Spark read Delta → write to parquet under UC volume
#    → Redshift COPY parquet → row count check.

import concurrent.futures as _cf
import time as _time


def _dist_clause(spec: str) -> str:
    if spec == "ALL":  return "DISTSTYLE ALL"
    if spec == "EVEN": return "DISTSTYLE EVEN"
    if spec.startswith("KEY("): return f"DISTSTYLE KEY {spec}"
    raise ValueError(f"unknown distribution_spec: {spec}")


def _create_table_ddl(table: str, sample_df) -> str:
    """Generate a Redshift CREATE TABLE DDL from a Spark DataFrame schema +
    the per-table layout. Type mapping mirrors what dbt-redshift uses."""
    dist_spec, sortkey_cols = TABLE_LAYOUTS[table]
    # IMPORTANT: Redshift COPY FROM PARQUET requires EXACT type match between
    # the target column and the parquet schema. Map keys MUST match
    # Spark's `DataType.simpleString()` output, which uses SQL-flavored names
    # (bigint/smallint/tinyint/int), NOT the Python type names
    # (long/short/byte/integer). Mismatch → all int/long columns silently
    # fall through to VARCHAR(MAX) → COPY fails on parquet schema mismatch.
    type_map = {
        "boolean":    "BOOLEAN",
        "tinyint":    "SMALLINT",            # Redshift has no TINYINT — SMALLINT is the smallest int
        "smallint":   "SMALLINT",
        "int":        "INTEGER",
        "bigint":     "BIGINT",
        "float":      "REAL",                # parquet FLOAT (32-bit) → Redshift REAL/FLOAT4
        "double":     "DOUBLE PRECISION",    # parquet DOUBLE (64-bit) → Redshift DOUBLE PRECISION/FLOAT8
        "date":       "DATE",
        "timestamp":  "TIMESTAMP",
        "binary":     "VARBYTE",
    }
    cols = []
    for f in sample_df.schema.fields:
        t = f.dataType.simpleString()
        if t.startswith("decimal"):
            cols.append(f'"{f.name}" {t.upper()}')
        elif t == "string":
            cols.append(f'"{f.name}" VARCHAR(MAX)')
        else:
            cols.append(f'"{f.name}" {type_map.get(t, "VARCHAR(MAX)")}')
    cols_sql = ", ".join(cols)
    sort_sql = f'SORTKEY({", ".join(sortkey_cols)})' if sortkey_cols else ""
    return f'''
        CREATE TABLE "{target_schema}"."{table}" (
            {cols_sql}
        )
        {_dist_clause(dist_spec)}
        {sort_sql}
    '''.strip()


def _seed_one(table: str) -> tuple[str, dict]:
    log: list[str] = [f"[{table}] starting"]
    t0 = _time.time()
    src_fq = f"{src_catalog}.{src_schema}.{table}"
    pq_path = f"{parquet_root}/{table}"

    # Defensive wipe — guard against stale parquet from a cancelled prior run
    try: dbutils.fs.rm(pq_path, recurse=True)
    except Exception: pass

    df = spark.read.table(src_fq)
    delta_rows = df.count()
    log.append(f"[{table}] delta_rows={delta_rows:,}, writing parquet")
    df.write.mode("overwrite").parquet(pq_path)

    # Databricks's Spark writer leaves `_committed_*` and `_SUCCESS` markers
    # in the output dir. Redshift COPY treats the source URI as a prefix and
    # tries to read ALL files matching it — the metadata markers cause a
    # Spectrum Scan Error ("invalid version number"). Two defenses:
    #   1. Delete the marker files after write so the dir holds only parquet
    #   2. Use a COPY URI ending in `/part-` so even if a marker survives,
    #      Redshift only matches part-NNNN-*.parquet files
    try:
        for entry in dbutils.fs.ls(pq_path):
            n = entry.name.rstrip("/")
            if n.startswith("_"):
                dbutils.fs.rm(entry.path, recurse=True)
    except Exception as _e:
        log.append(f"[{table}] WARN: cleanup of _* markers failed: {_e}")

    # Redshift COPY URI uses /part- prefix to be defensive about any
    # post-cleanup leftover non-parquet artifacts.
    s3_uri = pq_path.replace(tpcdi_directory, s3_prefix.rstrip("/") + "/") + "/part-"

    # Per-thread connection (psycopg2 not safe for concurrent cursors).
    local_conn = rs_connect(database=database, secret_scope=secret_scope,
                            query_group={"task": "onetime_stg_rs_tables",
                                         "table": table, "scale_factor": scale_factor})
    try:
        with local_conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{target_schema}"."{table}"')
            cur.execute(_create_table_ddl(table, df))
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
        local_conn.close()

    if rs_rows != delta_rows:
        raise RuntimeError(
            f"[{table}] row-count mismatch: delta={delta_rows:,} redshift={rs_rows:,}"
        )
    elapsed = _time.time() - t0
    log.append(f"[{table}] OK rs_rows={rs_rows:,} elapsed={elapsed:.1f}s")
    return (table, {"rows": rs_rows, "elapsed_s": elapsed, "log": log})


t_start = _time.time()
results: list[tuple[str, dict]] = []
failures: list[tuple[str, str]] = []
with _cf.ThreadPoolExecutor(max_workers=parallel) as ex:
    futures = {ex.submit(_seed_one, t): t for t in missing}
    for f in _cf.as_completed(futures):
        try:
            name, info = f.result()
            print("\n".join(info["log"]))
            results.append((name, info))
        except Exception as e:
            name = futures[f]
            print(f"[FAIL] {name}: {type(e).__name__}: {e}")
            failures.append((name, str(e)))

print()
print(f"[done] seeded {len(results)}/{len(missing)} tables in {_time.time()-t_start:.1f}s")
if failures:
    print(f"[FAIL] {len(failures)} table(s) failed: {failures}")
    raise RuntimeError(f"onetime_stg_rs_tables: {len(failures)} table(s) failed")

# COMMAND ----------

conn.close()
print("[done] Redshift staging bootstrap complete.")
