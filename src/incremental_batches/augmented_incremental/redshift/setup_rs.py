# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "psycopg2-binary",
# ]
# ///
# Per-run Redshift setup. Dispatches DDL/CTAS to Redshift from a Databricks
# task. No Spark compute needed beyond the JDBC client.
#
# Sequence:
#   1. DROP+CREATE the per-run Redshift schema {database}.{wh_db}_{sf}
#   2. CTAS 22 historical/reference tables from {database}.tpcdi_staging_sf{sf}
#      (zero-copy clone isn't available in Redshift; CTAS is the lightweight
#      alternative, ~1-3 min at SF=20k for the 22 tables together)
#   3. Pre-create the 7 empty bronze + account_updates_from_customer target
#      tables that dbt populates per batch (via COPY in load_bronze_rs.py)
#   4. Emit batch_date_ls task value for the parent's for_each loop
#
# Self-bootstrapping: if {database}.tpcdi_staging_sf{sf} doesn't exist yet
# for this scale factor, this notebook imports `rs_staging_bootstrap` and
# seeds it inline (Delta → parquet → Redshift COPY). Idempotent — no-op
# when all 22 staging tables are already present. No separate workflow
# task; mirrors setup_bq.py's bootstrap pattern.
#
# Auth: reads connection creds from `tpcdi_redshift` secret scope (see _rs_conn).

# COMMAND ----------

dbutils.widgets.text("database",        "dev", "Redshift database (default 'dev')")
dbutils.widgets.text("wh_db",           "", "wh_db prefix; final schema = {wh_db}_{scale_factor}")
dbutils.widgets.dropdown("scale_factor","10", ["10","100","1000","5000","10000","20000"])
dbutils.widgets.text("secret_scope",    "tpcdi_redshift", "Databricks secret scope")
dbutils.widgets.text("incremental_batches_to_run", "365",
                     "Number of batches the for_each loop runs")
dbutils.widgets.text("databricks_catalog", "main",
                     "Databricks UC catalog where tpcdi_incremental_staging_{sf} lives")
dbutils.widgets.text("tpcdi_directory", "/Volumes/main/tpcdi_raw_data/tpcdi_volume/",
                     "UC external volume root")
dbutils.widgets.text("s3_volume_prefix", "s3://tpcds-datasets/shannon_tpcdi/",
                     "S3 prefix matching the UC volume backing")
dbutils.widgets.text("aws_region",      "us-west-2", "Region for COPY")

database         = dbutils.widgets.get("database")
wh_db            = dbutils.widgets.get("wh_db")
scale_factor     = dbutils.widgets.get("scale_factor")
secret_scope     = dbutils.widgets.get("secret_scope")
incremental_n    = int(dbutils.widgets.get("incremental_batches_to_run"))
databricks_catalog = dbutils.widgets.get("databricks_catalog")
tpcdi_directory  = dbutils.widgets.get("tpcdi_directory").rstrip("/") + "/"
s3_volume_prefix = dbutils.widgets.get("s3_volume_prefix")
aws_region       = dbutils.widgets.get("aws_region")

if not wh_db:
    raise ValueError("wh_db is required")

target_schema  = f"{wh_db}_{scale_factor}".lower()    # Redshift identifiers are lowercase by default
staging_schema = f"tpcdi_staging_sf{scale_factor}".lower()
print(f"target  = {database}.{target_schema}")
print(f"staging = {database}.{staging_schema} (CTAS source; must exist)")

# COMMAND ----------

# MAGIC %run ./_rs_conn

# COMMAND ----------

conn = rs_connect(
    database=database,
    secret_scope=secret_scope,
    query_group={
        "wh_db":        wh_db,
        "scale_factor": scale_factor,
        "task":         "setup_rs",
    },
)
print(f"[ok] connected to Redshift {database}")

# COMMAND ----------

# 0. Self-bootstrap the staging schema if needed. Mirrors setup_bq.py /
#    setup_sf.py pattern: import the bootstrap module from the notebook
#    directory and call ensure_staging_environment() inline.
#    Idempotent — no-op when all 22 staging tables already present.
import sys, os
try:
    _nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None)
    if _nb_path and not _nb_path.startswith("/Workspace"):
        _nb_path = "/Workspace" + _nb_path
    _module_dir = os.path.dirname(_nb_path) if _nb_path else os.getcwd()
except Exception:
    _module_dir = os.getcwd()
if _module_dir not in sys.path:
    sys.path.insert(0, _module_dir)
import rs_staging_bootstrap as bootstrap

parquet_root = f"{tpcdi_directory}staging_parquet_rs/sf={scale_factor}"
iam_role     = rs_iam_role(secret_scope=secret_scope)

_boot = bootstrap.ensure_staging_environment(
    conn,
    database=database,
    target_schema=staging_schema,
    src_catalog=databricks_catalog,
    src_schema=f"tpcdi_incremental_staging_{scale_factor}",
    parquet_root=parquet_root,
    volume_root=tpcdi_directory,
    s3_volume_prefix=s3_volume_prefix,
    iam_role=iam_role,
    aws_region=aws_region,
    spark=spark,
    dbutils=dbutils,
    secret_scope=secret_scope,
    parallel=4,
)
print(f"[bootstrap] {_boot}")

# COMMAND ----------

# 1. DROP+CREATE the per-run schema. Redshift has no CREATE OR REPLACE SCHEMA;
#    DROP CASCADE then CREATE is the clean-slate equivalent. Bronze tables
#    live in this same schema (no separate _bronze schema in the new
#    pre_hook-COPY architecture).
with conn.cursor() as cur:
    cur.execute(f'DROP SCHEMA IF EXISTS "{target_schema}" CASCADE')
    cur.execute(f'CREATE SCHEMA "{target_schema}"')
print(f"[ok] schema {database}.{target_schema} ready")

# COMMAND ----------

# 2. CTAS 22 staging tables into the per-run schema with explicit
#    DISTKEY/SORTKEY/DISTSTYLE declarations.
#
# Per Redshift semantics: DISTKEY/SORTKEY/DISTSTYLE are immutable once a table
# exists — must be declared at CREATE time. Setup owns the layout; dbt models
# declare nothing about distribution.
#
# Layout strategy decisions are documented in PORT_NOTES.md "DISTKEY / SORTKEY
# strategy" section. Small reference tables (under ~5M rows) get DISTSTYLE ALL
# so every node has a copy and joins stay local. Large facts use DISTKEY on
# the most-frequent join column.
#
# Format below: (table_name, distribution_spec, sortkey_cols)
#   distribution_spec: "ALL" | f"KEY({col})" | "EVEN"
#   sortkey_cols: tuple of column names for compound SORTKEY (Redshift's
#                 default); empty tuple = no sort key
TABLE_LAYOUTS = {
    # Bronze CDC-history tables (per-batch dbt pre_hook appends rows here).
    "bronzecustomer":             ("KEY(customerid)",    ("update_dt",)),
    "bronzeaccount":              ("KEY(accountid)",     ("update_dt",)),
    "bronzetrade":                ("KEY(tradeid)",       ("event_dt",)),
    "bronzecashtransaction":      ("KEY(accountid)",     ("event_dt",)),
    "bronzeholdings":             ("KEY(hh_t_id)",       ("event_dt",)),
    "bronzewatches":              ("KEY(w_c_id)",        ("event_dt",)),

    # Facts (large) — DISTKEY on join column, SORTKEY on date(s) for prune
    "factmarkethistory":          ("KEY(sk_securityid)", ("sk_dateid", "sk_securityid", "sk_companyid")),
    "factwatches":                ("KEY(sk_customerid)", ("sk_dateid_dateremoved", "sk_customerid", "sk_securityid")),
    "factholdings":               ("KEY(sk_customerid)", ("sk_dateid", "sk_customerid", "sk_securityid")),
    "factcashbalances":           ("KEY(sk_customerid)", ("sk_dateid", "sk_customerid")),

    # Dims (medium-large) — DISTKEY on natural key, SORTKEY on enddate for SCD2 prune
    "dimtrade":                   ("KEY(sk_securityid)", ("sk_closedateid", "sk_brokerid", "sk_securityid")),
    "dimcustomer":                ("KEY(customerid)",    ("enddate", "customerid")),
    "dimaccount":                 ("KEY(accountid)",     ("enddate", "accountid")),

    # Small dims / snapshot tables — DISTSTYLE ALL
    "currentaccountbalances":     ("ALL", ("accountid",)),
    "dimbroker":                  ("ALL", ("brokerid",)),
    "dimsecurity":                ("ALL", ("symbol",)),    # ~16M, big-ish but heavily joined
    "dimcompany":                 ("ALL", ("companyid",)),
    "dimdate":                    ("ALL", ("sk_dateid",)),
    "dimtime":                    ("ALL", ("sk_timeid",)),

    # Tiny reference — DISTSTYLE ALL
    "taxrate":                    ("ALL", ()),
    "industry":                   ("ALL", ()),
    "tradetype":                  ("ALL", ()),
    "statustype":                 ("ALL", ()),
    "batchdate":                  ("ALL", ()),

    # Bronze staging (event-time-clustered)
    "bronzedailymarket":          ("KEY(dm_s_symb)",     ("dm_date",)),

    # Quarterly financial data
    "financial":                  ("KEY(sk_companyid)",  ("fi_year", "fi_qtr")),
    "companyyeareps":             ("KEY(sk_companyid)",  ("qtr_start_date",)),

    # Cash transaction history — large, time-ordered
    "cashtransactionhistorical":  ("KEY(accountid)",     ("event_dt", "ct_dts")),
}

# Verify completeness against the canonical 22 STAGING_TABLES set used on
# the SF / BQ sides. Update this list if the canonical set ever changes.
STAGING_TABLES_EXPECTED = {
    # 6 bronze CDC-history tables (carry pre-2016-07-06 SCD2 history;
    # dbt bronze pre_hook appends new daily batches on top).
    "bronzecustomer", "bronzeaccount", "bronzetrade",
    "bronzecashtransaction", "bronzeholdings", "bronzewatches",
    # 22 reference + fact + dim staging tables.
    "bronzedailymarket", "factmarkethistory", "factwatches", "dimtrade",
    "factholdings", "factcashbalances", "cashtransactionhistorical",
    "financial", "companyyeareps", "dimaccount", "dimcustomer",
    "currentaccountbalances", "dimbroker", "dimsecurity", "dimcompany",
    "dimtime", "dimdate", "taxrate", "industry", "tradetype",
    "statustype", "batchdate",
}
_missing_layouts = STAGING_TABLES_EXPECTED - set(TABLE_LAYOUTS)
if _missing_layouts:
    raise RuntimeError(
        f"TABLE_LAYOUTS is missing layout decisions for: {sorted(_missing_layouts)}"
    )

# COMMAND ----------

# Run the 22 CTAS statements in parallel. Independent table writes — Redshift's
# psql wire protocol supports concurrent statements (each cursor on its own
# implicit transaction).
import concurrent.futures as _cf
import time as _time

def _ctas_one(table_name: str) -> tuple[str, float]:
    dist_spec, sortkey_cols = TABLE_LAYOUTS[table_name]

    # Build the layout clauses. `DISTKEY(col)` alone implies `DISTSTYLE KEY`.
    if dist_spec == "ALL":
        dist_sql = "DISTSTYLE ALL"
    elif dist_spec == "EVEN":
        dist_sql = "DISTSTYLE EVEN"
    elif dist_spec.startswith("KEY("):
        col = dist_spec[len("KEY("):-1]
        dist_sql = f"DISTKEY({col})"
    else:
        raise ValueError(f"Unknown distribution_spec for {table_name}: {dist_spec}")

    sort_sql = f"SORTKEY({', '.join(sortkey_cols)})" if sortkey_cols else ""

    sql = f'''
        CREATE TABLE "{target_schema}"."{table_name}"
            {dist_sql}
            {sort_sql}
        AS
        SELECT * FROM "{staging_schema}"."{table_name}"
    '''.strip()

    t0 = _time.time()
    # Each thread needs its own connection — psycopg2 connections aren't safe
    # for concurrent use across threads.
    local_conn = rs_connect(
        database=database, secret_scope=secret_scope,
        query_group={"task": "setup_rs", "phase": "ctas", "table": table_name,
                     "wh_db": wh_db, "scale_factor": scale_factor},
    )
    try:
        with local_conn.cursor() as cur:
            cur.execute(sql)
    finally:
        local_conn.close()
    return (table_name, _time.time() - t0)


t_clone = _time.time()
print(f"[parallel] CTAS {len(STAGING_TABLES_EXPECTED)} tables (8 concurrent)...")
with _cf.ThreadPoolExecutor(max_workers=8) as ex:
    futures = {ex.submit(_ctas_one, t): t for t in sorted(STAGING_TABLES_EXPECTED)}
    for f in _cf.as_completed(futures):
        try:
            name, wall = f.result()
            print(f"[ctas] {name:32s} {wall:5.2f}s")
        except Exception as e:
            name = futures[f]
            print(f"[FAIL] {name:32s}  {type(e).__name__}: {e}")
            raise
print(f"[parallel] CTAS done in {_time.time() - t_clone:.1f}s")

# COMMAND ----------

# 3. Bronze layer setup: the CTAS step above already created all 22 staging
#    tables in the main schema, INCLUDING bronzedailymarket / bronzecustomer
#    / bronzeaccount / etc. (which are part of STAGING_TABLES). dbt's rs_bronze
#    models use these as their incremental `{{ this }}` target — the pre_hook
#    `CREATE TEMP TABLE foo_stg (LIKE foo)` clause inherits the schema from
#    these CTAS'd tables, and each per-batch INSERT … SELECT FROM stg appends
#    the day's rows.
#
#    No separate `_bronze` landing schema, no pre-created empty bronze
#    tables. Bronze ingestion lives entirely inside the dbt run (pre_hook
#    COPYs from S3 on the same compute that runs silver+gold) — same
#    compute attribution as the SF/BQ variants. See
#    `dbt/macros/rs_bronze_copy_prehook.sql` for the COPY incantation.

# COMMAND ----------

# 4. Emit batch_date_ls — match setup_dbt.py / setup_sf.py / setup_bq.py exactly:
#    AUG_FILES_DATE_START is hardcoded to 2016-07-06.
import datetime as dt
incr_start = dt.date(2016, 7, 6)
batches = [(incr_start + dt.timedelta(days=i)).isoformat()
           for i in range(incremental_n)]
dbutils.jobs.taskValues.set("batch_date_ls", batches)
print(f"emitted batch_date_ls: {len(batches)} dates, first={batches[0]}, last={batches[-1]}")

# COMMAND ----------

conn.close()
print("[done] Redshift setup complete.")
