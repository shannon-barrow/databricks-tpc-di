# Databricks notebook source
# Per-run Snowflake setup. Dispatches SQL to Snowflake from a Databricks
# interactive cluster (no Snowflake compute used here aside from the
# instant zero-copy CLONEs + DDL). Replaces the older `setup.py` which
# CREATE-OR-REPLACE'd target tables; this version CLONEs from a one-time
# `STAGING_SF{sf}` schema seeded by `seed_staging.py`.
#
# Sequence:
#   1. CREATE OR REPLACE SCHEMA TPCDI_TEST.{wh_db}_{sf}
#   2. CLONE 12 reference + dimension tables from TPCDI_TEST.STAGING_SF{sf}
#   3. Pre-create the 16 bronze/silver/gold target tables with CLUSTER BY
#   4. Emit batch_date_ls task value for the parent's for_each loop
#
# Auth: reads creds from the {secret_scope} Databricks secret scope (see
# ./_sf_conn.py).

# COMMAND ----------

dbutils.widgets.text("catalog",         "TPCDI_TEST", "Snowflake database (treated as `catalog`)")
dbutils.widgets.text("wh_db",           "",           "wh_db prefix; final schema = {wh_db}_{scale_factor}")
dbutils.widgets.dropdown("scale_factor","10", ["10","100","1000","5000","10000","20000"], "scale_factor")
dbutils.widgets.text("tpcdi_directory", "/Volumes/main/tpcdi_raw_data/tpcdi_benchmarking/",
                     "UC external volume root the per-batch files land under")
dbutils.widgets.text("snowflake_stage", "TPCDI_STAGE", "Snowflake stage name (no @)")
dbutils.widgets.text("secret_scope",    "tpcdi_snowflake", "Databricks secret scope")
dbutils.widgets.text("snowflake_warehouse", "", "Override the Snowflake warehouse (empty = use secret_scope.warehouse default)")
dbutils.widgets.text("snowflake_warehouse_setup", "", "Setup-only WH override (empty = same as snowflake_warehouse). Setup CTAS is one-time and resource-heavy at SF=20k — size up here (e.g., BARROW_LARGE_GEN2) without affecting per-batch dbt perf.")
dbutils.widgets.dropdown("table_format", "snowflake_native", ["snowflake_native","iceberg"],
                          "Source of the per-run seed: 'snowflake_native' = CLONE from STAGING_SF{sf} (parquet→COPY INTO seed); 'iceberg' = CTAS from STAGING_SF{sf}_DBX (federated Iceberg, zero parquet copy)")
dbutils.widgets.text("incremental_batches_to_run", "365", "Number of batches the for_each loop runs")
dbutils.widgets.text("benchmark_start_date",       "2015-07-06", "Start of the prior-year backfill window")

catalog          = dbutils.widgets.get("catalog")
wh_db            = dbutils.widgets.get("wh_db")
scale_factor     = dbutils.widgets.get("scale_factor")
secret_scope     = dbutils.widgets.get("secret_scope")
warehouse        = dbutils.widgets.get("snowflake_warehouse") or None  # None lets sf_connect fall back to secret
# Setup-specific WH override: setup_sf does 22 large CTAS operations one-time
# per benchmark run — sized up to BARROW_LARGE_GEN2 typically. Falls back to
# snowflake_warehouse if not set so existing callers don't break.
setup_warehouse  = dbutils.widgets.get("snowflake_warehouse_setup") or warehouse
table_format     = dbutils.widgets.get("table_format")
incremental_n    = int(dbutils.widgets.get("incremental_batches_to_run"))

if not wh_db:
    raise ValueError("wh_db is required")
if table_format not in ("snowflake_native","iceberg"):
    raise ValueError(f"table_format must be 'snowflake_native' or 'iceberg', got: {table_format!r}")

target_schema    = f"{wh_db}_{scale_factor}"
# snowflake_native: parquet → COPY INTO seeded schema (TPCDI_TEST.STAGING_SF{sf})
# iceberg:          federated UC Iceberg schema (TPCDI_TEST.STAGING_SF{sf}_DBX)
staging_schema   = f"STAGING_SF{scale_factor}" if table_format == "snowflake_native" else f"STAGING_SF{scale_factor}_DBX"
print(f"target       = {catalog}.{target_schema}")
print(f"staging      = {catalog}.{staging_schema}")
print(f"table_format = {table_format}")

# COMMAND ----------

# MAGIC %run ./_sf_conn

# COMMAND ----------

conn = sf_connect(
    database=catalog,
    secret_scope=secret_scope,
    warehouse=setup_warehouse,
    query_tag={
        "wh_db":        wh_db,
        "scale_factor": scale_factor,
        "table_format": table_format,
        "task":         "setup_sf",
    },
)
print(f"[ok] connected to Snowflake; setup_warehouse={setup_warehouse or '(from secret_scope default)'}  per-batch warehouse={warehouse or '(secret default)'}")
cur  = conn.cursor()

cur.execute(f"CREATE DATABASE IF NOT EXISTS {catalog}")
cur.execute(f"USE DATABASE {catalog}")
cur.execute(f"CREATE OR REPLACE SCHEMA {catalog}.{target_schema}")
cur.execute(f"USE SCHEMA {catalog}.{target_schema}")
print(f"[ok] schema {catalog}.{target_schema} ready")

# COMMAND ----------

# 1. CLONE reference + dimension tables from the staging schema.
# Match seed_staging_py.STAGING_TABLES — every table the staging job
# pre-populates needs to be CLONEd into the run schema so dbt's
# incremental MERGE/APPEND models build on top of historical rows
# (otherwise factwatches/dimtrade/etc. join against an empty dimcustomer
# and emit 0 rows).
STAGING_TABLES = [
    "taxrate", "dimdate", "industry", "tradetype", "dimbroker",
    "dimsecurity", "statustype", "dimcompany", "dimtime", "financial",
    "companyyeareps", "currentaccountbalances", "dimaccount",
    "dimcustomer", "dimtrade", "factwatches", "factcashbalances",
    "factholdings", "factmarkethistory", "bronzedailymarket",
    "cashtransactionhistorical", "batchdate",
]
# Cluster keys mirror the Databricks-side Liquid layout (see
# historical/*.sql `CLUSTER BY (...)` clauses + setup.py / setup_dbt.py
# DEEP CLONE targets). For Snowflake-side iceberg mode we set them at
# CTAS time so auto-clustering kicks in on the run-schema target without
# dbt's per-batch model needing to re-issue ALTER CLUSTER BY (same
# setup-owns-layout pattern the dbt-Databricks variant uses).
# Tables not in this dict are CTAS'd plain (no clustering — reference /
# small dims where clustering offers no benefit).
CLUSTER_KEYS = {
    "dimcustomer":       "enddate",
    "dimaccount":        "enddate",
    "dimtrade":          "sk_closedateid",
    "factwatches":       "sk_dateid_dateremoved",
    "factmarkethistory": "sk_dateid",
    "factcashbalances":  "sk_dateid",
    "factholdings":      "sk_dateid",
    "bronzedailymarket": "dm_date",
    "companyyeareps":    "qtr_start_date",
}

# Submit the 22 CTAS / CLONE statements in parallel — matches the
# Databricks-side setup.py / setup_dbt.py pattern (ThreadPoolExecutor
# over clone_table). snowflake-connector connections are NOT thread-safe,
# so each worker opens its own. max_workers=8 saturates BARROW_S's
# max_concurrency_level. Each worker's query_tag mirrors the schema-
# create conn's so QUERY_HISTORY attributes them all to task='setup_sf'.
import concurrent.futures as _cf
import time as _time

def _seed_one(table_name: str) -> tuple[str, str, float]:
    """Open a fresh Snowflake conn, run the CTAS / CLONE for `table_name`,
    return (table_name, status_msg, wall_sec). Caller raises on exception."""
    _t0 = _time.time()
    _conn = sf_connect(
        database=catalog,
        secret_scope=secret_scope,
        warehouse=setup_warehouse,
        query_tag={
            "wh_db":        wh_db,
            "scale_factor": scale_factor,
            "table_format": table_format,
            "task":         "setup_sf",
        },
    )
    try:
        _cluster = f"\n  CLUSTER BY ({CLUSTER_KEYS[table_name]})" if table_name in CLUSTER_KEYS else ""
        with _conn.cursor() as _c:
            if table_format == "iceberg":
                # External-catalog Iceberg can't be CLONEd into Snowflake-native;
                # CTAS materializes from federated Iceberg into a writable native
                # table. Replaces the parquet→COPY INTO seed step entirely.
                _c.execute(
                    f"CREATE OR REPLACE TABLE {catalog}.{target_schema}.{table_name}"
                    f"{_cluster} AS "
                    f"SELECT * FROM {catalog}.{staging_schema}.{table_name}"
                )
                _op = "ctas"
            else:
                # Snowflake-native CLONE preserves source clustering.
                _c.execute(
                    f"CREATE OR REPLACE TABLE {catalog}.{target_schema}.{table_name} "
                    f"CLONE {catalog}.{staging_schema}.{table_name}"
                )
                _op = "clone"
        return (table_name, _op, _time.time() - _t0)
    finally:
        _conn.close()

_t_setup = _time.time()
print(f"[parallel] seeding {len(STAGING_TABLES)} tables on {setup_warehouse or '(secret default)'} (8 concurrent)...")
with _cf.ThreadPoolExecutor(max_workers=8) as _ex:
    _futures = {_ex.submit(_seed_one, t): t for t in STAGING_TABLES}
    for _f in _cf.as_completed(_futures):
        try:
            _name, _op, _wall = _f.result()
            _key = f" (CLUSTER BY {CLUSTER_KEYS[_name]})" if _name in CLUSTER_KEYS else ""
            print(f"[{_op}]  {_name:30s} {_wall:6.1f}s{_key}")
        except Exception as _e:
            _name = _futures[_f]
            print(f"[FAIL] {_name:30s}  {type(_e).__name__}: {_e}")
            raise
print(f"[parallel] seed done in {_time.time() - _t_setup:.1f}s")

# COMMAND ----------

# 2. Pre-create bronze/silver/gold target tables (empty; dbt fills them).
#    Same DDL as the previous setup.py, kept here for self-containment.
def _ddl(name, schema_sql, cluster_by=None):
    cluster_clause = f"\nCLUSTER BY ({cluster_by})" if cluster_by else ""
    cur.execute(f"CREATE OR REPLACE TABLE {catalog}.{target_schema}.{name} ({schema_sql}){cluster_clause}")
    print(f"[ddl] {name}")

_ddl("bronzeaccount", "cdc_flag STRING, cdc_dsn NUMBER, accountid NUMBER, brokerid NUMBER, customerid NUMBER, accountdesc STRING, taxstatus NUMBER, status STRING, update_dt DATE", "update_dt")
_ddl("bronzecashtransaction", "cdc_flag STRING, cdc_dsn NUMBER, accountid NUMBER, ct_dts TIMESTAMP, ct_amt FLOAT, ct_name STRING, event_dt DATE", "event_dt")
_ddl("bronzecustomer", "cdc_flag STRING, cdc_dsn NUMBER, customerid NUMBER, taxid STRING, status STRING, lastname STRING, firstname STRING, middleinitial STRING, gender STRING, tier NUMBER, dob DATE, addressline1 STRING, addressline2 STRING, postalcode STRING, city STRING, stateprov STRING, country STRING, c_ctry_1 STRING, c_area_1 STRING, c_local_1 STRING, c_ext_1 STRING, c_ctry_2 STRING, c_area_2 STRING, c_local_2 STRING, c_ext_2 STRING, c_ctry_3 STRING, c_area_3 STRING, c_local_3 STRING, c_ext_3 STRING, email1 STRING, email2 STRING, lcl_tx_id STRING, nat_tx_id STRING, update_dt DATE", "update_dt")
_ddl("bronzeholdings", "cdc_flag STRING, cdc_dsn NUMBER, hh_h_t_id NUMBER, hh_t_id NUMBER, hh_before_qty NUMBER, hh_after_qty NUMBER, event_dt DATE", "event_dt")
_ddl("bronzetrade", "cdc_flag STRING, cdc_dsn NUMBER, tradeid NUMBER, t_dts TIMESTAMP, status STRING, t_tt_id STRING, cashflag NUMBER, t_s_symb STRING, quantity NUMBER, bidprice FLOAT, t_ca_id NUMBER, executedby STRING, tradeprice FLOAT, fee FLOAT, commission FLOAT, tax FLOAT, event_dt DATE", "event_dt")
_ddl("bronzewatches", "cdc_flag STRING, cdc_dsn NUMBER, w_c_id NUMBER, w_s_symb STRING, w_dts TIMESTAMP, w_action STRING, event_dt DATE", "event_dt")
_ddl("account_updates_from_customer", "cdc_flag STRING, cdc_dsn NUMBER, accountid NUMBER, brokerid NUMBER, customerid NUMBER, accountdesc STRING, taxstatus NUMBER, status STRING, update_dt DATE", "update_dt")
# Note: dimaccount target is CLONED above (the SCD2 history pre-batch-start
# is in STAGING_SF{sf}); the per-batch silver/dimaccount dbt model writes
# back into the cloned table — that's intentional, matches Databricks side.
print(f"[ok] target tables ready under {catalog}.{target_schema}")

# COMMAND ----------

# 3. Emit batch_date_ls — match setup_dbt.py exactly: AUG_FILES_DATE_START
# is hardcoded to 2016-07-06. (Computing it as benchmark_start + 365 days
# lands on 2016-07-05 because 2016 is a leap year — the resulting batch
# falls one day before any source data exists.)
import datetime as dt
incr_start = dt.date(2016, 7, 6)  # AUG_FILES_DATE_START, see tpcdi_gen/config.py
batches = [(incr_start + dt.timedelta(days=i)).isoformat() for i in range(incremental_n)]
dbutils.jobs.taskValues.set("batch_date_ls", batches)
print(f"emitted batch_date_ls: {len(batches)} dates, first={batches[0]}, last={batches[-1]}")

# COMMAND ----------

cur.close()
conn.close()
print("[done] Snowflake setup complete.")
