# Databricks notebook source
# Per-run Snowflake setup. Dispatches SQL to Snowflake from a Databricks
# task (no Snowflake compute used here aside from instant zero-copy
# CLONEs + tiny DDL).
#
# Sequence:
#   1. CREATE OR REPLACE SCHEMA TPCDI_TEST.{wh_db}_{sf}
#   2. CLONE 22 historical/reference tables from TPCDI_TEST.STAGING_SF{sf}
#      (zero-copy, Snowflake clone preserves the CLUSTER BY each table
#      has on the source — set in the one-time materialization step)
#   3. Pre-create the 7 empty bronze + account_updates target tables
#      with CLUSTER BY (the dbt-managed targets dbt will populate per-batch)
#   4. Emit batch_date_ls task value for the parent's for_each loop
#
# Prereq: TPCDI_TEST.STAGING_SF{sf} must exist (materialized natively).
# Run `onetime_stg_snowflake_tables` once per SF to produce that schema
# from the federated UC Iceberg catalog (TPCDI_TEST.STAGING_SF{sf}_DBX).
#
# Auth: reads creds from the {secret_scope} Databricks secret scope
# (see ./_sf_conn.py).

# COMMAND ----------

dbutils.widgets.text("catalog",         "TPCDI_TEST", "Snowflake database (treated as `catalog`)")
dbutils.widgets.text("wh_db",           "",           "wh_db prefix; final schema = {wh_db}_{scale_factor}")
dbutils.widgets.dropdown("scale_factor","10", ["10","100","1000","5000","10000","20000"], "scale_factor")
dbutils.widgets.text("tpcdi_directory", "/Volumes/main/tpcdi_raw_data/tpcdi_volume/",
                     "UC external volume root the per-batch files land under")
dbutils.widgets.text("snowflake_stage", "TPCDI_STAGE", "Snowflake stage name (no @)")
dbutils.widgets.text("secret_scope",    "tpcdi_snowflake", "Databricks secret scope")
dbutils.widgets.text("snowflake_warehouse", "", "Override the Snowflake warehouse (empty = use secret_scope.warehouse default)")
dbutils.widgets.dropdown("table_format", "native", ["native","iceberg"],
                          "Lineage tag for query attribution. 'native' = the per-run tables are Snowflake-native (current default — what setup_sf actually does today, regardless of this value). 'iceberg' is reserved for when we plumb a real Iceberg-table path; setting it today is just a tag, NOT a behavior switch. Flows through to Snowflake query_tag so the dashboard can split runs by lineage.")
dbutils.widgets.text("incremental_batches_to_run", "365", "Number of batches the for_each loop runs")
dbutils.widgets.text("benchmark_start_date",       "2015-07-06", "Start of the prior-year backfill window")

catalog          = dbutils.widgets.get("catalog")
wh_db            = dbutils.widgets.get("wh_db")
scale_factor     = dbutils.widgets.get("scale_factor")
secret_scope     = dbutils.widgets.get("secret_scope")
warehouse        = dbutils.widgets.get("snowflake_warehouse") or None
table_format     = dbutils.widgets.get("table_format")
incremental_n    = int(dbutils.widgets.get("incremental_batches_to_run"))

if not wh_db:
    raise ValueError("wh_db is required")

target_schema    = f"{wh_db}_{scale_factor}"
staging_schema   = f"STAGING_SF{scale_factor}"
print(f"target  = {catalog}.{target_schema}")
print(f"staging = {catalog}.{staging_schema} (clone source — must be materialized via onetime_stg_snowflake_tables)")

# COMMAND ----------

# MAGIC %run ./_sf_conn

# COMMAND ----------

conn = sf_connect(
    database=catalog,
    secret_scope=secret_scope,
    warehouse=warehouse,
    query_tag={
        "wh_db":        wh_db,
        "scale_factor": scale_factor,
        "table_format": table_format,
        "task":         "setup_sf",
    },
)
print(f"[ok] connected to Snowflake; warehouse = {warehouse or '(from secret_scope default)'}")
cur = conn.cursor()

cur.execute(f"CREATE DATABASE IF NOT EXISTS {catalog}")
cur.execute(f"USE DATABASE {catalog}")
cur.execute(f"CREATE OR REPLACE SCHEMA {catalog}.{target_schema}")
cur.execute(f"USE SCHEMA {catalog}.{target_schema}")
print(f"[ok] schema {catalog}.{target_schema} ready")

# COMMAND ----------

# 1. CLONE 22 historical/reference tables from the staging schema.
# CLONE is zero-copy — Snowflake just stamps new pointers, divergence
# happens only when dbt MERGEs new rows in. The source's CLUSTER BY
# (set by onetime_stg_snowflake_tables) propagates to the clone, so
# auto-clustering stays active on the per-run target without any
# additional work.
STAGING_TABLES = [
    "taxrate", "dimdate", "industry", "tradetype", "dimbroker",
    "dimsecurity", "statustype", "dimcompany", "dimtime", "financial",
    "companyyeareps", "currentaccountbalances", "dimaccount",
    "dimcustomer", "dimtrade", "factwatches", "factcashbalances",
    "factholdings", "factmarkethistory", "bronzedailymarket",
    "cashtransactionhistorical", "batchdate",
]

# Parallel CLONE — each worker opens its own Snowflake conn (the
# connector isn't thread-safe). Clones are metadata-only so 8-way
# concurrency is overkill, but mirrors the materialization-time
# pattern for consistency. Done in seconds total.
import concurrent.futures as _cf
import time as _time

def _clone_one(table_name: str) -> tuple[str, float]:
    """Open a fresh Snowflake conn and clone one staging table into the
    run schema. Returns (table_name, wall_sec)."""
    _t0 = _time.time()
    _conn = sf_connect(
        database=catalog,
        secret_scope=secret_scope,
        warehouse=warehouse,
        query_tag={
            "wh_db":        wh_db,
            "scale_factor": scale_factor,
            "table_format": table_format,
            "task":         "setup_sf",
        },
    )
    try:
        with _conn.cursor() as _c:
            _c.execute(
                f"CREATE OR REPLACE TABLE {catalog}.{target_schema}.{table_name} "
                f"CLONE {catalog}.{staging_schema}.{table_name}"
            )
        return (table_name, _time.time() - _t0)
    finally:
        _conn.close()

_t_clone = _time.time()
print(f"[parallel] cloning {len(STAGING_TABLES)} tables (8 concurrent)...")
with _cf.ThreadPoolExecutor(max_workers=8) as _ex:
    _futures = {_ex.submit(_clone_one, t): t for t in STAGING_TABLES}
    for _f in _cf.as_completed(_futures):
        try:
            _name, _wall = _f.result()
            print(f"[clone] {_name:30s} {_wall:5.2f}s")
        except Exception as _e:
            _name = _futures[_f]
            print(f"[FAIL] {_name:30s}  {type(_e).__name__}: {_e}")
            raise
print(f"[parallel] clones done in {_time.time() - _t_clone:.1f}s")

# COMMAND ----------

# 2. Pre-create the 7 dbt-managed target tables (empty; dbt fills them
# per batch). These don't exist in STAGING_SF{sf} — dbt creates them
# from the bronze/silver model bodies. CLUSTER BY mirrors the
# Databricks-side variants' bronze layout (Auto Loader / streaming
# ingest in those variants; per-batch INSERT in the Snowflake dbt path).
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
