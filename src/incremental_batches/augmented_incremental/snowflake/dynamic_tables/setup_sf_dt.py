# Databricks notebook source
# Per-run Snowflake setup for the **Dynamic Tables** variant. No dbt.
#
# Sequence:
#   1. CREATE OR REPLACE SCHEMA TPCDI_TEST.{wh_db}_{sf}
#   2. CLONE 13 reference / dim tables from TPCDI_TEST.STAGING_SF{sf}
#      (taxrate, dimdate, industry, tradetype, dimbroker, dimsecurity,
#       statustype, dimcompany, dimtime, financial, companyyeareps,
#       batchdate, cashtransactionhistorical, bronzedailymarket)
#   3. Pre-create 6 empty `bronze*_raw` tables (the COPY INTO targets that
#      seed_raw.py populates per batch).
#   4. Run `dt_create.sql` — declares the 16 dynamic tables. After this
#      runs, the refresh DAG is live.
#   5. Emit batch_date_ls task value for the parent's for_each loop.
#
# Prereqs:
#   - TPCDI_TEST.STAGING_SF{sf} must exist (silver/gold + reference,
#     materialized natively via `onetime_stg_snowflake_tables.py`)
#   - TPCDI_TEST.STAGING_SF{sf}_DBX must exist (bronze, federated Iceberg
#     via the TPCDI_DBX_UC_SF10_INT catalog integration). Bronze tables
#     stay Iceberg — no native materialization step.

# COMMAND ----------

dbutils.widgets.text("catalog",         "TPCDI_TEST", "Snowflake database (treated as `catalog`)")
dbutils.widgets.text("wh_db",           "",           "wh_db prefix; final schema = {wh_db}_{scale_factor}")
dbutils.widgets.dropdown("scale_factor","10", ["10","100","1000","5000","10000","20000"], "scale_factor")
dbutils.widgets.text("snowflake_stage", "TPCDI_STAGE", "Snowflake stage name (no @)")
dbutils.widgets.text("secret_scope",    "tpcdi_snowflake", "Databricks secret scope")
dbutils.widgets.text("snowflake_warehouse", "BARROW_MED_GEN2",
                     "WH used as the DT refresh warehouse (every DT runs on this)")
dbutils.widgets.text("target_lag", "1 minute",
                     "TARGET_LAG for leaf gold DTs (intermediates use DOWNSTREAM)")
dbutils.widgets.text("incremental_batches_to_run", "365", "Number of batches the for_each loop runs")
dbutils.widgets.text("dt_create_sql_path", "", "Absolute workspace path to dt_create.sql (set by workflow builder)")

catalog          = dbutils.widgets.get("catalog")
wh_db            = dbutils.widgets.get("wh_db")
scale_factor     = dbutils.widgets.get("scale_factor")
secret_scope     = dbutils.widgets.get("secret_scope")
warehouse        = dbutils.widgets.get("snowflake_warehouse")
target_lag       = dbutils.widgets.get("target_lag")
incremental_n    = int(dbutils.widgets.get("incremental_batches_to_run"))
dt_create_sql_path = dbutils.widgets.get("dt_create_sql_path")

if not wh_db:
    raise ValueError("wh_db is required")
if not warehouse:
    raise ValueError("snowflake_warehouse is required (DT refresh warehouse)")
if not dt_create_sql_path:
    raise ValueError(
        "dt_create_sql_path is required — the workflow builder passes the "
        "absolute workspace path to dt_create.sql (sibling of this notebook).")

target_schema    = f"{wh_db}_{scale_factor}"
staging_schema   = f"STAGING_SF{scale_factor}"               # native, clonable (silver/gold + reference)
bronze_iceberg_schema = f"STAGING_SF{scale_factor}_DBX"      # federated Iceberg (bronze only, no native copy)
print(f"target  = {catalog}.{target_schema}")
print(f"staging = {catalog}.{staging_schema} (CLONE source for ref/silver/gold)")
print(f"bronze  = {catalog}.{bronze_iceberg_schema} (federated Iceberg — bronze*_raw seed source)")
print(f"DT warehouse = {warehouse}, target_lag (leaves) = {target_lag}")

# COMMAND ----------

# MAGIC %run ../_sf_conn

# COMMAND ----------

# Single connection for the whole setup — all this is fast metadata work
# (CLONE is zero-copy; DDL parses one model at a time). No need to parallelize.
conn = sf_connect(
    database=catalog,
    secret_scope=secret_scope,
    warehouse=warehouse,
    query_tag={
        "wh_db":        wh_db,
        "scale_factor": scale_factor,
        "table_format": "dynamic_tables",
        "task":         "setup_sf_dt",
    },
)
print(f"[ok] connected to Snowflake; warehouse = {warehouse}")
cur = conn.cursor()

cur.execute(f"CREATE DATABASE IF NOT EXISTS {catalog}")
cur.execute(f"USE DATABASE {catalog}")
cur.execute(f"CREATE OR REPLACE SCHEMA {catalog}.{target_schema}")
cur.execute(f"USE SCHEMA {catalog}.{target_schema}")
print(f"[ok] schema {catalog}.{target_schema} ready")

# COMMAND ----------

# 1. CLONE the reference + dim tables that don't change per batch.
# These stay as REGULAR tables (no DT semantics needed) — the DTs read from
# them as static joins.
#
# Note: differs from the dbt variant's CLONE list — we EXCLUDE the 8 tables
# that become dynamic tables (dimcustomer, dimaccount, dimtrade, factwatches,
# factcashbalances, factholdings, factmarkethistory, currentaccountbalances).
# bronzedailymarket is NOT cloned here — it's a bronze table, so per the
# DT-variant principle it stays Iceberg-only on the staging side. Below we
# CTAS it from the federated Iceberg schema into a writable native table
# (the per-batch DailyMarket.txt COPY INTO appends to it; factmarkethistory's
# DT reads from it directly).
CLONE_TABLES = [
    "taxrate", "dimdate", "industry", "tradetype", "dimbroker",
    "dimsecurity", "statustype", "dimcompany", "dimtime", "financial",
    "companyyeareps", "batchdate", "cashtransactionhistorical",
]

print(f"[clone] {len(CLONE_TABLES)} reference tables (sequential — zero-copy, fast)...")
import time as _time
_t0 = _time.time()
for tbl in CLONE_TABLES:
    cur.execute(
        f"CREATE OR REPLACE TABLE {catalog}.{target_schema}.{tbl} "
        f"CLONE {catalog}.{staging_schema}.{tbl}"
    )
print(f"[clone] done in {_time.time() - _t0:.1f}s")

# bronzedailymarket — writable native table seeded from federated Iceberg.
# Per-batch COPY INTO appends DailyMarket.txt rows here.
_t0 = _time.time()
cur.execute(
    f"CREATE OR REPLACE TABLE {catalog}.{target_schema}.bronzedailymarket "
    f"CLUSTER BY (dm_date) AS "
    f"SELECT * FROM {catalog}.{bronze_iceberg_schema}.bronzedailymarket"
)
cur.execute(f"SELECT COUNT(*) FROM {catalog}.{target_schema}.bronzedailymarket")
_n = (cur.fetchone() or [0])[0]
print(f"[ctas] bronzedailymarket: {_n:,} rows ({_time.time() - _t0:.1f}s, from federated Iceberg)")

# COMMAND ----------

# 2. Pre-create the 6 bronze RAW tables. seed_raw.py per-batch COPY INTOs
# new rows into these; the bronze* DTs read from them.
def _raw(name, schema_sql, cluster_by=None):
    cluster_clause = f"\nCLUSTER BY ({cluster_by})" if cluster_by else ""
    cur.execute(f"CREATE OR REPLACE TABLE {catalog}.{target_schema}.{name} ({schema_sql}){cluster_clause}")
    print(f"[raw] {name}")

_raw("bronzeaccount_raw",
     "cdc_flag STRING, cdc_dsn NUMBER, accountid NUMBER, brokerid NUMBER, customerid NUMBER, "
     "accountdesc STRING, taxstatus NUMBER, status STRING, update_dt DATE",
     "update_dt")
_raw("bronzecashtransaction_raw",
     "cdc_flag STRING, cdc_dsn NUMBER, accountid NUMBER, ct_dts TIMESTAMP, ct_amt FLOAT, "
     "ct_name STRING, event_dt DATE",
     "event_dt")
_raw("bronzecustomer_raw",
     "cdc_flag STRING, cdc_dsn NUMBER, customerid NUMBER, taxid STRING, status STRING, "
     "lastname STRING, firstname STRING, middleinitial STRING, gender STRING, tier NUMBER, "
     "dob DATE, addressline1 STRING, addressline2 STRING, postalcode STRING, city STRING, "
     "stateprov STRING, country STRING, c_ctry_1 STRING, c_area_1 STRING, c_local_1 STRING, "
     "c_ext_1 STRING, c_ctry_2 STRING, c_area_2 STRING, c_local_2 STRING, c_ext_2 STRING, "
     "c_ctry_3 STRING, c_area_3 STRING, c_local_3 STRING, c_ext_3 STRING, email1 STRING, "
     "email2 STRING, lcl_tx_id STRING, nat_tx_id STRING, update_dt DATE",
     "update_dt")
_raw("bronzeholdings_raw",
     "cdc_flag STRING, cdc_dsn NUMBER, hh_h_t_id NUMBER, hh_t_id NUMBER, hh_before_qty NUMBER, "
     "hh_after_qty NUMBER, event_dt DATE",
     "event_dt")
_raw("bronzetrade_raw",
     "cdc_flag STRING, cdc_dsn NUMBER, tradeid NUMBER, t_dts TIMESTAMP, status STRING, "
     "t_tt_id STRING, cashflag NUMBER, t_s_symb STRING, quantity NUMBER, bidprice FLOAT, "
     "t_ca_id NUMBER, executedby STRING, tradeprice FLOAT, fee FLOAT, commission FLOAT, "
     "tax FLOAT, event_dt DATE",
     "event_dt")
_raw("bronzewatches_raw",
     "cdc_flag STRING, cdc_dsn NUMBER, w_c_id NUMBER, w_s_symb STRING, w_dts TIMESTAMP, "
     "w_action STRING, event_dt DATE",
     "event_dt")
print(f"[ok] bronze_raw tables ready under {catalog}.{target_schema}")

# COMMAND ----------

# 2b. SEED bronze*_raw directly from the federated Iceberg bronze tables.
# Prereq: augmented_staging Stage 0 must have been run with
# `generate_bronze_staging=YES` at this SF, producing UniForm Delta
# tables under `main.tpcdi_incremental_staging_{sf}.bronze<dataset>`.
# Those are mirrored into Snowflake as federated Iceberg tables under
# `TPCDI_TEST.STAGING_SF{sf}_DBX.bronze<dataset>` via the
# TPCDI_DBX_UC_SF10_INT catalog integration. No native materialization
# of the bronze data ever occurs on the Snowflake side — types are
# preserved through Iceberg.
#
# This step IS required for the DT DAG to incrementally build correct
# silver/gold. Each DT first-refresh processes the cumulative bronze
# (historical seed + per-batch) through the dependency chain.
_BRONZE_RAW_SEED = [
    "bronzeaccount",
    "bronzecashtransaction",
    "bronzecustomer",
    "bronzeholdings",
    "bronzetrade",
    "bronzewatches",
]
print(f"[seed] inserting historical bronze events from {bronze_iceberg_schema} (Iceberg) into bronze*_raw...")
_t_seed = _time.time()
for ds in _BRONZE_RAW_SEED:
    _t0 = _time.time()
    cur.execute(
        f"INSERT INTO {catalog}.{target_schema}.{ds}_raw "
        f"SELECT * FROM {catalog}.{bronze_iceberg_schema}.{ds}"
    )
    # Confirm row count for telemetry
    cur.execute(f"SELECT COUNT(*) FROM {catalog}.{target_schema}.{ds}_raw")
    _n = (cur.fetchone() or [0])[0]
    print(f"[seed] {ds}_raw: {_n:>12,} rows ({_time.time() - _t0:5.1f}s)")
print(f"[seed] done in {_time.time() - _t_seed:.1f}s — bronze_raw fully populated with historical events")

# COMMAND ----------

# 3. Execute dt_create.sql — declares all 16 dynamic tables.
# The SQL file is a template with Python str.format placeholders. Substitute
# then split on `;` and execute each statement (Snowflake's connector doesn't
# multi-statement by default unless you set MULTI_STATEMENT_COUNT).
print(f"[ddl] reading {dt_create_sql_path}")
with open(dt_create_sql_path, "r") as _f:
    _ddl_template = _f.read()

# Strip comment-only lines FIRST so any stray {placeholder} in comments
# can't trip the substitution. Then do plain str.replace for the 5 real
# placeholders — avoids str.format()'s strict-all-keys behavior.
import re
_clean = re.sub(r"--[^\n]*\n", "\n", _ddl_template)

_subs = {
    "{catalog}":        catalog,
    "{schema}":         target_schema,
    "{staging_schema}": staging_schema,
    "{warehouse}":      warehouse,
    "{target_lag}":     f"'{target_lag}'",
}
for _k, _v in _subs.items():
    _clean = _clean.replace(_k, str(_v))
_ddl = _clean
_stmts = [s.strip() for s in _clean.split(";") if s.strip()]
print(f"[ddl] executing {len(_stmts)} DDL statements...")

_t_ddl = _time.time()
for i, _stmt in enumerate(_stmts, 1):
    # Pull out the table name from the CREATE OR REPLACE DYNAMIC TABLE statement
    # for the progress line.
    _m = re.search(r"DYNAMIC\s+TABLE\s+\S+\.\S+\.(\w+)", _stmt, re.IGNORECASE)
    _name = _m.group(1) if _m else f"stmt-{i}"
    _t0 = _time.time()
    cur.execute(_stmt)
    print(f"  [{i:2d}/{len(_stmts)}] {_name:35s} {_time.time() - _t0:5.1f}s")
print(f"[ddl] all DDL applied in {_time.time() - _t_ddl:.1f}s")

# COMMAND ----------

# 4. Emit batch_date_ls — match setup_sf.py exactly.
import datetime as dt
incr_start = dt.date(2016, 7, 6)
batches = [(incr_start + dt.timedelta(days=i)).isoformat() for i in range(incremental_n)]
dbutils.jobs.taskValues.set("batch_date_ls", batches)
print(f"emitted batch_date_ls: {len(batches)} dates, first={batches[0]}, last={batches[-1]}")

# COMMAND ----------

cur.close()
conn.close()
print("[done] Snowflake DT-variant setup complete.")
