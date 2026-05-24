# Databricks notebook source
# Per-run Snowflake setup for the **Dynamic Tables** variant. No dbt.
#
# Self-bootstrapping: on first run for a given SF, calls the
# `sf_staging_bootstrap` module to (a) refresh the catalog integration
# bearer token, (b) create the STAGING_SF{sf}_DBX federation schema with
# 28 federated Iceberg tables, and (c) CTAS the 22 silver/gold/ref
# tables into native STAGING_SF{sf} for cloning. On subsequent runs (or
# when the dbt variant has already bootstrapped), the bootstrap call is
# a no-op.
#
# Sequence:
#   1. ensure_staging_environment() — idempotent self-bootstrap.
#   2. CREATE OR REPLACE SCHEMA TPCDI_TEST.{wh_db}_{sf}
#   3. CLONE 13 reference / dim tables from native STAGING_SF{sf}.
#   4. materialize_bronze_into_schema() — CTAS the 7 bronze tables from
#      federated STAGING_SF{sf}_DBX into the per-run schema, all named
#      exactly bronze{name} (no `_raw` suffix). Tables get
#      CHANGE_TRACKING = TRUE so downstream silver/gold DTs can refresh
#      incrementally against them with no pass-through bronze DT layer.
#   5. Run `dt_create.sql` — declares the 16 dynamic tables.
#   6. Emit batch_date_ls task value for the parent's for_each loop.

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
dbutils.widgets.text("catalog_integration", "TPCDI_DBX_UC_SF10_INT",
                     "Snowflake catalog integration name pointing at the Databricks UC iceberg-rest endpoint")
dbutils.widgets.text("dbx_pat_secret_key", "",
                     "Optional: secret key (in `secret_scope`) holding a fresh Databricks PAT. Set on first bootstrap or after PAT rotation.")

catalog          = dbutils.widgets.get("catalog")
wh_db            = dbutils.widgets.get("wh_db")
scale_factor     = dbutils.widgets.get("scale_factor")
secret_scope     = dbutils.widgets.get("secret_scope")
warehouse        = dbutils.widgets.get("snowflake_warehouse")
target_lag       = dbutils.widgets.get("target_lag")
incremental_n    = int(dbutils.widgets.get("incremental_batches_to_run"))
dt_create_sql_path = dbutils.widgets.get("dt_create_sql_path")
catalog_integration = dbutils.widgets.get("catalog_integration")
dbx_pat_secret_key = dbutils.widgets.get("dbx_pat_secret_key")

if not wh_db:
    raise ValueError("wh_db is required")
if not warehouse:
    raise ValueError("snowflake_warehouse is required (DT refresh warehouse)")
if not dt_create_sql_path:
    raise ValueError(
        "dt_create_sql_path is required — the workflow builder passes the "
        "absolute workspace path to dt_create.sql (sibling of this notebook).")

target_schema   = f"{wh_db}_{scale_factor}"
staging_schema  = f"STAGING_SF{scale_factor}"               # native, clonable (silver/gold + reference)
print(f"target  = {catalog}.{target_schema}")
print(f"staging = {catalog}.{staging_schema}")
print(f"DT warehouse = {warehouse}, target_lag (leaves) = {target_lag}")

# COMMAND ----------

# MAGIC %run ../_sf_conn

# COMMAND ----------

# Add the parent snowflake/ dir to sys.path so we can import the bootstrap
# module. dt_create_sql_path is .../snowflake/dynamic_tables/dt_create.sql,
# so its parent's parent is .../snowflake/ — where sf_staging_bootstrap.py lives.
import sys, os
_snowflake_dir = os.path.dirname(os.path.dirname(dt_create_sql_path))
if _snowflake_dir not in sys.path:
    sys.path.insert(0, _snowflake_dir)
import sf_staging_bootstrap as bootstrap
print(f"[ok] bootstrap module loaded from {_snowflake_dir}")

# COMMAND ----------

# Single primary connection for the whole setup. The bootstrap module's
# parallel CTAS needs to open its own connections (snowflake-connector is
# not thread-safe across workers), so we pass it a factory closure.
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

def _new_conn():
    return sf_connect(
        database=catalog, secret_scope=secret_scope, warehouse=warehouse,
        query_tag={"scale_factor": scale_factor, "task": "setup_sf_dt:ctas"},
    )

# Optional fresh PAT for refreshing the catalog integration's bearer token.
# Only needed on first bootstrap or after PAT rotation (catalog integration
# silently returns 403 once its embedded PAT expires).
_new_pat = None
if dbx_pat_secret_key:
    try:
        _new_pat = dbutils.secrets.get(scope=secret_scope, key=dbx_pat_secret_key)
        print(f"[ok] picked up PAT from secret_scope.{dbx_pat_secret_key}")
    except Exception as _e:
        print(f"[warn] dbx_pat_secret_key={dbx_pat_secret_key!r} not in secret_scope — proceeding without token refresh")

# COMMAND ----------

# 1. Self-bootstrap — ensure the federation + native staging schemas exist
# with all expected tables. Idempotent: returns immediately if both are
# already complete.
_t0 = __import__("time").time()
boot = bootstrap.ensure_staging_environment(
    conn,
    catalog=catalog,
    scale_factor=scale_factor,
    catalog_integration=catalog_integration,
    new_pat=_new_pat,
    new_connection=_new_conn,
    parallel=8,
)
print(f"[bootstrap] result={boot}  ({__import__('time').time() - _t0:.1f}s total)")

# COMMAND ----------

# 2. Create the per-run benchmark schema.
cur = conn.cursor()
cur.execute(f"CREATE DATABASE IF NOT EXISTS {catalog}")
cur.execute(f"USE DATABASE {catalog}")
cur.execute(f"CREATE OR REPLACE SCHEMA {catalog}.{target_schema}")
cur.execute(f"USE SCHEMA {catalog}.{target_schema}")
print(f"[ok] schema {catalog}.{target_schema} ready")

# COMMAND ----------

# 3. CLONE the 13 reference / silver tables that don't change per batch from
# native STAGING_SF{sf}. These stay as REGULAR tables (no DT semantics
# needed) — the DTs read from them as static joins. We EXCLUDE the 8
# tables that become dynamic tables (dimcustomer, dimaccount, dimtrade,
# factwatches, factcashbalances, factholdings, factmarkethistory,
# currentaccountbalances) and the bronze tables (handled in step 4).
CLONE_TABLES = [
    "taxrate", "dimdate", "industry", "tradetype", "dimbroker",
    "dimsecurity", "statustype", "dimcompany", "dimtime", "financial",
    "companyyeareps", "batchdate", "cashtransactionhistorical",
]

print(f"[clone] {len(CLONE_TABLES)} reference tables (zero-copy)...")
import time as _time
_t0 = _time.time()
for tbl in CLONE_TABLES:
    cur.execute(
        f"CREATE OR REPLACE TABLE {catalog}.{target_schema}.{tbl} "
        f"CLONE {catalog}.{staging_schema}.{tbl}"
    )
print(f"[clone] done in {_time.time() - _t0:.1f}s")

# COMMAND ----------

# 4. CTAS the 7 bronze tables from federated Iceberg into the per-run
# schema. Per the DT-variant principle: bronze data stays Iceberg on the
# staging side, never materialized in STAGING_SF{sf}. Each per-run schema
# gets a fresh CTAS — type-preserving, no driver-side data movement.
bootstrap.materialize_bronze_into_schema(
    conn,
    catalog=catalog,
    scale_factor=scale_factor,
    target_schema=target_schema,
    new_connection=_new_conn,
    parallel=7,
)

# COMMAND ----------

# 5. Execute dt_create.sql — declares all 16 dynamic tables.
# The SQL file is a template with `{placeholder}` substitutions for
# catalog/schema/warehouse/target_lag. Strip comment lines first so any
# stray `{...}` in comments doesn't trip the substitution, then split on
# `;` and execute statements one at a time (snowflake-connector doesn't
# multi-statement by default).
print(f"[ddl] reading {dt_create_sql_path}")
with open(dt_create_sql_path, "r") as _f:
    _ddl_template = _f.read()

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
_stmts = [s.strip() for s in _clean.split(";") if s.strip()]
print(f"[ddl] executing {len(_stmts)} DDL statements...")

_t_ddl = _time.time()
for i, _stmt in enumerate(_stmts, 1):
    _m = re.search(r"DYNAMIC\s+TABLE\s+\S+\.\S+\.(\w+)", _stmt, re.IGNORECASE)
    _name = _m.group(1) if _m else f"stmt-{i}"
    _t0 = _time.time()
    cur.execute(_stmt)
    print(f"  [{i:2d}/{len(_stmts)}] {_name:35s} {_time.time() - _t0:5.1f}s")
print(f"[ddl] all DDL applied in {_time.time() - _t_ddl:.1f}s")

# COMMAND ----------

# 6. Emit batch_date_ls — match setup_sf.py exactly.
import datetime as dt
incr_start = dt.date(2016, 7, 6)
batches = [(incr_start + dt.timedelta(days=i)).isoformat() for i in range(incremental_n)]
dbutils.jobs.taskValues.set("batch_date_ls", batches)
print(f"emitted batch_date_ls: {len(batches)} dates, first={batches[0]}, last={batches[-1]}")

# COMMAND ----------

cur.close()
conn.close()
print("[done] Snowflake DT-variant setup complete.")
