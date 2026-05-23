# Databricks notebook source
# ONE-TIME per scale factor — materialize the 7 bronze staging tables into
# TPCDI_TEST.STAGING_SF{sf} (native Snowflake) by CTAS from the federated
# UC Iceberg catalog-linked schema TPCDI_TEST.STAGING_SF{sf}_DBX.
#
# Prereq: augmented_staging job must have been run with
# `generate_bronze_staging=YES` at this SF. That produces:
#   main.tpcdi_incremental_staging_{sf}.bronze<dataset>  (Iceberg-UniForm Delta)
# which is mirrored in Snowflake by the federated catalog as:
#   TPCDI_TEST.STAGING_SF{sf}_DBX.bronze<dataset>
#
# This notebook CTAS's those federated Iceberg tables into native Snowflake
# tables at TPCDI_TEST.STAGING_SF{sf}.bronze<dataset>, then setup_sf_dt
# INSERT INTO bronze*_raw SELECT FROM those native tables (clone-friendly).
#
# Why CTAS (not direct CLONE)? Federated Iceberg tables can't be CLONEd
# into Snowflake-native form. CTAS is the canonical path: it produces a
# native materialization that subsequent CLONEs into per-run schemas are
# zero-copy.
#
# Re-run only when the Databricks-side bronze staging tables change
# (i.e. after re-running augmented_staging with generate_bronze_staging=YES
# at this SF). Otherwise the STAGING_SF{sf}.bronze* tables are reusable
# across every DT benchmark run.

# COMMAND ----------

dbutils.widgets.text("catalog",         "TPCDI_TEST", "Snowflake database (treated as `catalog`)")
dbutils.widgets.dropdown("scale_factor","10", ["10","100","1000","5000","10000","20000"], "scale_factor")
dbutils.widgets.text("secret_scope",    "tpcdi_snowflake", "Databricks secret scope")
dbutils.widgets.text("snowflake_warehouse", "BARROW_LARGE_GEN2",
                     "WH for the one-time CTAS — Large is overkill at SF=10 but matches the SF/20k onetime convention.")

catalog          = dbutils.widgets.get("catalog")
scale_factor     = dbutils.widgets.get("scale_factor")
secret_scope     = dbutils.widgets.get("secret_scope")
warehouse        = dbutils.widgets.get("snowflake_warehouse") or None

source_schema    = f"STAGING_SF{scale_factor}_DBX"   # federated Iceberg (read-only)
target_schema    = f"STAGING_SF{scale_factor}"        # native materialized staging (shared with onetime_stg_snowflake_tables)
print(f"source = {catalog}.{source_schema}   (federated UC Iceberg)")
print(f"target = {catalog}.{target_schema}   (native Snowflake bronze staging)")

# COMMAND ----------

# MAGIC %run ./_sf_conn

# COMMAND ----------

conn = sf_connect(
    database=catalog,
    secret_scope=secret_scope,
    warehouse=warehouse,
    query_tag={
        "scale_factor": scale_factor,
        "task":         "onetime_stg_snowflake_dt_bronze_tables",
    },
)
print(f"[ok] connected to Snowflake; warehouse = {warehouse or '(from secret_scope default)'}")
cur = conn.cursor()

cur.execute(f"CREATE DATABASE IF NOT EXISTS {catalog}")
cur.execute(f"USE DATABASE {catalog}")
cur.execute(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{target_schema}")
cur.execute(f"USE SCHEMA {catalog}.{target_schema}")
print(f"[ok] schema {catalog}.{target_schema} ready (not REPLACE — preserves existing dim/fact staging)")

# COMMAND ----------

# The 7 bronze tables matching the bronze*_raw tables the DT variant uses.
# bronzedailymarket is INCLUDED (with cluster key dm_date) because the DT
# variant doesn't separate it into a _raw table — factmarkethistory's DT
# reads from this directly per dt_create.sql.
BRONZE_TABLES = [
    "bronzeaccount",
    "bronzecashtransaction",
    "bronzecustomer",
    "bronzedailymarket",
    "bronzeholdings",
    "bronzetrade",
    "bronzewatches",
]

CLUSTER_KEYS = {
    "bronzeaccount":         "update_dt",
    "bronzecashtransaction": "event_dt",
    "bronzecustomer":        "update_dt",
    "bronzedailymarket":     "dm_date",
    "bronzeholdings":        "event_dt",
    "bronzetrade":           "event_dt",
    "bronzewatches":         "event_dt",
}

# COMMAND ----------

# Parallel CTAS — each worker opens its own Snowflake connection
# (snowflake-connector connections aren't thread-safe).
import concurrent.futures as _cf
import time as _time

def _materialize_one(table_name: str) -> tuple[str, float, int]:
    _t0 = _time.time()
    _conn = sf_connect(
        database=catalog,
        secret_scope=secret_scope,
        warehouse=warehouse,
        query_tag={
            "scale_factor": scale_factor,
            "task":         "onetime_stg_snowflake_dt_bronze_tables",
            "table":        table_name,
        },
    )
    try:
        _cluster = f"\n  CLUSTER BY ({CLUSTER_KEYS[table_name]})" if table_name in CLUSTER_KEYS else ""
        with _conn.cursor() as _c:
            _c.execute(
                f"CREATE OR REPLACE TABLE {catalog}.{target_schema}.{table_name}"
                f"{_cluster} AS "
                f"SELECT * FROM {catalog}.{source_schema}.{table_name}"
            )
            _c.execute(
                f"SELECT ROW_COUNT FROM {catalog}.INFORMATION_SCHEMA.TABLES "
                f"WHERE TABLE_SCHEMA = '{target_schema}' AND TABLE_NAME = '{table_name.upper()}'"
            )
            _n = (_c.fetchone() or [0])[0] or 0
        return (table_name, _time.time() - _t0, int(_n))
    finally:
        _conn.close()

_t_setup = _time.time()
print(f"[parallel] materializing {len(BRONZE_TABLES)} bronze tables on {warehouse or '(secret default)'} (7 concurrent)...")
results = []
with _cf.ThreadPoolExecutor(max_workers=7) as _ex:
    _futures = {_ex.submit(_materialize_one, t): t for t in BRONZE_TABLES}
    for _f in _cf.as_completed(_futures):
        try:
            _name, _wall, _n = _f.result()
            _key = f" (CLUSTER BY {CLUSTER_KEYS[_name]})"
            print(f"[ctas]  {_name:30s} {_wall:6.1f}s  rows={_n:>15,d}{_key}")
            results.append((_name, _wall, _n))
        except Exception as _e:
            _name = _futures[_f]
            print(f"[FAIL] {_name:30s}  {type(_e).__name__}: {_e}")
            raise
total_rows = sum(n for _,_,n in results)
print()
print(f"[done] materialized {len(results)} bronze tables in {_time.time() - _t_setup:.1f}s, total rows = {total_rows:,}")
print(f"       {catalog}.{target_schema}.bronze* are now ready for setup_sf_dt to seed bronze*_raw from.")

# COMMAND ----------

cur.close()
conn.close()
