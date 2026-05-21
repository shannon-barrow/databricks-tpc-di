# Databricks notebook source
# ONE-TIME per scale factor — materialize the 22 staging tables into
# TPCDI_TEST.STAGING_SF{sf} (native Snowflake) by CTAS from the
# federated UC Iceberg catalog-linked schema
# TPCDI_TEST.STAGING_SF{sf}_DBX.
#
# Why one-time: external-catalog Iceberg tables can't be CLONEd into
# Snowflake-native (Snowflake refuses), so each benchmark run that
# wants native targets would otherwise have to re-CTAS — ~25 min at
# SF=20k. Materializing once into STAGING_SF{sf} (with Snowflake
# clustering) lets every subsequent per-run setup_sf use zero-copy
# CLONE instead (seconds).
#
# Sequence:
#   1. CREATE OR REPLACE SCHEMA TPCDI_TEST.STAGING_SF{sf}
#   2. CTAS 22 tables from TPCDI_TEST.STAGING_SF{sf}_DBX, applying the
#      CLUSTER BY each dim/fact table uses on the Databricks Liquid
#      side. Runs in parallel (8 workers, one Snowflake conn each).
#
# Re-run only when the upstream Databricks staging data changes for a
# given SF (i.e. after regenerating the augmented_staging job at that
# SF). Otherwise STAGING_SF{sf} stays as the stable, zero-copy-clonable
# source for every benchmark run on that SF.
#
# Auth: reads creds from the {secret_scope} Databricks secret scope
# (see ./_sf_conn.py).

# COMMAND ----------

dbutils.widgets.text("catalog",         "TPCDI_TEST", "Snowflake database (treated as `catalog`)")
dbutils.widgets.dropdown("scale_factor","10", ["10","100","1000","5000","10000","20000"], "scale_factor")
dbutils.widgets.text("secret_scope",    "tpcdi_snowflake", "Databricks secret scope")
dbutils.widgets.text("snowflake_warehouse", "BARROW_LARGE_GEN2",
                     "WH for the one-time CTAS — defaults to Large since the SF=20k materialization is resource-heavy.")

catalog          = dbutils.widgets.get("catalog")
scale_factor     = dbutils.widgets.get("scale_factor")
secret_scope     = dbutils.widgets.get("secret_scope")
warehouse        = dbutils.widgets.get("snowflake_warehouse") or None

source_schema    = f"STAGING_SF{scale_factor}_DBX"   # federated Iceberg (read-only)
target_schema    = f"STAGING_SF{scale_factor}"        # native materialized staging
print(f"source = {catalog}.{source_schema}   (federated UC Iceberg)")
print(f"target = {catalog}.{target_schema}   (native, clustered, clonable)")

# COMMAND ----------

# MAGIC %run ./_sf_conn

# COMMAND ----------

conn = sf_connect(
    database=catalog,
    secret_scope=secret_scope,
    warehouse=warehouse,
    query_tag={
        "scale_factor": scale_factor,
        "task":         "onetime_stg_snowflake_tables",
    },
)
print(f"[ok] connected to Snowflake; warehouse = {warehouse or '(from secret_scope default)'}")
cur = conn.cursor()

cur.execute(f"CREATE DATABASE IF NOT EXISTS {catalog}")
cur.execute(f"USE DATABASE {catalog}")
cur.execute(f"CREATE OR REPLACE SCHEMA {catalog}.{target_schema}")
cur.execute(f"USE SCHEMA {catalog}.{target_schema}")
print(f"[ok] schema {catalog}.{target_schema} ready (replaced)")

# COMMAND ----------

# Match the staging tables the Databricks-side augmented_staging job
# produces. Every table here will be available for setup_sf to CLONE.
STAGING_TABLES = [
    "taxrate", "dimdate", "industry", "tradetype", "dimbroker",
    "dimsecurity", "statustype", "dimcompany", "dimtime", "financial",
    "companyyeareps", "currentaccountbalances", "dimaccount",
    "dimcustomer", "dimtrade", "factwatches", "factcashbalances",
    "factholdings", "factmarkethistory", "bronzedailymarket",
    "cashtransactionhistorical", "batchdate",
]

# Cluster keys mirror the Databricks-side Liquid layout (see
# historical/*.sql `CLUSTER BY (...)` + setup_dbt.py / setup.py DEEP
# CLONE targets). Setting CLUSTER BY here means subsequent CLONEs into
# per-run schemas inherit the cluster key (and Snowflake auto-clustering
# stays active on the clone).
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

# COMMAND ----------

# Parallel CTAS — each worker opens its own Snowflake connection
# (snowflake-connector connections aren't thread-safe). max_workers=8
# saturates a Large WH's max_concurrency_level.
import concurrent.futures as _cf
import time as _time

def _materialize_one(table_name: str) -> tuple[str, float, int]:
    """CTAS one table from the federated Iceberg source into the native
    target schema. Returns (table_name, wall_sec, row_count)."""
    _t0 = _time.time()
    _conn = sf_connect(
        database=catalog,
        secret_scope=secret_scope,
        warehouse=warehouse,
        query_tag={
            "scale_factor": scale_factor,
            "task":         "onetime_stg_snowflake_tables",
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
            # row count — INFORMATION_SCHEMA is the cheapest path
            _c.execute(
                f"SELECT ROW_COUNT FROM {catalog}.INFORMATION_SCHEMA.TABLES "
                f"WHERE TABLE_SCHEMA = '{target_schema}' AND TABLE_NAME = '{table_name.upper()}'"
            )
            _n = (_c.fetchone() or [0])[0] or 0
        return (table_name, _time.time() - _t0, int(_n))
    finally:
        _conn.close()

_t_setup = _time.time()
print(f"[parallel] materializing {len(STAGING_TABLES)} tables on {warehouse or '(secret default)'} (8 concurrent)...")
results = []
with _cf.ThreadPoolExecutor(max_workers=8) as _ex:
    _futures = {_ex.submit(_materialize_one, t): t for t in STAGING_TABLES}
    for _f in _cf.as_completed(_futures):
        try:
            _name, _wall, _n = _f.result()
            _key = f" (CLUSTER BY {CLUSTER_KEYS[_name]})" if _name in CLUSTER_KEYS else ""
            print(f"[ctas]  {_name:30s} {_wall:6.1f}s  rows={_n:>15,d}{_key}")
            results.append((_name, _wall, _n))
        except Exception as _e:
            _name = _futures[_f]
            print(f"[FAIL] {_name:30s}  {type(_e).__name__}: {_e}")
            raise
total_rows = sum(n for _,_,n in results)
print()
print(f"[done] materialized {len(results)} tables in {_time.time() - _t_setup:.1f}s, total rows = {total_rows:,}")
print(f"       {catalog}.{target_schema} is now clonable by setup_sf via zero-copy CLONE.")

# COMMAND ----------

cur.close()
conn.close()
