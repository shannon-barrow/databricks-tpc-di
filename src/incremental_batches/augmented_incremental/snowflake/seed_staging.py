# Databricks notebook source
# ONE-TIME (per scale factor) seed of the Snowflake "golden" staging schema.
# Copies `main.tpcdi_incremental_staging_{sf}.{table}` from Databricks to
# `TPCDI_TEST.STAGING_SF{sf}.{table}` on Snowflake via the Snowflake-Spark
# connector. Once seeded, per-run setup (setup_sf.py) just CLONEs from
# this schema — zero-copy and instant.
#
# Run this manually from a notebook on an interactive cluster that has the
# snowflake-spark connector installed (it's pre-installed on standard
# Databricks runtimes). Re-run only when the underlying Databricks staging
# data changes for a given SF.

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Databricks catalog (source)")
dbutils.widgets.dropdown("scale_factor","10", ["10","100","1000","5000","10000","20000"], "scale_factor")
dbutils.widgets.text("snowflake_database", "TPCDI_TEST", "Snowflake DB (sink)")
dbutils.widgets.text("secret_scope",       "tpcdi_snowflake", "Databricks secret scope (creds for Snowflake)")

src_catalog       = dbutils.widgets.get("catalog")
scale_factor      = dbutils.widgets.get("scale_factor")
sf_db             = dbutils.widgets.get("snowflake_database")
secret_scope      = dbutils.widgets.get("secret_scope")

src_schema = f"tpcdi_incremental_staging_{scale_factor}"
sf_schema  = f"STAGING_SF{scale_factor}"
print(f"src = {src_catalog}.{src_schema}")
print(f"sink = {sf_db}.{sf_schema}")

# COMMAND ----------

def _secret(name, default=None):
    try: return dbutils.secrets.get(scope=secret_scope, key=name)
    except Exception: return default

sf_opts = {
    "sfUrl":       f"{_secret('account')}.snowflakecomputing.com",
    "sfUser":      _secret("user"),
    "sfRole":      _secret("role") or "ACCOUNTADMIN",
    "sfWarehouse": _secret("warehouse") or "BARROW_XS_GEN2",
    "sfDatabase":  sf_db,
    "sfSchema":    sf_schema,
    # Use keypair auth (more reliable than password for service users)
    "pem_private_key": _secret("private_key"),
}

# COMMAND ----------

# These mirror the reference + dimension tables our augmented_incremental dbt
# project sources from `run_schema`. Each gets a Spark→Snowflake write.
STAGING_TABLES = [
    "taxrate", "dimdate", "industry", "tradetype", "dimbroker",
    "dimsecurity", "statustype", "dimcompany", "dimtime", "financial",
    "companyyeareps", "currentaccountbalances", "dimaccount",
]

# Ensure the target schema exists on Snowflake.
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")  # speedup
from pyspark.sql.functions import expr
# Inline DDL via the Snowflake connector's Utils.runQuery
sc._jvm.net.snowflake.spark.snowflake.Utils.runQuery(
    {k: v for k, v in sf_opts.items() if v is not None},
    f"CREATE SCHEMA IF NOT EXISTS {sf_db}.{sf_schema}",
)
print(f"[ok] {sf_db}.{sf_schema} exists")

# COMMAND ----------

import time
for t in STAGING_TABLES:
    src_fq = f"{src_catalog}.{src_schema}.{t}"
    t0 = time.time()
    try:
        df = spark.read.table(src_fq)
        n  = df.count()  # so we can log row counts
    except Exception as e:
        print(f"[skip] {src_fq} not found: {e}")
        continue
    (df.write.format("snowflake")
        .options(**{k: v for k, v in sf_opts.items() if v is not None})
        .option("dbtable", t)
        .mode("overwrite")
        .save())
    print(f"[seed] {t}: {n:,} rows  ({time.time()-t0:.1f}s)")

# COMMAND ----------

print(f"[done] {sf_db}.{sf_schema} seeded. Per-run setup_sf.py can now CLONE from it.")
