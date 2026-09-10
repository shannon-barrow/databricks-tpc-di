# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# dependencies = [
#   "pyodbc",
#   "msal",
# ]
# ///
# MAGIC %md
# MAGIC # TPC-DI Augmented Incremental Teardown — Fabric DW variant
# MAGIC Drops:
# MAGIC - Warehouse schema `{wh_db}_{scale_factor}` (per-run target: CTAS baseline + bronze + silver + gold)
# MAGIC - OneLake dir `Files/.../_dailybatches/{wh_db}_{scale_factor}/` (per-batch CSV drop zone)
# MAGIC
# MAGIC **NOT removed**: the Lakehouse `staging_sf{sf}` OneLake tables (shared across runs,
# MAGIC reused by fabric_ss/nee), and the Databricks `main.tpcdi_incremental_staging_{sf}` Delta.

# COMMAND ----------

dbutils.widgets.text("wh_db",           "")
dbutils.widgets.dropdown("scale_factor","10", ["10","100","1000","5000","10000","20000"])
dbutils.widgets.text("tpcdi_directory", "/Volumes/main/tpcdi_raw_data/tpcdi_volume/")
dbutils.widgets.text("fabric_wh_host",  "skrtph5o6caeff4w6gdeuehp7q-wzykhydzalbexexpbi7qahqgrm.datawarehouse.fabric.microsoft.com")
dbutils.widgets.text("fabric_wh_name",  "tpcdi_fabric_dw")
dbutils.widgets.text("tenant_id",       "9f37a392-f0ae-4280-9796-f1864a10effc")
dbutils.widgets.text("fabric_workspace_id", "e0a370b6-0279-4bc2-92ef-0a3f001e068b")
dbutils.widgets.text("fabric_lakehouse_id", "54963c1f-bb06-4acd-8db3-b8b5056e81c3")

wh_db        = dbutils.widgets.get("wh_db")
scale_factor = dbutils.widgets.get("scale_factor")
wh_host      = dbutils.widgets.get("fabric_wh_host")
wh_name      = dbutils.widgets.get("fabric_wh_name")
tenant_id    = dbutils.widgets.get("tenant_id")
ws_id        = dbutils.widgets.get("fabric_workspace_id")
lh_id        = dbutils.widgets.get("fabric_lakehouse_id")

if not wh_db:
    raise ValueError("wh_db is required")
run_schema = f"{wh_db}_{scale_factor}".lower()

# COMMAND ----------

# MAGIC %run ./_fab_conn

# COMMAND ----------

conn = fab_connect(host=wh_host, database=wh_name, tenant_id=tenant_id,
                   label={"task": "teardown_fabric", "wh_db": wh_db, "scale_factor": scale_factor})
cur = conn.cursor()
try:
    # Fabric DW has no DROP SCHEMA CASCADE — drop the tables, then the schema.
    cur.execute("SELECT table_name FROM INFORMATION_SCHEMA.TABLES WHERE table_schema = ?", run_schema)
    tables = [r[0] for r in cur.fetchall()]
    for t in tables:
        cur.execute(f"DROP TABLE IF EXISTS [{run_schema}].[{t}]")
    cur.execute(f"IF SCHEMA_ID('{run_schema}') IS NOT NULL EXEC('DROP SCHEMA [{run_schema}]')")
    print(f"[ok] dropped {len(tables)} tables + schema [{run_schema}]")
except Exception as e:
    print(f"[warn] teardown DDL failed: {type(e).__name__}: {e}")
finally:
    conn.close()

# COMMAND ----------

# Wipe the per-batch OneLake drop zone (needs OneLake OAuth on the JVM conf).
_oauth = {
    "fs.azure.account.auth.type.onelake.dfs.fabric.microsoft.com": "OAuth",
    "fs.azure.account.oauth.provider.type.onelake.dfs.fabric.microsoft.com":
        "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id.onelake.dfs.fabric.microsoft.com":
        dbutils.secrets.get("tpcdi_fabric", "client_id"),
    "fs.azure.account.oauth2.client.secret.onelake.dfs.fabric.microsoft.com":
        dbutils.secrets.get("tpcdi_fabric", "client_secret"),
    "fs.azure.account.oauth2.client.endpoint.onelake.dfs.fabric.microsoft.com":
        f"https://login.microsoftonline.com/{tenant_id}/oauth2/token",
}
_hconf = spark._jsc.hadoopConfiguration()
for k, v in _oauth.items():
    spark.conf.set(k, v); _hconf.set(k, v)

batch_dir = (f"abfss://{ws_id}@onelake.dfs.fabric.microsoft.com/{lh_id}"
             f"/Files/augmented_incremental/_dailybatches/{run_schema}")
try:
    dbutils.fs.rm(batch_dir, recurse=True)
    print(f"[ok] removed {batch_dir}")
except Exception as e:
    print(f"[warn] batch-dir remove failed (may not exist): {e}")

print("Teardown complete. Lakehouse staging_sf{sf} + UC staging preserved.")
