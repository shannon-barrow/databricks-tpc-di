# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "psycopg2-binary",
# ]
# ///
# MAGIC %md
# MAGIC # TPC-DI Augmented Incremental Teardown — Redshift variant
# MAGIC
# MAGIC Drops:
# MAGIC - Redshift schema `{wh_db}_{scale_factor}` (per-run target — bronze + silver + gold + setup-created CTAS)
# MAGIC - UC volume dir `_dailybatches/{wh_db}_{scale_factor}/` (the per-batch CSV drop zone)
# MAGIC
# MAGIC **NOT removed**:
# MAGIC - `tpcdi_staging_sf{scale_factor}` — one-time per-SF staging schema is shared across runs.
# MAGIC - `main.tpcdi_incremental_staging_{scale_factor}` Databricks Delta staging.
# MAGIC - The shared `_staging/sf={sf}/` dir.

# COMMAND ----------

dbutils.widgets.text("database",         "dev")
dbutils.widgets.text("wh_db",            "")
dbutils.widgets.dropdown("scale_factor", "10",
                         ["10", "100", "1000", "5000", "10000", "20000"])
dbutils.widgets.text("tpcdi_directory",  "/Volumes/main/tpcdi_raw_data/tpcdi_volume/")
dbutils.widgets.text("secret_catalog",   "main", "Unity Catalog catalog holding the secret schema")
dbutils.widgets.text("secret_schema",    "tpcdi_redshift", "Unity Catalog schema holding the credentials")

database         = dbutils.widgets.get("database")
wh_db            = dbutils.widgets.get("wh_db")
scale_factor     = dbutils.widgets.get("scale_factor")
tpcdi_directory  = dbutils.widgets.get("tpcdi_directory")
secret_catalog   = dbutils.widgets.get("secret_catalog")
secret_schema    = dbutils.widgets.get("secret_schema")

if not wh_db:
    raise ValueError("wh_db is required")

target_schema  = f"{wh_db}_{scale_factor}".lower()
batches_dir    = f"{tpcdi_directory}augmented_incremental/_dailybatches/{target_schema}"

# COMMAND ----------

# MAGIC %run ./_rs_conn

# COMMAND ----------

conn = rs_connect(
    database=database, secret_catalog=secret_catalog, secret_schema=secret_schema,
    query_group={"wh_db": wh_db, "scale_factor": scale_factor, "task": "teardown_rs"},
)

# COMMAND ----------

with conn.cursor() as cur:
    try:
        cur.execute(f'DROP SCHEMA IF EXISTS "{target_schema}" CASCADE')
        print(f"[ok] dropped schema {database}.{target_schema}")
    except Exception as e:
        print(f"[warn] DROP SCHEMA failed: {type(e).__name__}: {e}")

conn.close()

# COMMAND ----------

# Wipe the per-batch CSV drop zone. Shared `_staging/sf={sf}/` is intentionally
# left intact — it's the source for subsequent runs.
import os
if os.path.exists(batches_dir):
    print(f"removing {batches_dir}...")
    dbutils.fs.rm(batches_dir, recurse=True)
    print("[ok] removed batches dir")
else:
    print(f"{batches_dir} does not exist; nothing to remove")

# COMMAND ----------

print()
print(f"Teardown complete. Staging data at "
      f"{tpcdi_directory}augmented_incremental/_staging/ preserved.")
print(f"Shared Redshift staging schema tpcdi_staging_sf{scale_factor} preserved.")
