# Databricks notebook source
# MAGIC %md
# MAGIC # TPC-DI Augmented Incremental Teardown
# MAGIC
# MAGIC Drops the run's schema and clears its Autoloader directories.
# MAGIC Wired into the parent job behind a `delete_when_finished_TRUE_FALSE`
# MAGIC condition gate, so it only fires when the user opts in (or
# MAGIC manually re-runs the parent with that parameter set to `TRUE`).
# MAGIC
# MAGIC Removed:
# MAGIC - `{catalog}.{wh_db}_{scale_factor}` schema (CASCADE)
# MAGIC - `_dailybatches/{wh_db}_{scale_factor}/` (Autoloader watch dir)
# MAGIC - `_checkpoints/{wh_db}_{scale_factor}/` (Autoloader state)
# MAGIC
# MAGIC **NOT removed** — the shared `_staging/sf={sf}/` is preserved so
# MAGIC other users / future runs can re-clone from it.

# COMMAND ----------

import os

dbutils.widgets.dropdown("scale_factor", "10", ["10", "100", "1000", "5000", "10000", "20000"])
dbutils.widgets.text("tpcdi_directory", "/Volumes/tpcdi/tpcdi_raw_data/tpcdi_volume/")
dbutils.widgets.text("catalog", "tpcdi")
dbutils.widgets.text("wh_db", "")

catalog         = dbutils.widgets.get("catalog")
scale_factor    = dbutils.widgets.get("scale_factor")
tpcdi_directory = dbutils.widgets.get("tpcdi_directory")
wh_db           = dbutils.widgets.get("wh_db")
tgt_db          = f"{wh_db}_{scale_factor}"
batches_dir     = f"{tpcdi_directory}augmented_incremental/_dailybatches/{tgt_db}"
checkpoint_dir  = f"{tpcdi_directory}augmented_incremental/_checkpoints/{tgt_db}"

# COMMAND ----------

target_schema = f"{catalog}.{tgt_db}"
print(f"Dropping {target_schema} CASCADE...")
spark.sql(f"DROP SCHEMA IF EXISTS {target_schema} CASCADE")

# COMMAND ----------

for path in (batches_dir, checkpoint_dir):
  if os.path.exists(path):
    print(f"Removing {path}...")
    dbutils.fs.rm(path, recurse=True)
  else:
    print(f"{path} does not exist; skipping.")

print()
print(f"Cleanup complete. Staging data at "
      f"{tpcdi_directory}augmented_incremental/_staging/ preserved.")
