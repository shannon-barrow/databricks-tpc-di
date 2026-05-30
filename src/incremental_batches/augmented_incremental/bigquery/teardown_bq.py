# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "google-cloud-bigquery",
# ]
# ///
# MAGIC %md
# MAGIC # TPC-DI Augmented Incremental Teardown — BigQuery variant
# MAGIC
# MAGIC BQ-side equivalent of the cluster `teardown.py`. Drops:
# MAGIC - BQ dataset `{catalog}.{wh_db}_{scale_factor}` (per-run target — bronze + silver + gold + setup-created tables)
# MAGIC - BQ dataset `{catalog}.{wh_db}_{scale_factor}_bronze` (external tables pointing at the per-batch CSV dir)
# MAGIC - UC volume dir `_dailybatches/{wh_db}_{scale_factor}/` (the per-batch CSV drop zone)
# MAGIC
# MAGIC **NOT removed**:
# MAGIC - `tpcdi_staging_sf{scale_factor}` — one-time per-SF staging dataset is shared across runs. Drop manually if you need to free space.
# MAGIC - `main.tpcdi_incremental_staging_{scale_factor}` Databricks Delta staging — owned by the augmented_staging workflow.
# MAGIC - The shared `_staging/sf={sf}/` dir — preserved so subsequent runs can re-clone.
# MAGIC
# MAGIC Wired into the parent job behind a `delete_when_finished_TRUE_FALSE`
# MAGIC condition gate (see augmented_bigquery.py).

# COMMAND ----------

dbutils.widgets.text("catalog",          "databricks-sandbox-perfeng", "BigQuery project")
dbutils.widgets.text("wh_db",            "",                            "wh_db prefix")
dbutils.widgets.dropdown("scale_factor", "10",
                         ["10", "100", "1000", "5000", "10000", "20000"])
dbutils.widgets.text("tpcdi_directory",  "/Volumes/main/tpcdi_raw_data/tpcdi_volume/")
dbutils.widgets.text("secret_scope",     "tpcdi_bigquery")
dbutils.widgets.text("bq_location",      "us-central1")

bq_project       = dbutils.widgets.get("catalog")
wh_db            = dbutils.widgets.get("wh_db")
scale_factor     = dbutils.widgets.get("scale_factor")
tpcdi_directory  = dbutils.widgets.get("tpcdi_directory")
secret_scope     = dbutils.widgets.get("secret_scope")
bq_location      = dbutils.widgets.get("bq_location")

if not wh_db:
    raise ValueError("wh_db is required")

target_dataset = f"{wh_db}_{scale_factor}"
bronze_dataset = f"{target_dataset}_bronze"
batches_dir    = f"{tpcdi_directory}augmented_incremental/_dailybatches/{target_dataset}"

# COMMAND ----------

# MAGIC %run ./_bq_conn

# COMMAND ----------

client = bq_connect(
    project=bq_project,
    location=bq_location,
    secret_scope=secret_scope,
    query_label={"wh_db": wh_db, "scale_factor": scale_factor, "task": "teardown_bq"},
)

# COMMAND ----------

# Drop both datasets (delete_contents=True removes all tables in one call).
for ds in (target_dataset, bronze_dataset):
    fq = f"{bq_project}.{ds}"
    try:
        client.delete_dataset(fq, delete_contents=True, not_found_ok=True)
        print(f"[ok] dropped dataset {fq}")
    except Exception as e:
        print(f"[warn] delete_dataset({fq}) failed: {type(e).__name__}: {e}")

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
print(f"Shared BQ staging dataset {bq_project}.tpcdi_staging_sf{scale_factor} preserved.")
