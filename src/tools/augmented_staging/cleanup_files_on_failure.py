# Databricks notebook source
# MAGIC %md
# MAGIC # Cleanup — Per-Day Staging Dirs (only on file-gen failure)
# MAGIC
# MAGIC Wired with `run_if=AT_LEAST_ONE_FAILED` against the 7 `stage_files_*`
# MAGIC tasks. Fires only when at least one of those tasks failed — in that
# MAGIC case we delete all 730 per-day directories under
# MAGIC `augmented_incremental/_staging/sf={sf}/` so the next run's
# MAGIC early-exit check (which counts date dirs) doesn't false-positive
# MAGIC and skip the rebuild.
# MAGIC
# MAGIC When all 7 stage_files succeed, this task is SKIPPED (its `run_if`
# MAGIC condition isn't met) and the staging dirs remain intact.

# COMMAND ----------

dbutils.widgets.dropdown("scale_factor", "10",
                         ["10", "100", "1000", "5000", "10000", "20000"])
dbutils.widgets.text("catalog", "main")

scale_factor = dbutils.widgets.get("scale_factor").strip()
catalog      = dbutils.widgets.get("catalog").strip()

staging_dir = (f"/Volumes/{catalog}/tpcdi_raw_data/tpcdi_volume/"
               f"augmented_incremental/_staging/sf={scale_factor}")

# COMMAND ----------

print(f"At least one stage_files task failed — cleaning up {staging_dir}")
try:
    dbutils.fs.rm(staging_dir, recurse=True)
    print(f"Deleted {staging_dir}")
except Exception as e:
    print(f"rm {staging_dir} failed: {type(e).__name__}: {e}")
