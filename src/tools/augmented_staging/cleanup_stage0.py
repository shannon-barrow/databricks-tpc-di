# Databricks notebook source
# MAGIC %md
# MAGIC # Cleanup — Stage 0 leftovers
# MAGIC
# MAGIC Drops the 7 temp Delta tables in `tpcdi_raw_data` created by stage 0,
# MAGIC and removes the Spark generator's per-Batch and parquet-staging dirs
# MAGIC under `augmented_incremental/_staging/sf={sf}/` (`Batch1/2/3`,
# MAGIC `_staging/`). Those are intermediate outputs of the spark generator
# MAGIC that the augmented benchmark doesn't read — historical/* SQL reads
# MAGIC from the temp Delta tables (now dropped here too) and bronze ingest
# MAGIC reads from the per-date dirs which we leave intact.
# MAGIC
# MAGIC Runs unconditionally (`ALL_DONE`) at the end of the workflow once
# MAGIC stage_tables + stage_files have consumed the data.

# COMMAND ----------

dbutils.widgets.dropdown("scale_factor", "10",
                         ["10", "100", "1000", "5000", "10000", "20000"])
dbutils.widgets.text("catalog", "main")

scale_factor = dbutils.widgets.get("scale_factor").strip()
catalog      = dbutils.widgets.get("catalog").strip()

# COMMAND ----------

for tbl in ("customermgmt", "trade", "tradehistory", "cashtransaction",
            "holdinghistory", "watchhistory", "dailymarket"):
    spark.sql(f"DROP TABLE IF EXISTS {catalog}.tpcdi_raw_data.{tbl}{scale_factor}")

# COMMAND ----------

# Spark-generator intermediate outputs the benchmark doesn't consume.
# Leaving them around bloats the volume by ~5-10x the per-date file
# footprint (FINWIRE files alone are ~10GB at SF=5000).
staging_root = (f"/Volumes/{catalog}/tpcdi_raw_data/tpcdi_volume/"
                f"augmented_incremental/_staging/sf={scale_factor}")
for sub in ("Batch1", "Batch2", "Batch3", "_staging"):
    path = f"{staging_root}/{sub}"
    try:
        dbutils.fs.rm(path, recurse=True)
        print(f"Removed {path}")
    except Exception as e:
        print(f"rm {path} skipped: {type(e).__name__}: {e}")
