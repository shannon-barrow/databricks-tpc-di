# Databricks notebook source
import os
import shutil
import concurrent.futures
import requests
from datetime import date, timedelta

# COMMAND ----------

dbutils.widgets.dropdown("scale_factor", "10", ["10", "100", "1000", "5000", "10000", "20000"])
dbutils.widgets.text("tpcdi_directory", "/Volumes/tpcdi/tpcdi_raw_data/tpcdi_volume/")
dbutils.widgets.text("catalog", "tpcdi")
dbutils.widgets.text("batch_date", "")
dbutils.widgets.text("wh_db", "")

catalog         = dbutils.widgets.get("catalog")
scale_factor    = dbutils.widgets.get("scale_factor")
tpcdi_directory = dbutils.widgets.get("tpcdi_directory")
batch_date      = dbutils.widgets.get("batch_date")
wh_db           = dbutils.widgets.get("wh_db")
batches_dir     = f"{tpcdi_directory}augmented_incremental/_dailybatches/{wh_db}_{scale_factor}"
staging_dir     = f"{tpcdi_directory}augmented_incremental/_staging/sf={scale_factor}"
day_src_dir     = f"{staging_dir}/{batch_date}"

# COMMAND ----------

def copy_file(source_file, target_file):
  dbutils.fs.cp(source_file, target_file)
  return f"Successfully moved {source_file} to {target_file}"

# COMMAND ----------

# MAGIC %md
# MAGIC # Clear prior day's files before staging the new batch
# MAGIC Autoloader checkpoints already track what's been processed, so removing
# MAGIC the prior day's files is safe and keeps the volume size bounded over a
# MAGIC 730-day run.

# COMMAND ----------

if os.path.exists(batches_dir):
  dbutils.fs.rm(batches_dir, recurse=True)
dbutils.fs.mkdirs(batches_dir)

# COMMAND ----------

# List whatever files exist under the day's staging dir and copy them all. stage_files now produces single-part dates as `Customer.txt` and multi-part dates as `Customer_1.txt`, `Customer_2.txt`, ... — the bronze ingest's glob (`{Customer.txt,Customer_[0-9]*.txt}`) handles both. Sparse datasets (Customer/Account at low SF) may not have a file for every date; we just copy whatever's there.
try:
  src_files = [e for e in dbutils.fs.ls(day_src_dir) if not e.isDir()]
except Exception as e:
  print(f"No staging files for {batch_date}: {type(e).__name__}: {e}")
  src_files = []

print(f"Copying {len(src_files)} files for {batch_date}")
with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, len(src_files))) as executor:
  futures = [executor.submit(copy_file,
                             source_file=e.path,
                             target_file=f"{batches_dir}/{batch_date}/{e.name}")
             for e in src_files]
  for future in concurrent.futures.as_completed(futures):
    try: print(future.result())
    except requests.ConnectTimeout: print("ConnectTimeout.")