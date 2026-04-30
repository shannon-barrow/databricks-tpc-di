# Databricks notebook source
import os
import concurrent.futures
import requests

# COMMAND ----------

dbutils.widgets.dropdown("scale_factor", "10", ["10", "100", "1000", "5000", "10000", "20000"])
dbutils.widgets.text("tpcdi_directory", "/Volumes/tpcdi/tpcdi_raw_data/tpcdi_volume/")
dbutils.widgets.text("catalog", "tpcdi")
dbutils.widgets.text("batch_date", "")
dbutils.widgets.text("wh_db", "")
dbutils.widgets.text("file_ext", "txt")

catalog         = dbutils.widgets.get("catalog")
scale_factor    = dbutils.widgets.get("scale_factor")
tpcdi_directory = dbutils.widgets.get("tpcdi_directory")
batch_date      = dbutils.widgets.get("batch_date")
wh_db           = dbutils.widgets.get("wh_db")
file_ext        = dbutils.widgets.get("file_ext").strip()

# stage_to_files writes via Spark's CSV writer (default), which produces
# `*.csv` part files. The benchmark side wants `*.txt`. For any other
# file_ext (e.g. parquet) the source extension matches the target.
read_file_ext = "csv" if file_ext == "txt" else file_ext

batches_dir = f"{tpcdi_directory}augmented_incremental/_dailybatches/{wh_db}_{scale_factor}"
staging_dir = f"{tpcdi_directory}augmented_incremental/_staging/sf={scale_factor}"

# 7 stage_files notebooks each wrote {staging_dir}/{Dataset}/_pdate={date}/part-*.{read_file_ext}.
# stage_to_files repartitions by date_col before partitionBy, so each date
# lands in a single shuffle bucket → single task → single part file per
# date. We copy that one part file into the auto-loader watch dir,
# renamed to {Dataset}.{file_ext}. (Copy not move so the staging tree
# stays intact for re-runs of any individual batch.) Sparse datasets
# (e.g. Customer) may have no _pdate= dir for a given date — that's
# expected, just skip.
DATASETS = [
    "Customer", "Account", "Trade", "CashTransaction",
    "HoldingHistory", "DailyMarket", "WatchHistory",
]

# COMMAND ----------

# MAGIC %md
# MAGIC # Clear prior day's files before staging the new batch
# MAGIC Autoloader checkpoints already track what's been processed, so removing
# MAGIC the prior day's files is safe and keeps the volume size bounded over a
# MAGIC 730-day run.

# COMMAND ----------

if os.path.exists(batches_dir):
  dbutils.fs.rm(batches_dir, recurse=True)
dbutils.fs.mkdirs(f"{batches_dir}/{batch_date}")

# COMMAND ----------

def collect_one(dataset):
    src_dir = f"{staging_dir}/{dataset}/_pdate={batch_date}"
    try:
        entries = dbutils.fs.ls(src_dir)
    except Exception:
        return []
    parts = [e for e in entries if e.name.endswith(f".{read_file_ext}")]
    if not parts:
        return []
    if len(parts) > 1:
        raise RuntimeError(
            f"{dataset} {batch_date}: expected 1 .{read_file_ext} file after "
            f"repartition(_pdate), got {len(parts)}: {[e.name for e in parts]}")
    return [(parts[0].path, f"{batches_dir}/{batch_date}/{dataset}.{file_ext}")]

cp_pairs = []
for ds in DATASETS:
    cp_pairs.extend(collect_one(ds))

print(f"Copying {len(cp_pairs)} files for {batch_date}")

def do_cp(pair):
    src, target = pair
    dbutils.fs.cp(src, target)
    return f"{src} → {target}"

with concurrent.futures.ThreadPoolExecutor(
        max_workers=min(8, max(1, len(cp_pairs)))) as executor:
    futures = [executor.submit(do_cp, p) for p in cp_pairs]
    for future in concurrent.futures.as_completed(futures):
        try: print(future.result())
        except requests.ConnectTimeout: print("ConnectTimeout.")
