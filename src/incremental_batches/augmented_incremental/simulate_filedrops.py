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

catalog         = dbutils.widgets.get("catalog")
scale_factor    = dbutils.widgets.get("scale_factor")
tpcdi_directory = dbutils.widgets.get("tpcdi_directory")
batch_date      = dbutils.widgets.get("batch_date")
wh_db           = dbutils.widgets.get("wh_db")
batches_dir     = f"{tpcdi_directory}augmented_incremental/_dailybatches/{wh_db}_{scale_factor}"
staging_dir     = f"{tpcdi_directory}augmented_incremental/_staging/sf={scale_factor}"

# 7 stage_files notebooks each wrote {staging_dir}/{dataset}/_pdate={date}/part-*.csv.
# stage_to_files repartitions by date_col before partitionBy, so each date
# lands in a single shuffle bucket → single task → single part file per
# date. We move (not copy) that one part file into the auto-loader watch
# dir, renamed to {dataset}.txt. Sparse datasets (e.g. Customer) may have
# no _pdate= dir for a given date — that's expected, just skip.
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
    parts = [e for e in entries if e.name.startswith("part-")]
    if len(parts) > 1:
        raise RuntimeError(
            f"{dataset} {batch_date}: expected 1 part file after repartition(_pdate), "
            f"got {len(parts)}: {[e.name for e in parts]}")
    if not parts:
        return []
    return [(parts[0].path, f"{batches_dir}/{batch_date}/{dataset}.txt")]

move_pairs = []
for ds in DATASETS:
    move_pairs.extend(collect_one(ds))

print(f"Moving {len(move_pairs)} files for {batch_date}")

def do_mv(pair):
    src, target = pair
    dbutils.fs.mv(src, target)
    return f"{src} → {target}"

with concurrent.futures.ThreadPoolExecutor(
        max_workers=min(8, max(1, len(move_pairs)))) as executor:
    futures = [executor.submit(do_mv, p) for p in move_pairs]
    for future in concurrent.futures.as_completed(futures):
        try: print(future.result())
        except requests.ConnectTimeout: print("ConnectTimeout.")
