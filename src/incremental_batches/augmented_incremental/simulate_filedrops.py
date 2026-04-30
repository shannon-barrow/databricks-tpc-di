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

# 7 stage_files notebooks each wrote {staging_dir}/{Dataset}/_pdate={date}/part-*.csv.
# We move (not copy) the per-dataset part files for THIS day directly into
# the auto-loader watch dir, with renamed targets matching the bronze
# pathGlobfilter `{Dataset}_[0-9]*.txt`. Sparse datasets (e.g. Customer)
# may have no _pdate= dir for a given date — that's expected, just skip.
DATASETS = [
    ("Customer",        "Customer.txt"),
    ("Account",         "Account.txt"),
    ("Trade",           "Trade.txt"),
    ("CashTransaction", "CashTransaction.txt"),
    ("HoldingHistory",  "HoldingHistory.txt"),
    ("DailyMarket",     "DailyMarket.txt"),
    ("WatchHistory",    "WatchHistory.txt"),
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

def collect_one(dataset_name, target_filename):
    src_dir = f"{staging_dir}/{dataset_name}/_pdate={batch_date}"
    base, ext = os.path.splitext(target_filename)
    try:
        entries = dbutils.fs.ls(src_dir)
    except Exception:
        return []
    parts = sorted((e for e in entries if e.name.startswith("part-")),
                   key=lambda e: e.name)
    return [(p.path, f"{batches_dir}/{batch_date}/{base}_{i}{ext}")
            for i, p in enumerate(parts, start=1)]

move_pairs = []
for ds, fn in DATASETS:
    move_pairs.extend(collect_one(ds, fn))

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
