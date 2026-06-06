# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# Per-batch task: copies the day's pre-staged .txt files from `_staging/sf=N/`
# to the per-(wh_db, sf, batch_date) directory under the UC external volume.
# Redshift reads the same bytes via the COPY statement issued by
# load_bronze_rs.py.
#
# Adapter of simulate_filedrops_bq.py. The Redshift workgroup is in us-west-2
# and the UC external volume's S3 backing bucket is also us-west-2 —
# zero cross-region transfer. Mirrors the BQ pattern except the per-batch
# dir naming follows lowercased Redshift identifier convention.

import os
import concurrent.futures
import requests

# COMMAND ----------

dbutils.widgets.dropdown("scale_factor","10", ["10","100","1000","5000","10000","20000"])
dbutils.widgets.text("tpcdi_directory", "/Volumes/main/tpcdi_raw_data/tpcdi_volume/")
dbutils.widgets.text("catalog",         "main", "Databricks UC catalog for the external volume")
dbutils.widgets.text("batch_date",      "")
dbutils.widgets.text("wh_db",           "")
dbutils.widgets.text("file_ext",        "txt")

scale_factor    = dbutils.widgets.get("scale_factor")
tpcdi_directory = dbutils.widgets.get("tpcdi_directory")
batch_date      = dbutils.widgets.get("batch_date")
wh_db           = dbutils.widgets.get("wh_db")
file_ext        = dbutils.widgets.get("file_ext").strip()

# Spark CSV files end in .csv on disk regardless of the bronze "file_ext"
# we want Redshift to see. Same trick BQ uses.
read_file_ext = "csv" if file_ext == "txt" else file_ext

# Redshift schemas are lowercase by convention; per-batch dir lowercased too
target_id = f"{wh_db}_{scale_factor}".lower()
batches_dir = f"{tpcdi_directory}augmented_incremental/_dailybatches/{target_id}"
staging_dir = f"{tpcdi_directory}augmented_incremental/_staging/sf={scale_factor}"

DATASETS = [
    "Customer", "Account", "Trade", "CashTransaction",
    "HoldingHistory", "DailyMarket", "WatchHistory",
]

# COMMAND ----------

# Clear prior day's files so load_bronze_rs's COPY doesn't accidentally pick
# up stale data.
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
