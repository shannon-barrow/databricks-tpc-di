# Databricks notebook source
# Per-batch task: copies the day's pre-staged .txt files from `_staging/sf=N/`
# to the per-(wh_db, sf, batch_date) directory under the UC external volume
# (which maps 1:1 to GCS). BigQuery reads the same bytes via external tables
# or a `bq load` step — that's done by a separate downstream task, NOT here.
#
# Adapter of the Snowflake-side `simulate_filedrops_sf.py`. Only difference:
# the volume backs GCS (not S3) and the per-batch dir is named
# `{wh_db}_sf{N}` instead of `{wh_db}_{N}` so dbt-bigquery's dataset naming
# (lowercase, `_sf{N}` suffix) lines up with the volume layout.

import os
import concurrent.futures
import requests

# COMMAND ----------

dbutils.widgets.dropdown("scale_factor","10", ["10","100","1000","5000","10000","20000"])
dbutils.widgets.text("tpcdi_directory", "/Volumes/main/tpcdi_raw_data/tpcdi_volume/")
dbutils.widgets.text("catalog",         "databricks-sandbox-perfeng",
                     "Treated as BQ project — same meaning as `catalog` everywhere else")
dbutils.widgets.text("batch_date",      "")
dbutils.widgets.text("wh_db",           "")
dbutils.widgets.text("file_ext",        "txt")

scale_factor    = dbutils.widgets.get("scale_factor")
tpcdi_directory = dbutils.widgets.get("tpcdi_directory")
batch_date      = dbutils.widgets.get("batch_date")
wh_db           = dbutils.widgets.get("wh_db")
file_ext        = dbutils.widgets.get("file_ext").strip()

read_file_ext = "csv" if file_ext == "txt" else file_ext

batches_dir = f"{tpcdi_directory}augmented_incremental/_dailybatches/{wh_db}_sf{scale_factor}"
staging_dir = f"{tpcdi_directory}augmented_incremental/_staging/sf={scale_factor}"

DATASETS = [
    "Customer", "Account", "Trade", "CashTransaction",
    "HoldingHistory", "DailyMarket", "WatchHistory",
]

# COMMAND ----------

# Clear prior day's files so the downstream BQ-side bronze step doesn't see stale data.
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
