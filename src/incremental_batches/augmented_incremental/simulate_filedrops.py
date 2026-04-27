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
filenames       = ['Account.txt', 'CashTransaction.txt', 'Customer.txt', 'DailyMarket.txt', 'HoldingHistory.txt', 'Trade.txt', 'WatchHistory.txt']
threads         = len(filenames)

# COMMAND ----------

def copy_file(source_file, target_file):
  # shutil.copyfile(source_file, target_file)
  dbutils.fs.cp(source_file, target_file)
  return f"Successfully moved {source_file} to {target_file}"  

# COMMAND ----------

with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
  futures = []
  for filename in filenames:
    futures.append(executor.submit(copy_file, source_file=f"{staging_dir}/{batch_date}/{filename}", target_file=f"{batches_dir}/{batch_date}/{filename}"))
  for future in concurrent.futures.as_completed(futures):
    try: print(future.result())
    except requests.ConnectTimeout: print("ConnectTimeout.")