# Databricks notebook source
import shutil
import concurrent.futures
import requests
import os

# COMMAND ----------

dbutils.widgets.dropdown("scale_factor", "10", ["10", "100", "1000", "5000", "10000", "20000"])
dbutils.widgets.text("tpcdi_directory", "/Volumes/tpcdi/tpcdi_raw_data/tpcdi_volume/")
dbutils.widgets.text("catalog", "tpcdi")
dbutils.widgets.text("wh_db", "")
dbutils.widgets.text("incremental_batches_to_run", "730")

catalog         = dbutils.widgets.get("catalog")
scale_factor    = dbutils.widgets.get("scale_factor")
tpcdi_directory = dbutils.widgets.get("tpcdi_directory")
wh_db           = dbutils.widgets.get("wh_db")
n_batches       = max(1, min(730, int(dbutils.widgets.get("incremental_batches_to_run").strip())))
tgt_db          = f"{wh_db}_{scale_factor}"
staging_db      = f"tpcdi_incremental_staging_{scale_factor}"
batches_dir     = f"{tpcdi_directory}augmented_incremental/_dailybatches/{wh_db}_{scale_factor}"

# COMMAND ----------

# MAGIC %md
# MAGIC # Clone Staging Tables to New Benchmark Schema

# COMMAND ----------

display(spark.sql(f"DROP SCHEMA if exists {catalog}.{tgt_db} cascade"))
display(spark.sql(f"CREATE SCHEMA {catalog}.{tgt_db}"))
display(spark.sql(f"ALTER SCHEMA {catalog}.{tgt_db} ENABLE PREDICTIVE OPTIMIZATION"))

# COMMAND ----------

def clone_table(table_name, clone_type):
  spark.sql(f"CREATE OR REPLACE TABLE {catalog}.{tgt_db}.{table_name} {clone_type} CLONE {catalog}.{staging_db}.{table_name}")
  spark.sql(f"ANALYZE TABLE {catalog}.{tgt_db}.{table_name} COMPUTE STATISTICS FOR ALL COLUMNS")

# COMMAND ----------

shallow_tbls = [
  'taxrate',
  'dimdate',
  'industry',
  'tradetype',
  'dimbroker',
  'financial',
  'companyyeareps',
  'dimsecurity',
  'statustype',
  'dimcompany',
  'dimtime'
]
threads = len(shallow_tbls)
with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
  futures = []
  for tbl in shallow_tbls:
    futures.append(executor.submit(clone_table, table_name=tbl, clone_type="SHALLOW"))
  # for tbl in deep_tbls:
  #   futures.append(executor.submit(clone_table, table_name=tbl, clone_type="DEEP"))
  for future in concurrent.futures.as_completed(futures):
    try: print(future.result())
    except requests.ConnectTimeout: print("ConnectTimeout.")

# COMMAND ----------

# MAGIC %md
# MAGIC # Create/Reset Autoloader Batch Directories

# COMMAND ----------

if os.path.exists(batches_dir):
  print(f"Removing existing batches directory and recreating new one. ")
  dbutils.fs.rm(batches_dir, recurse=True)
dbutils.fs.mkdirs(batches_dir)

# COMMAND ----------

# MAGIC %md
# MAGIC # Create list of dates that will loop in subsequent step of the pipeline 
# MAGIC - Set the list of dates as a jobs task value and use "for each" task loop to execute each batch date in order

# COMMAND ----------

from datetime import date, timedelta, datetime

batch_date_ls = []
start_date    = datetime(2015, 7, 6)
for dt_interval in range(0, n_batches):
  batch_date_ls.append((start_date + timedelta(days=dt_interval)).strftime("%Y-%m-%d"))
print(f"Emitting {len(batch_date_ls)} batch dates ({batch_date_ls[0]} → {batch_date_ls[-1]})")
dbutils.jobs.taskValues.set(key = "batch_date_ls", value = batch_date_ls)