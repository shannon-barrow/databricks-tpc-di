# Databricks notebook source
# MAGIC %md
# MAGIC # Augmented Incremental — Liquid-clustered setup (perf-opt jobs variant)
# MAGIC
# MAGIC Mirrors `setup.py` but every per-run table is **liquid clustered** instead
# MAGIC of partitioned. Mirrors the SDP-liquid pipeline's clustering choices:
# MAGIC
# MAGIC | Table | Cluster column | Notes |
# MAGIC |---|---|---|
# MAGIC | bronzecustomer / bronzeaccount | `update_dt` | per-batch ingest filter (was PARTITIONED BY) |
# MAGIC | bronzecashtransaction / bronzeholdings / bronzetrade / bronzewatches | `event_dt` | per-batch filter |
# MAGIC | bronzedailymarket | `dm_date` | FMH 365-day rolling window scan |
# MAGIC | dimcustomer / dimaccount | `enddate` | SCD2 — equivalent of SDP's `__END_AT` (current rows = 9999-12-31 cluster together) |
# MAGIC | dimtrade | `sk_closedateid` | matches SDP |
# MAGIC | factmarkethistory | `sk_dateid` | matches SDP |
# MAGIC | factwatches | `sk_dateid_dateremoved` | matches SDP |
# MAGIC | factholdings | `sk_dateid` | matches SDP |
# MAGIC | factcashbalances | (kept as DEEP CLONE) | SDP doesn't override; small running-aggregate state |
# MAGIC
# MAGIC ## Why CTAS instead of DEEP CLONE for liquid tables
# MAGIC DEEP CLONE preserves the source's file layout and clustering. Since the
# MAGIC staging schema's tables are partitioned, deep-cloning and just running
# MAGIC `ALTER TABLE ... CLUSTER BY` would only mark the table for liquid going
# MAGIC forward — the existing files would still be partitioned until an
# MAGIC `OPTIMIZE FULL` rewrites them. CTAS forces the rewrite up front so the
# MAGIC starting state is fully liquid.
# MAGIC
# MAGIC The CTAS step is wrapped in a parallel future alongside ANALYZE +
# MAGIC OPTIMIZE so the per-table prep stays concurrent.

import shutil
import concurrent.futures
import requests
import os

# COMMAND ----------

dbutils.widgets.dropdown("scale_factor", "10", ["10", "100", "1000", "5000", "10000", "20000"])
dbutils.widgets.text("tpcdi_directory", "/Volumes/tpcdi/tpcdi_raw_data/tpcdi_volume/")
dbutils.widgets.text("catalog", "tpcdi")
dbutils.widgets.text("wh_db", "")
dbutils.widgets.text("incremental_batches_to_run", "365")

catalog         = dbutils.widgets.get("catalog")
scale_factor    = dbutils.widgets.get("scale_factor")
tpcdi_directory = dbutils.widgets.get("tpcdi_directory")
wh_db           = dbutils.widgets.get("wh_db")
n_batches       = max(1, min(365, int(dbutils.widgets.get("incremental_batches_to_run").strip())))
tgt_db          = f"{wh_db}_{scale_factor}"
staging_db      = f"tpcdi_incremental_staging_{scale_factor}"
batches_dir     = f"{tpcdi_directory}augmented_incremental/_dailybatches/{tgt_db}"
checkpoint_dir  = f"{tpcdi_directory}augmented_incremental/_checkpoints/{tgt_db}"

# COMMAND ----------

# MAGIC %md
# MAGIC # Build target schema

# COMMAND ----------

display(spark.sql(f"DROP SCHEMA if exists {catalog}.{tgt_db} cascade"))
display(spark.sql(f"CREATE SCHEMA {catalog}.{tgt_db}"))
display(spark.sql(f"ALTER SCHEMA {catalog}.{tgt_db} ENABLE PREDICTIVE OPTIMIZATION"))

# COMMAND ----------

# MAGIC %md
# MAGIC # Per-table prep helpers

# COMMAND ----------

def shallow_clone(table_name):
    """SHALLOW clone — small static reference tables. Inherits source layout
    (no rewrite). ANALYZE for stats."""
    spark.sql(f"CREATE OR REPLACE TABLE {catalog}.{tgt_db}.{table_name} SHALLOW CLONE {catalog}.{staging_db}.{table_name}")
    spark.sql(f"ANALYZE TABLE {catalog}.{tgt_db}.{table_name} COMPUTE STATISTICS FOR ALL COLUMNS")

def deep_clone(table_name):
    """DEEP clone — used for tables we don't want to liquid-cluster (only
    factcashbalances in this notebook). Inherits source partitioning."""
    spark.sql(f"CREATE OR REPLACE TABLE {catalog}.{tgt_db}.{table_name} DEEP CLONE {catalog}.{staging_db}.{table_name}")
    spark.sql(f"ANALYZE TABLE {catalog}.{tgt_db}.{table_name} COMPUTE STATISTICS FOR ALL COLUMNS")

def liquid_ctas(table_name, cluster_cols):
    """CTAS with liquid clustering, then OPTIMIZE + ANALYZE.
    Forces a rewrite (vs deep-clone-then-alter) so the starting layout is
    actually liquid. OPTIMIZE inside the same future keeps each table's
    prep self-contained and concurrent across the pool."""
    cluster_clause = f"CLUSTER BY ({', '.join(cluster_cols)})"
    spark.sql(
        f"CREATE OR REPLACE TABLE {catalog}.{tgt_db}.{table_name} "
        f"{cluster_clause} AS SELECT * FROM {catalog}.{staging_db}.{table_name}")
    spark.sql(f"OPTIMIZE {catalog}.{tgt_db}.{table_name}")
    spark.sql(f"ANALYZE TABLE {catalog}.{tgt_db}.{table_name} COMPUTE STATISTICS FOR ALL COLUMNS")

# COMMAND ----------

# MAGIC %md
# MAGIC # Submit clones / CTAS in parallel

# COMMAND ----------

# Small reference tables — SHALLOW CLONE keeps them cheap.
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
    'dimtime',
    'currentaccountbalances',
]

# Liquid-clustered SCD2 / fact tables. Cluster columns mirror SDP-liquid
# (dlt_historical_liquid.sql) where applicable. dimcustomer/dimaccount
# use `enddate` (Cluster path's equivalent of SDP's `__END_AT`).
liquid_tbls = {
    'dimcustomer':       ['enddate'],
    'dimaccount':        ['enddate'],
    'dimtrade':          ['sk_closedateid'],
    'factwatches':       ['sk_dateid_dateremoved'],
    'factholdings':      ['sk_dateid'],
    'factmarkethistory': ['sk_dateid'],
    # bronzedailymarket is staged with the prior year of DM rows; the
    # FMH incremental does a 365-day rolling lookback into it. dm_date
    # is the dominant filter column.
    'bronzedailymarket': ['dm_date'],
}

# DEEP CLONE for tables we keep on partitioned/source layout.
deep_tbls = ['factcashbalances']

threads = len(shallow_tbls) + len(liquid_tbls) + len(deep_tbls)
with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
    futures = []
    for tbl in shallow_tbls:
        futures.append(executor.submit(shallow_clone, table_name=tbl))
    for tbl, cols in liquid_tbls.items():
        futures.append(executor.submit(liquid_ctas, table_name=tbl, cluster_cols=cols))
    for tbl in deep_tbls:
        futures.append(executor.submit(deep_clone, table_name=tbl))
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
# MAGIC # Create/Reset Checkpoint Directories

# COMMAND ----------

incr_tbls = [
    "bronzeaccount",
    "bronzecashtransaction",
    "bronzecustomer",
    "bronzedailymarket",
    "bronzeholdings",
    "bronzetrade",
    "bronzewatches",
    'factholdings',
    'dimcustomer',
    'dimaccount',
    'factcashbalances',
    'dimtrade',
    'factwatches',
    'factmarkethistory',
    'bronzeaccountcustomer'
]
if os.path.exists(checkpoint_dir):
    print(f"Removing existing checkpoints directory and recreating new one. ")
    dbutils.fs.rm(checkpoint_dir, recurse=True)
for tbl in incr_tbls:
    dbutils.fs.mkdirs(f"{checkpoint_dir}/{tbl}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Create/Reset Bronze Ingestion Tables (liquid-clustered)
# MAGIC
# MAGIC Same column on the cluster key as the original `PARTITIONED BY`.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.bronzecustomer') (
# MAGIC   cdc_flag STRING,
# MAGIC   cdc_dsn INT,
# MAGIC   customerid STRING,
# MAGIC   taxid STRING,
# MAGIC   status STRING,
# MAGIC   lastname STRING,
# MAGIC   firstname STRING,
# MAGIC   middleinitial STRING,
# MAGIC   gender STRING,
# MAGIC   tier STRING,
# MAGIC   dob STRING,
# MAGIC   addressline1 STRING,
# MAGIC   addressline2 STRING,
# MAGIC   postalcode STRING,
# MAGIC   city STRING,
# MAGIC   stateprov STRING,
# MAGIC   country STRING,
# MAGIC   c_ctry_1 STRING,
# MAGIC   c_area_1 STRING,
# MAGIC   c_local_1 STRING,
# MAGIC   c_ext_1 STRING,
# MAGIC   c_ctry_2 STRING,
# MAGIC   c_area_2 STRING,
# MAGIC   c_local_2 STRING,
# MAGIC   c_ext_2 STRING,
# MAGIC   c_ctry_3 STRING,
# MAGIC   c_area_3 STRING,
# MAGIC   c_local_3 STRING,
# MAGIC   c_ext_3 STRING,
# MAGIC   email1 STRING,
# MAGIC   email2 STRING,
# MAGIC   lcl_tx_id STRING,
# MAGIC   nat_tx_id STRING,
# MAGIC   update_dt DATE
# MAGIC )
# MAGIC CLUSTER BY (update_dt)
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.autoOptimize.autoCompact' = 'false',
# MAGIC   'delta.autoOptimize.optimizeWrite' = 'true'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.bronzeaccount') (
# MAGIC   cdc_flag STRING,
# MAGIC   cdc_dsn INT,
# MAGIC   accountid STRING,
# MAGIC   brokerid STRING,
# MAGIC   customerid STRING,
# MAGIC   accountdesc STRING,
# MAGIC   taxstatus STRING,
# MAGIC   status STRING,
# MAGIC   update_dt DATE
# MAGIC )
# MAGIC CLUSTER BY (update_dt)
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.autoOptimize.autoCompact' = 'false',
# MAGIC   'delta.autoOptimize.optimizeWrite' = 'true'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.bronzecashtransaction') (
# MAGIC   cdc_flag STRING,
# MAGIC   cdc_dsn BIGINT,
# MAGIC   accountid BIGINT,
# MAGIC   ct_dts TIMESTAMP,
# MAGIC   ct_amt DOUBLE,
# MAGIC   ct_name STRING,
# MAGIC   event_dt DATE
# MAGIC )
# MAGIC CLUSTER BY (event_dt)
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.autoOptimize.autoCompact' = 'false',
# MAGIC   'delta.autoOptimize.optimizeWrite' = 'true'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.bronzeholdings') (
# MAGIC   cdc_flag STRING,
# MAGIC   cdc_dsn BIGINT,
# MAGIC   hh_h_t_id BIGINT,
# MAGIC   hh_t_id BIGINT,
# MAGIC   hh_before_qty INT,
# MAGIC   hh_after_qty INT,
# MAGIC   event_dt DATE
# MAGIC )
# MAGIC CLUSTER BY (event_dt)
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.autoOptimize.autoCompact' = 'false',
# MAGIC   'delta.autoOptimize.optimizeWrite' = 'true'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.bronzetrade') (
# MAGIC   cdc_flag STRING,
# MAGIC   cdc_dsn BIGINT,
# MAGIC   tradeid BIGINT,
# MAGIC   t_dts TIMESTAMP,
# MAGIC   status STRING,
# MAGIC   t_tt_id STRING,
# MAGIC   cashflag TINYINT,
# MAGIC   t_s_symb STRING,
# MAGIC   quantity INT,
# MAGIC   bidprice DOUBLE,
# MAGIC   t_ca_id BIGINT,
# MAGIC   executedby STRING,
# MAGIC   tradeprice DOUBLE,
# MAGIC   fee DOUBLE,
# MAGIC   commission DOUBLE,
# MAGIC   tax DOUBLE,
# MAGIC   event_dt DATE
# MAGIC )
# MAGIC CLUSTER BY (event_dt)
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.autoOptimize.autoCompact' = 'false',
# MAGIC   'delta.autoOptimize.optimizeWrite' = 'true'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.bronzewatches') (
# MAGIC   cdc_flag STRING,
# MAGIC   cdc_dsn BIGINT,
# MAGIC   w_c_id BIGINT,
# MAGIC   w_s_symb STRING,
# MAGIC   w_dts TIMESTAMP,
# MAGIC   w_action STRING,
# MAGIC   event_dt DATE
# MAGIC )
# MAGIC CLUSTER BY (event_dt)
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.autoOptimize.autoCompact' = 'false',
# MAGIC   'delta.autoOptimize.optimizeWrite' = 'true'
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC # Emit batch_date list for the parent for_each loop

# COMMAND ----------

from datetime import date, timedelta, datetime

batch_date_ls = []
start_date    = datetime(2016, 7, 6)
for dt_interval in range(0, n_batches):
    batch_date_ls.append((start_date + timedelta(days=dt_interval)).strftime("%Y-%m-%d"))
print(f"Emitting {len(batch_date_ls)} batch dates ({batch_date_ls[0]} → {batch_date_ls[-1]})")
dbutils.jobs.taskValues.set(key = "batch_date_ls", value = batch_date_ls)
