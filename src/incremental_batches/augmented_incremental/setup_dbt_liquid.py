# Databricks notebook source
# MAGIC %md
# MAGIC # Augmented Incremental — dbt + Liquid clustering Setup
# MAGIC
# MAGIC Mirrors `setup_dbt.py` (the dbt path) but Liquid-clusters the SCD2 /
# MAGIC fact tables via CTAS — same column choices as `setup_liquid.py`
# MAGIC (the Cluster-job liquid path). dbt MERGEs into the already-Liquid
# MAGIC tables; the per-model `liquid_clustered_by` config gated by
# MAGIC `use_liquid_clustering=true` keeps the table layout consistent if
# MAGIC dbt ever recreates the table.
# MAGIC
# MAGIC | Table | Layout | Cluster column |
# MAGIC |---|---|---|
# MAGIC | dimcustomer / dimaccount | CTAS + CLUSTER BY | `enddate` (SCD2 — current rows = 9999-12-31 cluster together) |
# MAGIC | dimtrade | CTAS + CLUSTER BY | `sk_closedateid` |
# MAGIC | factwatches | CTAS + CLUSTER BY | `sk_dateid_dateremoved` |
# MAGIC | factholdings | CTAS + CLUSTER BY | `sk_dateid` |
# MAGIC | factmarkethistory | CTAS + CLUSTER BY | `sk_dateid` (dbt switches strategy `insert_overwrite` → `merge` when `use_liquid_clustering=true`) |
# MAGIC | bronzedailymarket | CTAS + CLUSTER BY | `dm_date` (FMH rolling-year lookback filter column) |
# MAGIC | factcashbalances | DEEP CLONE | small running-aggregate state — kept on source layout (matches `setup_liquid.py`) |
# MAGIC | reference tables (12) | SHALLOW CLONE | unchanged |
# MAGIC
# MAGIC ## Why CTAS instead of DEEP CLONE for liquid tables
# MAGIC DEEP CLONE preserves the source's partitioned file layout. Just
# MAGIC running `ALTER TABLE ... CLUSTER BY` afterward would only mark the
# MAGIC table for liquid going forward — existing files would still be
# MAGIC partitioned until an `OPTIMIZE FULL` rewrites them. CTAS forces
# MAGIC the rewrite up front so the starting state is fully liquid.

# COMMAND ----------

import concurrent.futures
import os
import requests

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
    spark.sql(f"CREATE OR REPLACE TABLE {catalog}.{tgt_db}.{table_name} SHALLOW CLONE {catalog}.{staging_db}.{table_name}")
    spark.sql(f"ANALYZE TABLE {catalog}.{tgt_db}.{table_name} COMPUTE STATISTICS FOR ALL COLUMNS")

def deep_clone(table_name):
    spark.sql(f"CREATE OR REPLACE TABLE {catalog}.{tgt_db}.{table_name} DEEP CLONE {catalog}.{staging_db}.{table_name}")
    spark.sql(f"ANALYZE TABLE {catalog}.{tgt_db}.{table_name} COMPUTE STATISTICS FOR ALL COLUMNS")

def liquid_ctas(table_name, cluster_cols):
    """CTAS with liquid clustering, then OPTIMIZE + ANALYZE.
    Forces a rewrite (vs deep-clone-then-alter) so the starting layout is
    actually liquid."""
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

# Static reference tables — SHALLOW CLONE keeps them cheap.
# Note: currentaccountbalances is NOT pre-cloned (vs setup_dbt.py): the staging
# version is PARTITIONED BY latest_batch (boolean), and the dbt model with
# `use_liquid_clustering=true` declares liquid_clustered_by='accountid'.
# If we pre-clone the partitioned table, dbt's first incremental run tries
# `ALTER TABLE ... CLUSTER BY accountid` against the partitioned target and
# Delta rejects it (DELTA_ALTER_TABLE_CLUSTER_BY_ON_PARTITIONED_TABLE_NOT_ALLOWED).
# Skipping the clone lets dbt CREATE the table fresh with Liquid layout.
shallow_tbls = [
    'taxrate', 'dimdate', 'industry', 'tradetype', 'dimbroker',
    'financial', 'companyyeareps', 'dimsecurity', 'statustype',
    'dimcompany', 'dimtime',
]

# Liquid-clustered tables. Cluster columns mirror setup_liquid.py.
liquid_tbls = {
    'dimcustomer':       ['enddate'],
    'dimaccount':        ['enddate'],
    'dimtrade':          ['sk_closedateid'],
    'factwatches':       ['sk_dateid_dateremoved'],
    'factholdings':      ['sk_dateid'],
    'factmarkethistory': ['sk_dateid'],
    # bronzedailymarket is staged with the prior year of DM rows; FMH
    # incremental does a 365-day rolling lookback into it. dm_date is
    # the dominant filter column.
    'bronzedailymarket': ['dm_date'],
}

# No DEEP CLONEs in the Liquid path. factcashbalances would be a candidate
# (same pattern as setup_liquid.py for the cluster job), but the dbt model
# declares liquid_clustered_by='sk_dateid' in the Liquid path. A partitioned
# DEEP CLONE would trip DELTA_ALTER_TABLE_CLUSTER_BY_ON_PARTITIONED_TABLE_NOT_ALLOWED
# when dbt's first run tried ALTER TABLE ... CLUSTER BY on the existing
# partitioned target. Let dbt CREATE it fresh on first run instead.
deep_tbls = []

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
# MAGIC # Pre-create Bronze + Gold tables (Liquid clustered)
# MAGIC
# MAGIC Pre-create the 6 bronze tables and `factcashbalances` here so dbt's
# MAGIC per-batch run finds them already Liquid-clustered. The dbt models
# MAGIC under `use_liquid_clustering=true` deliberately omit any
# MAGIC `liquid_clustered_by` / `tblproperties` config — otherwise
# MAGIC dbt-databricks issues `ALTER TABLE CLUSTER BY` and
# MAGIC `ALTER TABLE SET TBLPROPERTIES` against the target on every batch
# MAGIC (synchronizing model config to table state, even when nothing has
# MAGIC drifted). That's both noisy in query history and adds per-batch
# MAGIC overhead. By owning the layout here and leaving dbt unopinionated,
# MAGIC every batch becomes a clean MERGE/APPEND with no DDL.
# MAGIC
# MAGIC `currentaccountbalances` is intentionally NOT pre-created: its
# MAGIC dbt model uses `insert_overwrite` (no `partition_by` in the Liquid
# MAGIC path), which degrades to `CREATE OR REPLACE TABLE AS SELECT` on
# MAGIC every batch — any cluster_by we set here would be wiped. The
# MAGIC table is small (one row per touched account) so unclustered is fine.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.bronzecustomer') (
# MAGIC   cdc_flag STRING, cdc_dsn BIGINT, customerid BIGINT, taxid STRING, status STRING,
# MAGIC   lastname STRING, firstname STRING, middleinitial STRING, gender STRING, tier TINYINT,
# MAGIC   dob DATE, addressline1 STRING, addressline2 STRING, postalcode STRING, city STRING,
# MAGIC   stateprov STRING, country STRING,
# MAGIC   c_ctry_1 STRING, c_area_1 STRING, c_local_1 STRING, c_ext_1 STRING,
# MAGIC   c_ctry_2 STRING, c_area_2 STRING, c_local_2 STRING, c_ext_2 STRING,
# MAGIC   c_ctry_3 STRING, c_area_3 STRING, c_local_3 STRING, c_ext_3 STRING,
# MAGIC   email1 STRING, email2 STRING, lcl_tx_id STRING, nat_tx_id STRING, update_dt DATE
# MAGIC )
# MAGIC CLUSTER BY (update_dt)
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.autoOptimize.autoCompact' = 'false',
# MAGIC   'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC   'delta.dataSkippingNumIndexedCols' = '34'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.bronzeaccount') (
# MAGIC   cdc_flag STRING, cdc_dsn BIGINT, accountid BIGINT, brokerid BIGINT, customerid BIGINT,
# MAGIC   accountdesc STRING, taxstatus TINYINT, status STRING, update_dt DATE
# MAGIC )
# MAGIC CLUSTER BY (update_dt)
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.autoOptimize.autoCompact' = 'false',
# MAGIC   'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC   'delta.dataSkippingNumIndexedCols' = '34'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.bronzecashtransaction') (
# MAGIC   cdc_flag STRING, cdc_dsn BIGINT, accountid BIGINT, ct_dts TIMESTAMP,
# MAGIC   ct_amt DOUBLE, ct_name STRING, event_dt DATE
# MAGIC )
# MAGIC CLUSTER BY (event_dt)
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.autoOptimize.autoCompact' = 'false',
# MAGIC   'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC   'delta.dataSkippingNumIndexedCols' = '34'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.bronzeholdings') (
# MAGIC   cdc_flag STRING, cdc_dsn BIGINT, hh_h_t_id BIGINT, hh_t_id BIGINT,
# MAGIC   hh_before_qty INT, hh_after_qty INT, event_dt DATE
# MAGIC )
# MAGIC CLUSTER BY (event_dt)
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.autoOptimize.autoCompact' = 'false',
# MAGIC   'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC   'delta.dataSkippingNumIndexedCols' = '34'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.bronzetrade') (
# MAGIC   cdc_flag STRING, cdc_dsn BIGINT, tradeid BIGINT, t_dts TIMESTAMP, status STRING,
# MAGIC   t_tt_id STRING, cashflag TINYINT, t_s_symb STRING, quantity INT, bidprice DOUBLE,
# MAGIC   t_ca_id BIGINT, executedby STRING, tradeprice DOUBLE, fee DOUBLE, commission DOUBLE,
# MAGIC   tax DOUBLE, event_dt DATE
# MAGIC )
# MAGIC CLUSTER BY (event_dt)
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.autoOptimize.autoCompact' = 'false',
# MAGIC   'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC   'delta.dataSkippingNumIndexedCols' = '34'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.bronzewatches') (
# MAGIC   cdc_flag STRING, cdc_dsn BIGINT, w_c_id BIGINT, w_s_symb STRING, w_dts TIMESTAMP,
# MAGIC   w_action STRING, event_dt DATE
# MAGIC )
# MAGIC CLUSTER BY (event_dt)
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.autoOptimize.autoCompact' = 'false',
# MAGIC   'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC   'delta.dataSkippingNumIndexedCols' = '34'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC -- factcashbalances: pre-create empty so dbt's first MERGE finds an already-Liquid target.
# MAGIC -- Schema mirrors models/gold/factcashbalances.sql output columns.
# MAGIC CREATE OR REPLACE TABLE IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.factcashbalances') (
# MAGIC   sk_customerid BIGINT,
# MAGIC   sk_accountid BIGINT,
# MAGIC   sk_dateid BIGINT,
# MAGIC   cash DECIMAL(15,2)
# MAGIC )
# MAGIC CLUSTER BY (sk_dateid)
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.autoOptimize.autoCompact' = 'false',
# MAGIC   'delta.autoOptimize.optimizeWrite' = 'true'
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC # Create/Reset Daily Batches Watch Directory

# COMMAND ----------

if os.path.exists(batches_dir):
    print(f"Removing existing batches directory and recreating new one.")
    dbutils.fs.rm(batches_dir, recurse=True)
dbutils.fs.mkdirs(batches_dir)

# COMMAND ----------

# MAGIC %md
# MAGIC # Emit batch_date list for the parent for_each loop

# COMMAND ----------

from datetime import date, timedelta, datetime

batch_date_ls = []
start_date = datetime(2016, 7, 6)
for dt_interval in range(0, n_batches):
    batch_date_ls.append((start_date + timedelta(days=dt_interval)).strftime("%Y-%m-%d"))
print(f"Emitting {len(batch_date_ls)} batch dates ({batch_date_ls[0]} → {batch_date_ls[-1]})")
dbutils.jobs.taskValues.set(key="batch_date_ls", value=batch_date_ls)
