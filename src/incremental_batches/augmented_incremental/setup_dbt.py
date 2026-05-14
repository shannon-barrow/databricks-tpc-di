# Databricks notebook source
# MAGIC %md
# MAGIC # Augmented Incremental — dbt setup
# MAGIC
# MAGIC Builds the per-run schema for the dbt benchmark variant by cloning the
# MAGIC shared `tpcdi_incremental_staging_{sf}` schema. Staging is now
# MAGIC Liquid-clustered (each `historical/*.sql` builds its dim/fact table
# MAGIC with `CLUSTER BY (...)` already), so DEEP CLONE inherits the layout
# MAGIC directly — no per-table CTAS needed.
# MAGIC
# MAGIC dbt MERGEs / APPENDs into already-Liquid tables; the dbt model configs
# MAGIC deliberately omit any `liquid_clustered_by` / `tblproperties` so
# MAGIC dbt-databricks doesn't issue per-batch `ALTER TABLE CLUSTER BY` /
# MAGIC `ALTER TABLE SET TBLPROPERTIES` (the "setup-owns-layout" pattern).
# MAGIC
# MAGIC | Group | Tables | Operation |
# MAGIC |---|---|---|
# MAGIC | Reference (small, static) + currentaccountbalances | taxrate, dimdate, industry, tradetype, dimbroker, financial, companyyeareps, dimsecurity, statustype, dimcompany, dimtime, currentaccountbalances | SHALLOW CLONE (currentaccountbalances dbt model reads `{{ this }}` once for historical baseline, then rewrites via CREATE OR REPLACE TABLE AS SELECT — a pointer is enough) |
# MAGIC | Dim/Fact + bronzedailymarket | dimcustomer, dimaccount, dimtrade, factwatches, factholdings, factmarkethistory, bronzedailymarket, factcashbalances | DEEP CLONE (Liquid layout inherited from staging) |
# MAGIC | 6 streaming bronze + account_updates_from_customer | bronzeaccount, bronzecashtransaction, bronzecustomer, bronzeholdings, bronzetrade, bronzewatches, account_updates_from_customer | Empty `CREATE TABLE … CLUSTER BY` (no staging source — 6 bronze are populated by Auto Loader during the daily loop; account_updates_from_customer is a dbt-managed staging table derived from bronzecustomer 'U' events for dimaccount to UNION) |

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

def clone_table(table_name, clone_type):
    """SHALLOW or DEEP clone from staging, then ANALYZE for stats."""
    spark.sql(f"CREATE OR REPLACE TABLE {catalog}.{tgt_db}.{table_name} {clone_type} CLONE {catalog}.{staging_db}.{table_name}")
    spark.sql(f"ANALYZE TABLE {catalog}.{tgt_db}.{table_name} COMPUTE STATISTICS FOR ALL COLUMNS")

# COMMAND ----------

# MAGIC %md
# MAGIC # Submit clones in parallel

# COMMAND ----------

# Small reference tables — SHALLOW CLONE keeps them cheap.
# currentaccountbalances is shallow-cloned so the dbt model can read
# `{{ this }}` for the historical baseline on its first batch; dbt's
# CREATE OR REPLACE TABLE AS SELECT then rewrites the table in-place
# starting from the next batch, so a physical copy here would be wasted.
shallow_tbls = [
    'taxrate', 'dimdate', 'industry', 'tradetype', 'dimbroker',
    'financial', 'companyyeareps', 'dimsecurity', 'statustype',
    'dimcompany', 'dimtime',
    'currentaccountbalances',
]

# DEEP CLONE for dim/fact + bronzedailymarket. Layout (Liquid CLUSTER BY)
# inherited from staging — see historical/*.sql for cluster columns.
# factcashbalances starts with all historical balances; dbt's merge keyed
# on (sk_accountid, sk_dateid) preserves them and inserts new daily rows.
deep_tbls = [
    'dimcustomer',
    'dimaccount',
    'dimtrade',
    'factwatches',
    'factholdings',
    'factmarkethistory',
    'bronzedailymarket',
    'factcashbalances',
]

threads = len(shallow_tbls) + len(deep_tbls)
with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
    futures = []
    for tbl in shallow_tbls:
        futures.append(executor.submit(clone_table, table_name=tbl, clone_type="SHALLOW"))
    for tbl in deep_tbls:
        futures.append(executor.submit(clone_table, table_name=tbl, clone_type="DEEP"))
    for future in concurrent.futures.as_completed(futures):
        try: print(future.result())
        except requests.ConnectTimeout: print("ConnectTimeout.")

# COMMAND ----------

# MAGIC %md
# MAGIC # Pre-create 6 streaming bronze tables (Liquid clustered)
# MAGIC
# MAGIC The 6 bronze tables have no staging source — they're populated fresh
# MAGIC by Auto Loader during the daily loop. Pre-create them here so dbt's
# MAGIC first batch finds them already Liquid-clustered (setup-owns-layout
# MAGIC pattern). The dbt models deliberately omit any `liquid_clustered_by`
# MAGIC / `tblproperties` config — otherwise dbt-databricks would issue
# MAGIC per-batch `ALTER TABLE CLUSTER BY` / `ALTER TABLE SET TBLPROPERTIES`
# MAGIC against the target (synchronizing model config to table state, even
# MAGIC when nothing has drifted).

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
# MAGIC -- account_updates_from_customer: staged rows derived from bronzecustomer
# MAGIC -- SCD2 'U' events joined to dimaccount AS-OF batch start. dimaccount
# MAGIC -- UNIONs this with the day's bronzeaccount file drops. Schema mirrors
# MAGIC -- bronzeaccount so the UNION ALL doesn't need column shimming.
# MAGIC CREATE OR REPLACE TABLE IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.account_updates_from_customer') (
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
