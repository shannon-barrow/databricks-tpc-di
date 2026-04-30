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

catalog         = dbutils.widgets.get("catalog")
scale_factor    = dbutils.widgets.get("scale_factor")
tpcdi_directory = dbutils.widgets.get("tpcdi_directory")
wh_db           = dbutils.widgets.get("wh_db")
tgt_db          = f"{wh_db}_{scale_factor}"
staging_db      = f"tpcdi_incremental_staging_{scale_factor}"
batches_dir     = f"{tpcdi_directory}augmented_incremental/_dailybatches/{tgt_db}"
checkpoint_dir  = f"{tpcdi_directory}augmented_incremental/_checkpoints/{tgt_db}"

# COMMAND ----------

# MAGIC %md
# MAGIC # Deep Clone Staging Tables to New Benchmark Schema

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
  'dimtime',
  'currentaccountbalances',
  # Historical cash transaction events (pre-2015-07-06). SDP's bronze
  # backfill reads this to seed bronzecashtransaction so the running-sum
  # daily balance starts from the correct baseline. Cluster path doesn't
  # use it (it reads currentaccountbalances directly) — extra clone is
  # harmless.
  'cashtransactionhistorical',
]
deep_tbls = [
  'dimtrade',
  'factwatches',
  'factholdings',
  'dimcustomer',
  'dimaccount',
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

# MAGIC %sql
# MAGIC USE IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor);
# MAGIC CREATE OR REPLACE TABLE factmarkethistory (
# MAGIC   sk_securityid BIGINT NOT NULL COMMENT 'Surrogate key for SecurityID',
# MAGIC   sk_companyid BIGINT COMMENT 'Surrogate key for CompanyID',
# MAGIC   sk_dateid BIGINT NOT NULL COMMENT 'Surrogate key for the date',
# MAGIC   peratio DOUBLE COMMENT 'Price to earnings per share ratio',
# MAGIC   yield DOUBLE COMMENT 'Dividend to price ratio, as a percentage',
# MAGIC   fiftytwoweekhigh DOUBLE COMMENT 'Security highest price in last 52 weeks from this day',
# MAGIC   sk_fiftytwoweekhighdate BIGINT COMMENT 'Earliest date on which the 52 week high price was set',
# MAGIC   fiftytwoweeklow DOUBLE COMMENT 'Security lowest price in last 52 weeks from this day',
# MAGIC   sk_fiftytwoweeklowdate BIGINT COMMENT 'Earliest date on which the 52 week low price was set',
# MAGIC   closeprice DOUBLE COMMENT 'Security closing price on this day',
# MAGIC   dayhigh DOUBLE COMMENT 'Highest price for the security on this day',
# MAGIC   daylow DOUBLE COMMENT 'Lowest price for the security on this day',
# MAGIC   volume INT COMMENT 'Trading volume of the security on this day',
# MAGIC   CONSTRAINT fmh_pk PRIMARY KEY(sk_securityid, sk_dateid),
# MAGIC   CONSTRAINT fmh_security_fk FOREIGN KEY (sk_securityid) REFERENCES DimSecurity(sk_securityid),
# MAGIC   CONSTRAINT fmh_company_fk FOREIGN KEY (sk_companyid) REFERENCES DimCompany(sk_companyid),
# MAGIC   CONSTRAINT fmh_date_fk FOREIGN KEY (sk_dateid) REFERENCES DimDate(sk_dateid)
# MAGIC )
# MAGIC PARTITIONED BY (sk_dateid)
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.autoOptimize.autoCompact' = 'true',
# MAGIC   'delta.autoOptimize.optimizeWrite' = 'true'
# MAGIC );

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
# MAGIC # Create/Reset Bronze Ingestion Tables

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
# MAGIC PARTITIONED BY (update_dt)
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
# MAGIC PARTITIONED BY (update_dt)
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
# MAGIC PARTITIONED BY (event_dt)
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.autoOptimize.autoCompact' = 'false',
# MAGIC   'delta.autoOptimize.optimizeWrite' = 'true'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.bronzedailymarket') (
# MAGIC   cdc_flag STRING, 
# MAGIC   cdc_dsn BIGINT, 
# MAGIC   dm_date DATE, 
# MAGIC   dm_s_symb STRING, 
# MAGIC   dm_close DOUBLE, 
# MAGIC   dm_high DOUBLE, 
# MAGIC   dm_low DOUBLE, 
# MAGIC   dm_vol INT
# MAGIC )
# MAGIC PARTITIONED BY (dm_date)
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
# MAGIC PARTITIONED BY (event_dt)
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
# MAGIC PARTITIONED BY (event_dt)
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
# MAGIC PARTITIONED BY (event_dt)
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.autoOptimize.autoCompact' = 'false',
# MAGIC   'delta.autoOptimize.optimizeWrite' = 'true'
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC # Create list of dates that will loop in subsequent step of the pipeline 
# MAGIC - Set the list of dates as a jobs task value and use "for each" task loop to execute each batch date in order

# COMMAND ----------

from datetime import date, timedelta, datetime

batch_date_ls = []
start_date    = datetime(2015, 7, 6)
for dt_interval in range(0, 730):
  batch_date_ls.append((start_date + timedelta(days=dt_interval)).strftime("%Y-%m-%d"))
dbutils.jobs.taskValues.set(key = "batch_date_ls", value = batch_date_ls)