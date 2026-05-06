# Databricks notebook source
# MAGIC %md
# MAGIC # Augmented Incremental — dbt-variant Setup
# MAGIC
# MAGIC Same as `setup.py` but trimmed for the dbt path:
# MAGIC
# MAGIC - Clones the per-SF shared staging schema into the run schema
# MAGIC   (12 shallow + 6 deep) — same as setup.py.
# MAGIC - Creates `factmarkethistory` empty so the schema is consistent with
# MAGIC   the Classic / SDP variants (dbt's `gold/factmarkethistory.sql`
# MAGIC   incremental writes into it via delete+insert).
# MAGIC - Resets the `_dailybatches` watch dir for `simulate_filedrops`.
# MAGIC - Emits the batch_date list for the parent's for_each loop.
# MAGIC
# MAGIC Skipped vs setup.py:
# MAGIC
# MAGIC - **No empty bronze tables** — the dbt models in `models/bronze/`
# MAGIC   declare their own schemas via `read_files()` and create the
# MAGIC   target tables on first run. Pre-creating with the Classic
# MAGIC   schema (which uses INT/STRING for some columns where
# MAGIC   `read_files()` would produce BIGINT) would cause schema-drift
# MAGIC   issues on append.
# MAGIC - **No streaming checkpoint dirs** — dbt doesn't use Auto Loader
# MAGIC   or Spark Streaming, so the `_checkpoints/` tree is irrelevant.

# COMMAND ----------

import concurrent.futures
import os
import requests

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
batches_dir     = f"{tpcdi_directory}augmented_incremental/_dailybatches/{tgt_db}"

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
    'taxrate', 'dimdate', 'industry', 'tradetype', 'dimbroker',
    'financial', 'companyyeareps', 'dimsecurity', 'statustype',
    'dimcompany', 'dimtime', 'currentaccountbalances',
]
deep_tbls = [
    'dimtrade', 'factwatches', 'factholdings',
    'dimcustomer', 'dimaccount', 'factcashbalances',
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
start_date = datetime(2015, 7, 6)
for dt_interval in range(0, n_batches):
    batch_date_ls.append((start_date + timedelta(days=dt_interval)).strftime("%Y-%m-%d"))
print(f"Emitting {len(batch_date_ls)} batch dates ({batch_date_ls[0]} → {batch_date_ls[-1]})")
dbutils.jobs.taskValues.set(key="batch_date_ls", value=batch_date_ls)
