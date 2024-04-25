# Databricks notebook source
# MAGIC %md
# MAGIC # FactCashBalances

# COMMAND ----------

# MAGIC %md
# MAGIC # Setup

# COMMAND ----------

import string
import json

with open("../../tools/traditional_config.json", "r") as json_conf:
  table_conf = json.load(json_conf)['views']['CashTransactionHistory']

user_name = spark.sql("select lower(regexp_replace(split(current_user(), '@')[0], '(\\\W+)', ' '))").collect()[0][0]
default_catalog = 'tpcdi' if spark.conf.get('spark.databricks.unityCatalog.enabled') == 'true' else 'hive_metastore'
default_wh = f"{string.capwords(user_name).replace(' ','_')}_TPCDI"

dbutils.widgets.text("tpcdi_directory", "/tmp/tpcdi/", "Directory where Raw Files are located")
dbutils.widgets.text("scale_factor", "10", "Scale factor")
dbutils.widgets.text("catalog", default_catalog, 'Target Catalog')
dbutils.widgets.text("wh_db", default_wh,'Target Database')
dbutils.widgets.text("batch_id", "1", "Batch ID (1,2,3)")

catalog = dbutils.widgets.get("catalog")
wh_db = f"{dbutils.widgets.get('wh_db')}"
staging_db = f"{dbutils.widgets.get('wh_db')}_stage"
batch_id = dbutils.widgets.get("batch_id")
scale_factor = dbutils.widgets.get("scale_factor")
tpcdi_directory = dbutils.widgets.get("tpcdi_directory")
files_directory = f"{tpcdi_directory}sf={scale_factor}"
spark.sql(f"USE CATALOG {catalog}")

# COMMAND ----------

# MAGIC %md
# MAGIC # This Notebook is for historical and incremental loads
# MAGIC * When it is historical load (batchid = 1) reads from temp view of raw TEXT file CashTransaction
# MAGIC * Incremental (batches 2 and 3) are being loaded via autoloader into staging DB CashTransactionIncremental table

# COMMAND ----------

spark.read.csv(
  f"{files_directory}/{table_conf['path']}/{table_conf['filename']}", 
  schema=table_conf['raw_schema'], 
  sep=table_conf['sep'], 
  header=table_conf['header'], 
  inferSchema=False).createOrReplaceTempView("CashTransaction")

# COMMAND ----------

if(batch_id == '1'):
  spark.sql(f"""
    CREATE OR REPLACE TABLE {staging_db}.FactCashBalancesStg PARTITIONED BY (batchid) AS SELECT
      accountid, 
      datevalue, 
      sum(account_daily_total) OVER (partition by accountid order by datevalue) cash,
      batchid
    FROM (
      SELECT 
        accountid,
        datevalue,
        sum(ct_amt) account_daily_total,
        batchid
      FROM (
        SELECT 
          ct_ca_id accountid,
          to_date(ct_dts) datevalue,
          ct_amt,
          1 batchid
        FROM CashTransaction
        UNION ALL
        SELECT 
          ct_ca_id accountid,
          to_date(ct_dts) datevalue,
          ct_amt,
          batchid
        FROM {staging_db}.CashTransactionIncremental
      )
      GROUP BY
        accountid,
        datevalue,
        batchid
    )
    """)
  
  spark.sql(f"ANALYZE TABLE {staging_db}.FactCashBalancesStg COMPUTE STATISTICS FOR ALL COLUMNS")

# COMMAND ----------

spark.sql(f"""
  INSERT INTO {wh_db}.FactCashBalances 
  SELECT 
    sk_customerid, 
    sk_accountid, 
    sk_dateid, 
    fcb.cash,
    fcb.batchid
  FROM {staging_db}.FactCashBalancesStg fcb
  JOIN {wh_db}.DimDate d 
    ON fcb.datevalue = d.datevalue
  LEFT JOIN {wh_db}.DimAccount a 
    ON 
      fcb.accountid = a.accountid
      AND fcb.datevalue >= a.effectivedate 
      AND fcb.datevalue < a.enddate  
  WHERE fcb.batchid = cast({batch_id} as int)
""")
