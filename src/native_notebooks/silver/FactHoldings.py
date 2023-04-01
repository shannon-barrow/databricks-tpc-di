# Databricks notebook source
# MAGIC %md
# MAGIC # FactHoldings

# COMMAND ----------

# MAGIC %md
# MAGIC # Setup

# COMMAND ----------

import json

with open("../../tools/traditional_config.json", "r") as json_conf:
  table_conf = json.load(json_conf)['views']['HoldingHistory']
user_name = spark.sql("select current_user()").collect()[0][0].split("@")[0].replace(".","_")

dbutils.widgets.text("wh_db", f"{user_name}_TPCDI",'Root name of Target Warehouse')
dbutils.widgets.text("tpcdi_directory", "/tmp/tpcdi/", "Directory where Raw Files are located")
dbutils.widgets.text("scale_factor", "10", "Scale factor")
dbutils.widgets.text("batch_id", "1", "Batch ID (1,2,3)")

batch_id = dbutils.widgets.get("batch_id")
wh_db = f"{dbutils.widgets.get('wh_db')}_wh"
staging_db = f"{dbutils.widgets.get('wh_db')}_stage"
scale_factor = dbutils.widgets.get("scale_factor")
tpcdi_directory = dbutils.widgets.get("tpcdi_directory")
files_directory = f"{tpcdi_directory}sf={scale_factor}"

# COMMAND ----------

# MAGIC %md
# MAGIC # This Notebook is for historical and incremental loads
# MAGIC * When it is historical load (batchid = 1) reads from temp view of raw TEXT file HoldingHistory
# MAGIC * Incremental (batches 2 and 3) are being loaded via autoloader into staging DB HoldingIncremental table

# COMMAND ----------

if(batch_id == '1'):
  spark.read.csv(
    f"{files_directory}/{table_conf['path']}/{table_conf['filename']}", 
    schema=table_conf['raw_schema'], 
    sep=table_conf['sep'], 
    header=table_conf['header'], 
    inferSchema=False).createOrReplaceTempView("HoldingHistory")
else:
  spark.sql(f"""
    CREATE OR REPLACE TEMPORARY VIEW HoldingHistory AS SELECT
      * except(cdc_flag, cdc_dsn)
    FROM {staging_db}.HoldingIncremental
    WHERE batchid = cast({batch_id} as int)
  """)

# COMMAND ----------

spark.sql(f"""
  INSERT INTO {wh_db}.FactHoldings 
  SELECT
    hh_h_t_id tradeid,
    hh_t_id currenttradeid,
    sk_customerid,
    sk_accountid,
    sk_securityid,
    sk_companyid,
    sk_closedateid sk_dateid,
    sk_closetimeid sk_timeid,
    tradeprice currentprice,
    hh_after_qty currentholding,
    {batch_id} batchid
  FROM HoldingHistory hh
  JOIN {wh_db}.DimTrade dt
    ON tradeid = hh_t_id
""")
