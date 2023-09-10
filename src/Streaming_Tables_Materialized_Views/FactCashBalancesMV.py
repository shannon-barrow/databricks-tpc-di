# Databricks notebook source
from dbsqlclient import ServerlessClient
import json
import re
import string

# COMMAND ----------

user_name = spark.sql("select lower(regexp_replace(split(current_user(), '@')[0], '(\\\W+)', ' '))").collect()[0][0]
default_catalog = 'tpcdi' if spark.conf.get('spark.databricks.unityCatalog.enabled') == 'true' else 'hive_metastore'
dbutils.widgets.text("scale_factor", "10", "Scale factor")
dbutils.widgets.text("catalog", default_catalog, 'Target Catalog')
scale_factor = dbutils.widgets.get("scale_factor")
default_wh = f"{string.capwords(user_name).replace(' ','_')}_TPCDI_STMV_{scale_factor}"
dbutils.widgets.text("wh_db", default_wh,'Target Database')
dbutils.widgets.text("warehouse_id", '', "Warehouse ID")

catalog = dbutils.widgets.get("catalog")
dbutils.widgets.dropdown("table_or_mv", "MATERIALIZED VIEW", ['TABLE', 'MATERIALIZED VIEW'], "Table Type")
table_or_mv = dbutils.widgets.get("table_or_mv")
wh_db = dbutils.widgets.get('wh_db')
staging_db = f"{wh_db}_stage"
warehouse_id = dbutils.widgets.get("warehouse_id")
serverless_client = ServerlessClient(warehouse_id=warehouse_id)

# COMMAND ----------

query = f"""CREATE {table_or_mv} IF NOT EXISTS {catalog}.{wh_db}.FactCashBalances AS SELECT
  a.sk_customerid, 
  a.sk_accountid, 
  d.sk_dateid, 
  sum(account_daily_total) OVER (partition by c.accountid order by c.datevalue) cash,
  c.batchid
FROM (
  SELECT 
    ct_ca_id accountid,
    to_date(ct_dts) datevalue,
    sum(ct_amt) account_daily_total,
    batchid
  FROM (
    SELECT *, 1 batchid FROM {catalog}.{staging_db}.CashTransactionHistory
    UNION ALL
    SELECT * except(cdc_flag, cdc_dsn)
    FROM {catalog}.{staging_db}.CashTransactionIncremental
  )
  GROUP BY
    accountid,
    datevalue,
    batchid) c 
JOIN {catalog}.{wh_db}.DimDate d 
  ON c.datevalue = d.datevalue
JOIN {catalog}.{wh_db}.DimAccount a 
  ON 
    c.accountid = a.accountid
    AND c.datevalue >= a.effectivedate 
    AND c.datevalue < a.enddate""" 

# COMMAND ----------

display(serverless_client.sql(sql_statement = query))

# COMMAND ----------


