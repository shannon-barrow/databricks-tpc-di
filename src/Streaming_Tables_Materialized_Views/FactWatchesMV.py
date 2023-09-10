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

query = f"""CREATE {table_or_mv} IF NOT EXISTS {catalog}.{wh_db}.FactWatches AS 
SELECT
  c.sk_customerid sk_customerid,
  s.sk_securityid sk_securityid,
  sk_dateid_dateplaced,
  sk_dateid_dateremoved,
  wh.batchid
FROM (
  SELECT * EXCEPT(w_dts)
  FROM (
    SELECT
      customerid,
      symbol,
      coalesce(sk_dateid_dateplaced, last_value(sk_dateid_dateplaced) IGNORE NULLS OVER (
        PARTITION BY customerid, symbol ORDER BY w_dts)) sk_dateid_dateplaced,
      coalesce(sk_dateid_dateremoved, last_value(sk_dateid_dateremoved) IGNORE NULLS OVER (
        PARTITION BY customerid, symbol ORDER BY w_dts)) sk_dateid_dateremoved,
      coalesce(dateplaced, last_value(dateplaced) IGNORE NULLS OVER (
        PARTITION BY customerid, symbol ORDER BY w_dts)) dateplaced,
      w_dts,
      coalesce(batchid, last_value(batchid) IGNORE NULLS OVER (
        PARTITION BY customerid, symbol ORDER BY w_dts)) batchid
    FROM ( 
      SELECT 
        wh.w_c_id customerid,
        wh.w_s_symb symbol,
        if(w_action = 'ACTV', d.sk_dateid, null) sk_dateid_dateplaced,
        if(w_action = 'CNCL', d.sk_dateid, null) sk_dateid_dateremoved,
        if(w_action = 'ACTV', d.datevalue, null) dateplaced,
        wh.w_dts,
        batchid 
      FROM (
        SELECT *, 1 batchid FROM {catalog}.{staging_db}.WatchHistory
        UNION ALL
        SELECT * except(cdc_flag, cdc_dsn) FROM {catalog}.{staging_db}.WatchIncremental) wh
      JOIN {catalog}.{wh_db}.DimDate d
        ON d.datevalue = date(wh.w_dts)))
  QUALIFY ROW_NUMBER() OVER (PARTITION BY customerid, symbol ORDER BY w_dts desc) = 1) wh
JOIN {catalog}.{wh_db}.DimSecurity s 
  ON 
    s.symbol = wh.symbol
    AND wh.dateplaced >= s.effectivedate 
    AND wh.dateplaced < s.enddate
JOIN {catalog}.{staging_db}.DimCustomerStg c 
  ON
    wh.customerid = c.customerid
    AND wh.dateplaced >= c.effectivedate 
    AND wh.dateplaced < c.enddate"""

# COMMAND ----------

display(serverless_client.sql(sql_statement = query))
