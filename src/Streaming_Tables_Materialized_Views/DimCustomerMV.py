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

query = f"""CREATE {table_or_mv} IF NOT EXISTS {catalog}.{wh_db}.DimCustomer AS SELECT 
  sk_customerid,
  c.customerid,
  c.taxid,
  c.status,
  c.lastname,
  c.firstname,
  c.middleinitial,
  if(c.gender IN ('M', 'F'), c.gender, 'U') gender,
  c.tier,
  c.dob,
  c.addressline1,
  c.addressline2,
  c.postalcode,
  c.city,
  c.stateprov,
  c.country,
  c.phone1,
  c.phone2,
  c.phone3,
  c.email1,
  c.email2,
  r_nat.TX_NAME as nationaltaxratedesc,
  r_nat.TX_RATE as nationaltaxrate,
  r_lcl.TX_NAME as localtaxratedesc,
  r_lcl.TX_RATE as localtaxrate,
  p.agencyid,
  p.creditrating,
  p.networth,
  p.marketingnameplate,
  c.iscurrent,
  c.batchid,
  c.effectivedate,
  c.enddate
FROM {catalog}.{staging_db}.DimCustomerStg c
JOIN {catalog}.{wh_db}.TaxRate r_lcl 
  ON c.LCL_TX_ID = r_lcl.TX_ID
JOIN {catalog}.{wh_db}.TaxRate r_nat 
  ON c.NAT_TX_ID = r_nat.TX_ID
LEFT JOIN {catalog}.{wh_db}.Prospect p 
  on upper(p.lastname) = upper(c.lastname)
  and upper(p.firstname) = upper(c.firstname)
  and upper(p.addressline1) = upper(c.addressline1)
  and upper(nvl(p.addressline2, '')) = upper(nvl(c.addressline2, ''))
  and upper(p.postalcode) = upper(c.postalcode)"""

# COMMAND ----------

display(serverless_client.sql(sql_statement = query))
