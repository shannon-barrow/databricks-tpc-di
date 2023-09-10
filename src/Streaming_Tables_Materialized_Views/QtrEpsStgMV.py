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

query = f"""CREATE {table_or_mv} IF NOT EXISTS {catalog}.{staging_db}.tempSumFiBasicEps AS SELECT
  sk_companyid,
  fi_qtr_start_date,
  sum(fi_basic_eps) OVER (PARTITION BY companyid ORDER BY fi_qtr_start_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) - fi_basic_eps sum_fi_basic_eps
FROM {catalog}.{wh_db}.Financial
JOIN {catalog}.{wh_db}.DimCompany
  USING (sk_companyid)"""

# COMMAND ----------

display(serverless_client.sql(sql_statement = query))
