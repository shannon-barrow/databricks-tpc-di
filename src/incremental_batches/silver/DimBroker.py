# Databricks notebook source
# MAGIC %md
# MAGIC # Dimbroker
# MAGIC * Silver Layer
# MAGIC * Per TPC, assume no updates happen.  So this table needs to be loaded only once
# MAGIC * Reads from HR file, which we make a view first

# COMMAND ----------

# MAGIC %md
# MAGIC # Setup

# COMMAND ----------

import json
import string

with open("../../tools/traditional_config.json", "r") as json_conf:
  table_conf = json.load(json_conf)['views']['HR']
  
user_name = spark.sql("select lower(regexp_replace(split(current_user(), '@')[0], '(\\\W+)', ' '))").collect()[0][0]
default_catalog = 'tpcdi' if spark.conf.get('spark.databricks.unityCatalog.enabled') == 'true' else 'hive_metastore'
default_wh = f"{string.capwords(user_name).replace(' ','_')}_TPCDI"

dbutils.widgets.text("catalog", default_catalog, 'Target Catalog')
dbutils.widgets.text("wh_db", default_wh,'Target Database')
dbutils.widgets.text("tpcdi_directory", "/tmp/tpcdi/", "Directory where Raw Files are located")
dbutils.widgets.text("scale_factor", "10", "Scale factor")

catalog = dbutils.widgets.get("catalog")
wh_db = f"{dbutils.widgets.get('wh_db')}"
scale_factor = dbutils.widgets.get("scale_factor")
tpcdi_directory = dbutils.widgets.get("tpcdi_directory")
files_directory = f"{tpcdi_directory}sf={scale_factor}"
spark.sql(f"USE CATALOG {catalog}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Create View on HR Then Write into DimBroker

# COMMAND ----------

spark.read.csv(f"{files_directory}/{table_conf['path']}/{table_conf['filename']}", 
               schema=table_conf['raw_schema'], 
               sep=table_conf['sep'], 
               header=table_conf['header'], 
               inferSchema=False).createOrReplaceTempView('v_hr')

# COMMAND ----------

spark.sql(f"""
INSERT OVERWRITE {wh_db}.DimBroker (brokerid, managerid, firstname, lastname, middleinitial, branch, office, phone, iscurrent, batchid, effectivedate, enddate)
SELECT
  cast(employeeid as BIGINT) brokerid,
  cast(managerid as BIGINT) managerid,
  employeefirstname firstname,
  employeelastname lastname,
  employeemi middleinitial,
  employeebranch branch,
  employeeoffice office,
  employeephone phone,
  true iscurrent,
  1 batchid,
  (SELECT min(to_date(datevalue)) as effectivedate FROM {wh_db}.DimDate) effectivedate,
  date('9999-12-31') enddate
FROM v_hr
WHERE employeejobcode = 314
""")

# COMMAND ----------

spark.sql(f"ANALYZE TABLE {wh_db}.DimBroker COMPUTE STATISTICS FOR ALL COLUMNS")
