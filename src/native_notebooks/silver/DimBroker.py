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

with open("../../tools/traditional_config.json", "r") as json_conf:
  table_conf = json.load(json_conf)['views']['HR']
  
user_name = spark.sql("select current_user()").collect()[0][0].split("@")[0].replace(".","_")
dbutils.widgets.text("wh_db", f"{user_name}_TPCDI",'Root name of Target Warehouse')
dbutils.widgets.text("tpcdi_directory", "/tmp/tpcdi/", "Directory where Raw Files are located")
dbutils.widgets.text("scale_factor", "10", "Scale factor")

wh_db = f"{dbutils.widgets.get('wh_db')}_wh"
scale_factor = dbutils.widgets.get("scale_factor")
tpcdi_directory = dbutils.widgets.get("tpcdi_directory")
files_directory = f"{tpcdi_directory}sf={scale_factor}"

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

# MAGIC %python
# MAGIC spark.sql(f"ANALYZE TABLE {wh_db}.DimBroker COMPUTE STATISTICS")
