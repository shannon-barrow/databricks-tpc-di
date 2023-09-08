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
dbutils.widgets.text("tpcdi_directory", "/Volumes/tpcdi/tpcdi_raw_data/tpcdi_volume/", "Directory where Raw Files are located")

catalog = dbutils.widgets.get("catalog")
dbutils.widgets.dropdown("table_or_st", "STREAMING TABLE", ['TABLE', 'STREAMING TABLE'], "Table Type")
table_or_st = "OR REFRESH STREAMING" if dbutils.widgets.get("table_or_st") == "STREAMING TABLE" else 'OR REPLACE'
wh_db = dbutils.widgets.get('wh_db')
staging_db = f"{wh_db}_stage"
warehouse_id = dbutils.widgets.get("warehouse_id")
serverless_client = ServerlessClient(warehouse_id=warehouse_id)

db_schema = "sk_brokerid BIGINT COMMENT 'Surrogate key for broker', brokerid BIGINT COMMENT 'Natural key for broker', managerid BIGINT COMMENT 'Natural key for manager’s HR record', firstname STRING COMMENT 'First name', lastname STRING COMMENT 'Last Name', middleinitial STRING COMMENT 'Middle initial', branch STRING COMMENT 'Facility in which employee has office', office STRING COMMENT 'Office number or description', phone STRING COMMENT 'Employee phone number', iscurrent BOOLEAN COMMENT 'True if this is the current record', batchid INT COMMENT 'Batch ID when this record was inserted', effectivedate DATE COMMENT 'Beginning of date range when this record was the current record', enddate DATE COMMENT 'Ending of date range when this record was the current record. A record that is not expired will use the date 9999-12-31.'"

# COMMAND ----------

query = f"""
    CREATE {table_or_st} TABLE {catalog}.{wh_db}.DimBroker AS SELECT
      *, bigint(concat(brokerid,date_format(effectivedate, 'DDDyyyy'))) sk_brokerid
    FROM (
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
        (SELECT min(to_date(datevalue)) as effectivedate FROM {catalog}.{wh_db}.DimDate) effectivedate,
        date('9999-12-31') enddate
      FROM {catalog}.{staging_db}.HRHistory
      WHERE employeejobcode = 314
    )"""

# COMMAND ----------

display(serverless_client.sql(sql_statement = query))
