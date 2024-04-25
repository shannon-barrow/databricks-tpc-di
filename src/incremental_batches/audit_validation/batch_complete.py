# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook Purpose
# MAGIC * This notebook is run after each Batch to provide a log entry that the batch has completed

# COMMAND ----------

import string

user_name = spark.sql("select lower(regexp_replace(split(current_user(), '@')[0], '(\\\W+)', ' '))").collect()[0][0]
default_catalog = 'tpcdi' if spark.conf.get('spark.databricks.unityCatalog.enabled') == 'true' else 'hive_metastore'
default_wh = f"{string.capwords(user_name).replace(' ','_')}_TPCDI"

dbutils.widgets.text("catalog", default_catalog, 'Target Catalog')
dbutils.widgets.text("wh_db", default_wh,'Target Database')
dbutils.widgets.text("batch_id", "1", "Batch ID (1,2,3)")

catalog = dbutils.widgets.get("catalog")
wh_db = f"{dbutils.widgets.get('wh_db')}"
batch_id = dbutils.widgets.get("batch_id")

# COMMAND ----------

spark.sql(f"""
  insert into {catalog}.{wh_db}.DIMessages
  SELECT
    CURRENT_TIMESTAMP() as MessageDateAndTime,
    {batch_id} as BatchID,
    'Phase Complete Record' as MessageSource,
    'Batch Complete' as MessageText,
    'PCR' as MessageType,
    NULL as MessageData
""")
