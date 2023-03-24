# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook Purpose
# MAGIC * This notebook is run after each Batch to provide a log entry that the batch has completed

# COMMAND ----------

user_name = spark.sql("select current_user()").collect()[0][0].split("@")[0].replace(".","_")
dbutils.widgets.text("wh_db", f"{user_name}_TPCDI",'Root name of Target Warehouse')
dbutils.widgets.text("batch_id", "1", "Batch ID (1,2,3)")

wh_db = f"{dbutils.widgets.get('wh_db')}_wh"
batch_id = dbutils.widgets.get("batch_id")

# COMMAND ----------

spark.sql(f"""
  insert into {wh_db}.DIMessages
  SELECT
    CURRENT_TIMESTAMP() as MessageDateAndTime,
    {batch_id} as BatchID,
    'Phase Complete Record' as MessageSource,
    'Batch Complete' as MessageText,
    'PCR' as MessageType,
    NULL as MessageData
""")
