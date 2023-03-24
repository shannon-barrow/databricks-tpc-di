# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook Purpose
# MAGIC * This file captures all of the table creation commands required to initialize the DW.
# MAGIC * DDLs in this file should not access staging data. (No external tables)

# COMMAND ----------

# MAGIC %md
# MAGIC # Setup

# COMMAND ----------

import json

spark.sql("set spark.databricks.delta.identityColumn.enabled = true") # Enable Delta Lake Identity Column feature
with open("../tools/traditional_config.json", "r") as json_conf:
  table_conf = json.load(json_conf)

# COMMAND ----------

user_name = spark.sql("select current_user()").collect()[0][0].split("@")[0].replace(".","_")
dbutils.widgets.text("wh_db", f"{user_name}_TPCDI",'Root name of Target Warehouse')
wh_db = f"{dbutils.widgets.get('wh_db')}_wh"
staging_db = f"{dbutils.widgets.get('wh_db')}_stage"

# COMMAND ----------

# DBTITLE 1,DROP and CREATE Databases
spark.sql(f"""DROP DATABASE IF EXISTS {wh_db} CASCADE""")
spark.sql(f"""DROP DATABASE IF EXISTS {staging_db} CASCADE""")
spark.sql(f"""CREATE DATABASE {wh_db} COMMENT 'TPC-DI benchmark Warehouse Database for {user_name}'""")
spark.sql(f"""CREATE DATABASE {staging_db} COMMENT 'TPC-DI benchmark Staging Database for {user_name}'""")

# COMMAND ----------

# MAGIC %md
# MAGIC # Programatically Create Warehouse Tables via Metadata 
# MAGIC Table configs are located in the traditional_config.json file

# COMMAND ----------

def create_table (table):
  tgt_db = wh_db if table_conf['tables'][table]['db'] == 'wh' else staging_db
  schema = table_conf['tables'][table]['raw_schema'] + str(table_conf['tables'][table].get('add_tgt_schema') or '')
  part = str(table_conf['tables'][table].get('partition') or '')
  spark.sql(f"""
    CREATE OR REPLACE TABLE {tgt_db}.{table} ({schema}) USING DELTA {part} TBLPROPERTIES (
      delta.tuneFileSizesForRewrites = true, 
      delta.autoOptimize.optimizeWrite = true
      --, delta.enableDeletionVectors=true
      );""")

# COMMAND ----------

for table_name in table_conf['tables']:
  create_table(table_name)
