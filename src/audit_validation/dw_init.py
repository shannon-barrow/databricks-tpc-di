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
import string

with open("../tools/traditional_config.json", "r") as json_conf:
  conf = json.load(json_conf)

for config in conf['spark_conf']:
  spark.conf.set(f"{config}", f"{conf['spark_conf'][config]}")

table_conf = conf['tables']

# COMMAND ----------

user_name = spark.sql("select lower(regexp_replace(split(current_user(), '@')[0], '(\\\W+)', ' '))").collect()[0][0]
default_catalog = 'tpcdi' if spark.conf.get('spark.databricks.unityCatalog.enabled') == 'true' else 'hive_metastore'
default_wh = f"{string.capwords(user_name).replace(' ','_')}_TPCDI"

dbutils.widgets.text("catalog", default_catalog, 'Target Catalog')
dbutils.widgets.text("wh_db", default_wh,'Target Database')

catalog = dbutils.widgets.get("catalog")
wh_db = f"{dbutils.widgets.get('wh_db')}"
staging_db = f"{dbutils.widgets.get('wh_db')}_stage"

# COMMAND ----------

# DBTITLE 1,DROP and CREATE Databases
catalog_exists = spark.sql(f"SELECT count(*) FROM system.information_schema.tables WHERE table_catalog = '{catalog}'").first()[0] > 0

if catalog != 'hive_metastore' and not catalog_exists:
  spark.sql(f"""CREATE CATALOG IF NOT EXISTS {catalog}""")
  spark.sql(f"""GRANT ALL PRIVILEGES ON CATALOG {catalog} TO `account users`""")

spark.sql(f"""DROP DATABASE IF EXISTS {catalog}.{wh_db} CASCADE""")
spark.sql(f"""DROP DATABASE IF EXISTS {catalog}.{staging_db} CASCADE""")
spark.sql(f"""CREATE DATABASE {catalog}.{wh_db} COMMENT 'TPC-DI benchmark Warehouse Database for {user_name}'""")
spark.sql(f"""CREATE DATABASE {catalog}.{staging_db} COMMENT 'TPC-DI benchmark Staging Database for {user_name}'""")
spark.sql(f"USE CATALOG {catalog}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Programatically Create Warehouse Tables via Metadata 
# MAGIC Table configs are located in the traditional_config.json file

# COMMAND ----------

def create_table (table):
  tgt_db = wh_db if table_conf[table]['db'] == 'wh' else staging_db
  spark.sql(f"USE {tgt_db}")
  constraints = '' if catalog == 'hive_metastore' else str(table_conf[table].get('constraints') or '')
  schema = table_conf[table]['raw_schema'] + str(table_conf[table].get('add_tgt_schema') or '') + constraints
  part = str(table_conf[table].get('partition') or '')
  spark.sql(f"""
    CREATE OR REPLACE TABLE {table} ({schema}) USING DELTA {part} TBLPROPERTIES (
      delta.tuneFileSizesForRewrites = true, 
      delta.autoOptimize.optimizeWrite = true,
      delta.enableDeletionVectors=true
      );
  """)

# COMMAND ----------

for table_name in conf['tables']:
  create_table(table_name)
