# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook Purpose
# MAGIC * This notebook is intended to be reused for ALL RAW data ingestion into BRONZE tables.
# MAGIC * It is metadata-driven and code is generic enough to allow for all required raw ingestion to happen
# MAGIC * The Workflow will have an instance of this notebook running for each of the raw tables (reduces the need for extra code/notebooks for every bronze table)

# COMMAND ----------

# MAGIC %md
# MAGIC # Setup

# COMMAND ----------

import json
import string

with open("../../tools/traditional_config.json", "r") as json_conf:
  tables_conf = json.load(json_conf)['tables']
  
# The raw tables we want to load direct include all staging tables AND all Warehouse tables that are considered BRONZE layer 
all_tables = ([k for (k,v) in tables_conf.items() if v['layer']=='bronze'])
all_tables.sort()
user_name = spark.sql("select lower(regexp_replace(split(current_user(), '@')[0], '(\\\W+)', ' '))").collect()[0][0]
default_catalog = 'tpcdi' if spark.conf.get('spark.databricks.unityCatalog.enabled') == 'true' else 'hive_metastore'
default_wh = f"{string.capwords(user_name).replace(' ','_')}_TPCDI"

dbutils.widgets.text("tpcdi_directory", "/tmp/tpcdi/", "Directory where Raw Files are located")
dbutils.widgets.text("scale_factor", "10", "Scale factor")
dbutils.widgets.text("catalog", default_catalog, 'Target Catalog')
dbutils.widgets.text("wh_db", default_wh,'Target Database')
dbutils.widgets.dropdown("table", all_tables[0], all_tables, "Target Table Name")

catalog = dbutils.widgets.get("catalog")
wh_db = f"{dbutils.widgets.get('wh_db')}"
staging_db = f"{dbutils.widgets.get('wh_db')}_stage"
tgt_table = dbutils.widgets.get("table")
scale_factor = dbutils.widgets.get("scale_factor")
tpcdi_directory = dbutils.widgets.get("tpcdi_directory")
files_directory = f"{tpcdi_directory}sf={scale_factor}"
checkpoint_root = f"{tpcdi_directory}user_checkpoints/{user_name.replace(' ','_')}/sf={scale_factor}/"

# COMMAND ----------

# MAGIC %md
# MAGIC # Reusable Programattic Code to use Metadata to Build out all Raw tables 
# MAGIC * Use the JSON metadata to Provide Needed Properties of the table. Table configs are located in the traditional_config.json file
# MAGIC * This makes this notebook reusable as it is metadata-driven

# COMMAND ----------

# DBTITLE 1,Helper Function to Build out the Autoloader Streaming Tables. 
def build_autoloader_stream(table):
  table_conf = tables_conf[table]
  file_format = str(table_conf.get('file_format') or 'csv')
  df = spark.readStream.format('cloudFiles').option('cloudFiles.format', file_format).option("pathGlobfilter", table_conf['filename']).option("inferSchema", False) 
  if file_format == 'csv': 
    df = df.schema(table_conf['raw_schema']).option("delimiter", table_conf['sep']).option("header", table_conf['header'])
  df = df.load(f"{files_directory}/{table_conf['path']}")
  if table_conf.get('add_tgt_query') is not None:
    df = df.selectExpr("*", table_conf.get('add_tgt_query'))
  
  # Now Write
  checkpoint_path = f"{checkpoint_root}/{table}"
  dbutils.fs.rm(checkpoint_path, True) #Drop existing checkpoint if one exists
  tgt_db = wh_db if table_conf['db'] == 'wh' else staging_db
  df.writeStream.option("checkpointLocation", f"{checkpoint_root}/{table}").trigger(availableNow=True).toTable(f"{catalog}.{tgt_db}.{table}")

# COMMAND ----------

build_autoloader_stream(tgt_table)
