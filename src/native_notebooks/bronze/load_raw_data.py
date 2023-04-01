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

with open("../../tools/traditional_config.json", "r") as json_conf:
  tables_conf = json.load(json_conf)['tables']

# The raw tables we want to load direct include all staging tables AND all Warehouse tables that are considered BRONZE layer 
all_tables = ([k for (k,v) in tables_conf.items() if v['layer']=='bronze'])
all_tables.sort()
user_name = spark.sql("select current_user()").collect()[0][0].split("@")[0].replace(".","_")

dbutils.widgets.text("wh_db", f"{user_name}_TPCDI",'Root name of Target Warehouse')
dbutils.widgets.text("tpcdi_directory", "/tmp/tpcdi/", "Directory where Raw Files are located")
dbutils.widgets.text("scale_factor", "10", "Scale factor")
dbutils.widgets.dropdown("table", all_tables[0], all_tables, "Target Table Name")

wh_db = f"{dbutils.widgets.get('wh_db')}_wh"
staging_db = f"{dbutils.widgets.get('wh_db')}_stage"
scale_factor = dbutils.widgets.get("scale_factor")
tpcdi_directory = dbutils.widgets.get("tpcdi_directory")
files_directory = f"{tpcdi_directory}sf={scale_factor}"
tgt_table = dbutils.widgets.get("table")

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
  checkpoint_path = f"{files_directory}/_checkpoints/{table}"
  dbutils.fs.rm(checkpoint_path, True) #Drop existing checkpoint if one exists
  tgt_db = wh_db if table_conf['db'] == 'wh' else staging_db
  df.writeStream.option("checkpointLocation", checkpoint_path).trigger(availableNow=True).toTable(f"{tgt_db}.{table}")

# COMMAND ----------

build_autoloader_stream(tgt_table)
