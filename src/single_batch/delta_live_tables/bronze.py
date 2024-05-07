# Databricks notebook source
import dlt
import json
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC # Declare table schema variables for the streams

# COMMAND ----------

tpcdi_directory = spark.conf.get('files_directory')
scale_factor = spark.conf.get('scale_factor')
bronze_tables = json.loads(spark.conf.get('bronze_tables'))

# COMMAND ----------

# MAGIC %md
# MAGIC # Helper Functions to Build out the Autoloader Streaming Tables. 
# MAGIC * Use the DLT Pipeline Config Parameters (in the JSON of the DLT Pipeine) to Provide Needed Properties of the table. 
# MAGIC * This makes the job metadata driven

# COMMAND ----------

def build_autoloader_stream(tbl):
  path = f"{tpcdi_directory}sf={scale_factor}/" + (tbl.get('path') or "Batch1")
  tgt_query = tbl.get('tgt_query') or "*"
  return spark.sql(f"""
    SELECT {tgt_query}
    FROM read_files(
      "{path}",
      format => "csv",
      inferSchema => False, 
      header => False,
      sep => "|",
      fileNamePattern => "{tbl.get('filename')}", 
      schema => "{tbl.get('raw_schema')}"
    )"""
  )

def generate_tables(tbl):
  tbl_name = tbl.get("table")
  if tbl.get("part") is not None:
    @dlt.table(
      name=tbl_name,
      partition_cols=[tbl.get("part")]
    )
    def create_table(): 
      return build_autoloader_stream(tbl)
  else:
    @dlt.table(name=tbl_name)
    def create_table(): 
      return build_autoloader_stream(tbl)

# COMMAND ----------

# MAGIC %md
# MAGIC # Loop through each bronze table to create the DLT streaming table

# COMMAND ----------

for table in bronze_tables:
  generate_tables(table)
