# Databricks notebook source
import yaml

dbutils.widgets.text("env",'a01','Name of the environment')
dbutils.widgets.text("files_directory", "/tmp/tpc-di", "Directory where Files are located")
dbutils.widgets.text("scale_factor", "10", "Scale factor")

env = dbutils.widgets.get("env")
files_directory = dbutils.widgets.get("files_directory")
scale_factor = dbutils.widgets.get("scale_factor")
wh_db = f"{env}_tpcdi_warehouse"

yaml_location = "../_init/table_schemas.yml"  # YAML file holds all the schema properties so they can be reused across notebooks
with open(yaml_location, "r") as schemas:
  schema = yaml.safe_load(schemas)['audit']['schema']

# COMMAND ----------

batchaudit_df = spark.read.csv(f'{files_directory}/{scale_factor}/Batch*/*_audit.csv', schema=schema, header=True)
audit_df = spark.read.csv(f'{files_directory}/{scale_factor}/*_audit.csv', schema=schema, header=True)

# COMMAND ----------

batchaudit_df.unionAll(audit_df).write.format("delta").mode("append").saveAsTable(f"{wh_db}.audit")
