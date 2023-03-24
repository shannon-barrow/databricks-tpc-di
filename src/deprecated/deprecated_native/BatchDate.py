# Databricks notebook source
import yaml
from pyspark.sql.functions import *
dbutils.widgets.text("env",'a01','Name of the environment')
dbutils.widgets.text("files_directory", "/tmp/tpc-di", "Directory where Files are located")
dbutils.widgets.text("scale_factor", "10", "Scale factor")
dbutils.widgets.text("batch_id", "1", "Batch ID (1,2,3)")

env = dbutils.widgets.get("env")
files_directory = dbutils.widgets.get("files_directory")
batch_id = dbutils.widgets.get("batch_id")
scale_factor = dbutils.widgets.get("scale_factor")
wh_db = f"{env}_tpcdi_warehouse"

yaml_location = "../_init/table_schemas.yml"  # YAML file holds all the schema properties so they can be reused across notebooks
with open(yaml_location, "r") as schemas:
  _schema = yaml.safe_load(schemas)['batchDate']['schema']

# COMMAND ----------

# DBTITLE 1,Doesn't Work with property: "spark.databricks.photon.allDataSources.enabled"
spark.read.csv(f'{files_directory}/{scale_factor}/Batch*/BatchDate.txt', schema=_schema, sep='|', header=False, inferSchema=False).drop('batchid') \
  .selectExpr("*", "cast(substring(input_file_name() FROM (position('/Batch', input_file_name()) + 6) FOR 1) as int) batchid")

# COMMAND ----------

spark.read.csv(f'{files_directory}/{scale_factor}/Batch1/BatchDate.txt', schema=_schema, sep='|', header=False, inferSchema=False).withColumn('batchid', lit(1)) \
  .unionAll(spark.read.csv(f'{files_directory}/{scale_factor}/Batch2/BatchDate.txt', schema=_schema, sep='|', header=False, inferSchema=False).withColumn('batchid', lit(2))) \
  .unionAll(spark.read.csv(f'{files_directory}/{scale_factor}/Batch3/BatchDate.txt', schema=_schema, sep='|', header=False, inferSchema=False).withColumn('batchid', lit(3))) \
  .write.format("delta").mode("overwrite").saveAsTable(f"{wh_db}.BatchDate")
