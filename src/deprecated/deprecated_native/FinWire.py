# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

dbutils.widgets.text("env",'a01','Name of the environment')
dbutils.widgets.text("files_directory", "/tmp/tpc-di", "Directory where Files are located")
dbutils.widgets.text("scale_factor", "10", "Scale factor")

env = dbutils.widgets.get("env")
files_directory = dbutils.widgets.get("files_directory")
scale_factor = dbutils.widgets.get("scale_factor")
wh_db = f"{env}_tpcdi_warehouse"

# COMMAND ----------

spark.read.text(f'{files_directory}/{scale_factor}/Batch1/FINWIRE[0-9][0-9][0-9][0-9]Q[1-4]').withColumn('rectype', substring(col('value'), 16, 3)).write.mode('overwrite').partitionBy('rectype').saveAsTable(f"{wh_db}.finwire")
