# Databricks notebook source
import yaml

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
  _schema = yaml.safe_load(schemas)['tradeType']['schema']

# COMMAND ----------

spark.read.csv(f'{files_directory}/{scale_factor}/Batch{batch_id}/TradeType.txt', schema=_schema, sep='|', header=False, inferSchema=False).createOrReplaceTempView('v_tt')

# COMMAND ----------

spark.sql(f"""
  INSERT INTO {wh_db}.TradeType
  SELECT *
  FROM v_tt
""")

# COMMAND ----------

spark.sql(f"ANALYZE TABLE {wh_db}.TradeType COMPUTE STATISTICS")
