# Databricks notebook source
import json
import string
from pyspark.sql.functions import col
from dbsqlclient import ServerlessClient

# COMMAND ----------

with open("../tools/streaming_tables_config.json", "r") as json_conf:
  views_conf = json.load(json_conf)['views']
user_name = spark.sql("select lower(regexp_replace(split(current_user(), '@')[0], '(\\\W+)', ' '))").collect()[0][0]
default_catalog = 'tpcdi' if spark.conf.get('spark.databricks.unityCatalog.enabled') == 'true' else 'hive_metastore'
dbutils.widgets.text("catalog", default_catalog, 'Target Catalog')
dbutils.widgets.text("scale_factor", "10", "Scale factor")
scale_factor = dbutils.widgets.get("scale_factor")
default_wh = f"{string.capwords(user_name).replace(' ','_')}_TPCDI_STMV_{scale_factor}"
dbutils.widgets.text("wh_db", default_wh,'Target Database')
dbutils.widgets.text("tpcdi_directory", "/Volumes/tpcdi/tpcdi_raw_data/tpcdi_volume/", "Directory where Raw Files are located")
dbutils.widgets.text("warehouse_id", '', "Warehouse ID")

catalog = dbutils.widgets.get("catalog")
wh_db = dbutils.widgets.get('wh_db')
staging_db = f"{wh_db}_stage"
tpcdi_directory = dbutils.widgets.get('tpcdi_directory')
files_directory = f"{tpcdi_directory}sf={scale_factor}"
warehouse_id = dbutils.widgets.get("warehouse_id")
serverless_client = ServerlessClient(warehouse_id=warehouse_id)
mv_list = "'dailymarketstg'" + ", 'dimcustomerstg'" + ", 'tempsumfibasiceps'"

# COMMAND ----------

if catalog != 'hive_metastore':
  catalog_exists = spark.sql(f"SELECT count(*) FROM system.information_schema.tables WHERE table_catalog = '{catalog}'").first()[0] > 0
  if not catalog_exists:
    spark.sql(f"""CREATE CATALOG IF NOT EXISTS {catalog}""")
    spark.sql(f"""GRANT ALL PRIVILEGES ON CATALOG {catalog} TO `account users`""")
  mvs = spark.sql(f"""
    select table_name, if(table_type='MANAGED', 'TABLE', 'MATERIALIZED VIEW') table_type
    FROM {catalog}.information_schema.tables 
    WHERE table_schema = lower('{staging_db}')
    AND table_name in ({mv_list})
  """).toPandas()

  for index, row in mvs.iterrows():
    serverless_client.sql(sql_statement = f"DROP {row['table_type']} IF EXISTS {catalog}.{staging_db}.{row['table_name']}")
else:
  for mv in mv_list:
    serverless_client.sql(sql_statement = f"DROP TABLE IF EXISTS {catalog}.{staging_db}.{mv}")

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"DROP DATABASE IF EXISTS {wh_db} cascade")
spark.sql(f"CREATE DATABASE {wh_db}")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {staging_db}")
spark.sql(f"USE {staging_db}")
serverless_client.sql(sql_statement = f"DROP TABLE IF EXISTS {catalog}.{wh_db}.finwire")

notebook_run_timeout = max(600, int(int(scale_factor)/4))   # dbutils.notebook.run requires a timeout period. Adjust to the data size
if spark.sql('show tables').filter(col("tableName") == 'customermgmt').count() == 0:
  dbutils.notebook.run("../native_notebooks/bronze/CustomerMgmtRaw", notebook_run_timeout, {"catalog": catalog, "wh_db": wh_db, "tpcdi_directory": tpcdi_directory, "scale_factor": scale_factor})

# COMMAND ----------

for view in views_conf:
  conf = views_conf[view]
  cols = []
  raw_query = f"""split(value, "[{conf['sep']}]") val"""
  for idx, col in list(enumerate(conf['raw_schema'].split(','))):
    cols.append(f"{col.strip().split(' ')[1]}(val[{idx}]) {col.strip().split(' ')[0]}")
  if conf.get('add_batchid') == 'True': 
    cols.append('batchid')
    raw_query = raw_query + ", cast(substring(_metadata.file_path FROM (position('/Batch', _metadata.file_path) + 6) FOR 1) as int) batchid"
  # query = ', '.join(cols) if conf.get('add_tgt_query') is None else ', '.join(cols) + conf.get('add_tgt_query')
  spark.sql(f"""CREATE VIEW IF NOT EXISTS {catalog}.{staging_db}.{view} AS SELECT {', '.join(cols)} FROM (
    SELECT {raw_query} FROM text.`{files_directory}/{conf['path']}/{conf['filename']}`)
  """)
