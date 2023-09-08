# Databricks notebook source
from dbsqlclient import ServerlessClient
import json
import re
import string

# COMMAND ----------

with open("../tools/streaming_tables_config.json", "r") as json_conf:
  tables_conf = json.load(json_conf)['tables']
all_tables = list(tables_conf.keys())
all_tables.sort()
user_name = spark.sql("select lower(regexp_replace(split(current_user(), '@')[0], '(\\\W+)', ' '))").collect()[0][0]
default_catalog = 'tpcdi' if spark.conf.get('spark.databricks.unityCatalog.enabled') == 'true' else 'hive_metastore'

dbutils.widgets.text("tpcdi_directory", "/tmp/tpcdi/", "Directory where Raw Files are located")
dbutils.widgets.text("scale_factor", "10", "Scale factor")
dbutils.widgets.text("catalog", default_catalog, 'Target Catalog')
scale_factor = dbutils.widgets.get("scale_factor")
default_wh = f"{string.capwords(user_name).replace(' ','_')}_TPCDI_STMV_{scale_factor}"
dbutils.widgets.text("wh_db", default_wh,'Target Database')
dbutils.widgets.dropdown("table", all_tables[0], all_tables, "Target Table Name")
dbutils.widgets.text("warehouse_id", '', "Warehouse ID")

catalog = dbutils.widgets.get("catalog")
dbutils.widgets.dropdown("table_or_st", "STREAMING TABLE", ['TABLE', 'STREAMING TABLE'], "Table Type")
table_or_st = dbutils.widgets.get("table_or_st")
wh_db = dbutils.widgets.get('wh_db')
staging_db = f"{wh_db}_stage"
tgt_table = dbutils.widgets.get("table")
tpcdi_directory = dbutils.widgets.get("tpcdi_directory")
files_directory = f"{tpcdi_directory}sf={scale_factor}"
warehouse_id = dbutils.widgets.get("warehouse_id")
serverless_client = ServerlessClient(warehouse_id=warehouse_id)

# COMMAND ----------

# MAGIC %md
# MAGIC # Reusable Programattic Code to use Metadata to Build out all Raw tables 
# MAGIC * Use the JSON metadata to Provide Needed Properties of the table. Table configs are located in the traditional_config.json file
# MAGIC * This makes this notebook reusable as it is metadata-driven

# COMMAND ----------

def get_streaming_table_query(table):
  table_conf = tables_conf[table]
  file_format = str(table_conf.get('file_format') or 'csv')
  schema = table_conf['raw_schema'] + str(table_conf.get('add_tgt_schema') or '')
  part = str(table_conf.get('partition') or '')
  selectExpr = '*' if table_conf.get('add_tgt_query') is None else f"*, {table_conf.get('add_tgt_query')}"
  if table_conf.get('add_batchid') == 'True': 
    schema = schema + ", batchid INT COMMENT 'Batch ID when this record was inserted'"
    selectExpr = selectExpr + ", cast(substring(_metadata.file_path FROM (position('/Batch', _metadata.file_path) + 6) FOR 1) as int) batchid"
  tgt_db = wh_db if table_conf['db'] == 'wh' else staging_db
  csv_conf = f"header => '{table_conf['header']}', sep => '{table_conf['sep']}', " if file_format == 'csv' else ''

  query = f"""
    CREATE OR REFRESH STREAMING TABLE {catalog}.{tgt_db}.{table} ({schema}) {part}
    AS SELECT {selectExpr}
    FROM STREAM read_files(
      "{files_directory}/{table_conf['path']}", 
      format => "{file_format}",
      inferSchema => False,
      {csv_conf}
      fileNamePattern => "{table_conf['filename']}", 
      schema => "{table_conf['raw_schema']}"
    );
  """

  return query

# COMMAND ----------

def get_ingest_table_query(table):
  table_conf = tables_conf[table]
  part = str(table_conf.get('partition') or '')
  tgt_db = wh_db if table_conf['db'] == 'wh' else staging_db
  schema = table_conf['raw_schema'] + str(table_conf.get('add_tgt_schema') or '')
  if table_conf.get('sep') is None:
    cols = ['value']
    raw_query = 'value'
  else:
    cols = []
    raw_query = f"""split(value, "[{table_conf['sep']}]") val"""
    for idx, col in list(enumerate(table_conf['raw_schema'].split(','))):
      cols.append(f"{col.strip().split(' ')[1]}(val[{idx}]) {col.strip().split(' ')[0]}")
  if table_conf.get('add_batchid') == 'True': 
    cols.append('batchid')
    schema = schema + ", batchid INT COMMENT 'Batch ID when this record was inserted'"
    raw_query = raw_query + ", cast(substring(_metadata.file_path FROM (position('/Batch', _metadata.file_path) + 6) FOR 1) as int) batchid"
  # query = ', '.join(cols) if table_conf.get('add_tgt_query') is None else ', '.join(cols) + table_conf.get('add_tgt_query')
  if table_conf.get('add_tgt_query') is not None: cols.append(table_conf.get('add_tgt_query'))
  query = f"""CREATE OR REPLACE TABLE {catalog}.{tgt_db}.{table} {part} AS SELECT {', '.join(cols)} FROM (
    SELECT {raw_query} FROM text.`{files_directory}/{table_conf['path']}/{table_conf['filename']}`)
  """

  return query

# COMMAND ----------

display(serverless_client.sql(sql_statement = get_ingest_table_query(tgt_table)))

# COMMAND ----------

# display(serverless_client.sql(sql_statement = get_streaming_table_query(tgt_table)))
