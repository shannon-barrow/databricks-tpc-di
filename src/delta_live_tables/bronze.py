# Databricks notebook source
import dlt
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC # Helper Functions to Build out the Autoloader Streaming Tables. 
# MAGIC * Use the DLT Pipeline Config Parameters (in the JSON of the DLT Pipeine) to Provide Needed Properties of the table. 
# MAGIC * This makes the job metadata driven

# COMMAND ----------

def build_autoloader_stream(table):
  src_dir = spark.conf.get(f'{table}.path')
  return spark.readStream.format('cloudFiles') \
      .option('cloudFiles.format', 'csv') \
      .schema(spark.conf.get(f'{table}.schema')) \
      .option("inferSchema", False) \
      .option("delimiter", spark.conf.get(f'{table}.sep')) \
      .option("header", spark.conf.get(f'{table}.header')) \
      .option("pathGlobfilter", spark.conf.get(f'{table}.filename')) \
      .load(f"{spark.conf.get('files_directory')}/sf={spark.conf.get('scale_factor')}/{src_dir}")

def generate_tables(table_nm):
  @dlt.table(name=table_nm)
  def create_table(): 
    if table_nm in spark.conf.get('tables_with_batchid').replace(" ", "").split(","):
      return build_autoloader_stream(table_nm).selectExpr("*", "cast(substring(input_file_name() FROM (position('/Batch', input_file_name()) + 6) FOR 1) as int) batchid")
    else:
      return build_autoloader_stream(table_nm)

# COMMAND ----------

# MAGIC %md
# MAGIC # Programatically Create Tables using Metadata and looping through required tables
# MAGIC * Most tables use common signature - leverage metadata driven pipeline then to follow the pattern and simplify code

# COMMAND ----------

# DBTITLE 1,Generate All Raw Table Ingestion
for table in spark.conf.get('raw_tables').replace(" ", "").split(","):
  generate_tables(table)

# COMMAND ----------

# MAGIC %md
# MAGIC ## FinWire is the Only Fixed Length Text File to ingest so no need for programmatic loop

# COMMAND ----------

@dlt.table(partition_cols=["rectype"])
def FinWire():
  return spark.readStream.format('cloudFiles') \
    .option('cloudFiles.format', 'text') \
    .option("inferSchema", False) \
    .option("pathGlobfilter", spark.conf.get('FinWire.filename')) \
    .load(f"{spark.conf.get('files_directory')}/sf={spark.conf.get('scale_factor')}/{spark.conf.get('FinWire.path')}") \
    .withColumn('rectype', F.substring(F.col('value'), 16, 3))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Separating out DailyMarketHistorical since the historical table is very large and it is faster to just define as view and not write twice
# MAGIC * In this case, the view doesn't cause an issue with collectmetrics forcing execution out of photon and into JVM since the subsequent bounded window cannot be performed in Photon as of DBR 11.3

# COMMAND ----------

@dlt.view
def DailyMarketHistorical():
  return spark.read.csv(f"{spark.conf.get('files_directory')}/sf={spark.conf.get('scale_factor')}/Batch1/{spark.conf.get('DailyMarketHistorical.filename')}", 
                        schema=spark.conf.get('DailyMarketHistorical.schema'), 
                        sep=spark.conf.get('DailyMarketHistorical.sep'), 
                        header=spark.conf.get('DailyMarketHistorical.header'), 
                        inferSchema=False).withColumn('batchid', F.lit(1))
