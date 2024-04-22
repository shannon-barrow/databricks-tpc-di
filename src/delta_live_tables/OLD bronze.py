# Databricks notebook source
import dlt
import json
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC # Helper Functions to Build out the Autoloader Streaming Tables. 
# MAGIC * Use the DLT Pipeline Config Parameters (in the JSON of the DLT Pipeine) to Provide Needed Properties of the table. 
# MAGIC * This makes the job metadata driven

# COMMAND ----------

config = {
  "raw_tables": [
    {
      "table": "TaxRate",
      "filename": "TaxRate.txt",
      "raw_schema": "tx_id STRING NOT NULL COMMENT 'Tax rate code', tx_name STRING NOT NULL COMMENT 'Tax rate description', tx_rate FLOAT NOT NULL COMMENT 'Tax rate'"
    },
    {
      "table": "DimTime",
      "filename": "Time.txt",
      "raw_schema": "sk_timeid BIGINT NOT NULL COMMENT 'Surrogate key for the time', timevalue STRING NOT NULL COMMENT 'The time stored appropriately for doing', hourid INT NOT NULL COMMENT 'Hour number as a number e.g. 01', hourdesc STRING NOT NULL COMMENT 'Hour number as text e.g. 01', minuteid INT NOT NULL COMMENT 'Minute as a number e.g. 23', minutedesc STRING NOT NULL COMMENT 'Minute as text e.g. 01:23', secondid INT NOT NULL COMMENT 'Second as a number e.g. 45', seconddesc STRING NOT NULL COMMENT 'Second as text e.g. 01:23:45', markethoursflag BOOLEAN COMMENT 'Indicates a time during market hours', officehoursflag BOOLEAN COMMENT 'Indicates a time during office hours'"
    },
    {
      "table": "DimDate",
      "filename": "Date.txt",
      "raw_schema": "sk_dateid BIGINT NOT NULL COMMENT 'Surrogate key for the date', datevalue DATE NOT NULL COMMENT 'The date stored appropriately for doing comparisons in the Data Warehouse', datedesc STRING NOT NULL COMMENT 'The date in full written form e.g. July 7 2004', calendaryearid INT NOT NULL COMMENT 'Year number as a number', calendaryeardesc STRING NOT NULL COMMENT 'Year number as text', calendarqtrid INT NOT NULL COMMENT 'Quarter as a number e.g. 20042', calendarqtrdesc STRING NOT NULL COMMENT 'Quarter as text e.g. 2004 Q2', calendarmonthid INT NOT NULL COMMENT 'Month as a number e.g. 20047', calendarmonthdesc STRING NOT NULL COMMENT 'Month as text e.g. 2004 July', calendarweekid INT NOT NULL COMMENT 'Week as a number e.g. 200428', calendarweekdesc STRING NOT NULL COMMENT 'Week as text e.g. 2004-W28', dayofweeknum INT NOT NULL COMMENT 'Day of week as a number e.g. 3', dayofweekdesc STRING NOT NULL COMMENT 'Day of week as text e.g. Wednesday', fiscalyearid INT NOT NULL COMMENT 'Fiscal year as a number e.g. 2005', fiscalyeardesc STRING NOT NULL COMMENT 'Fiscal year as text e.g. 2005', fiscalqtrid INT NOT NULL COMMENT 'Fiscal quarter as a number e.g. 20051', fiscalqtrdesc STRING NOT NULL COMMENT 'Fiscal quarter as text e.g. 2005 Q1', holidayflag BOOLEAN COMMENT 'Indicates holidays'"
    },
    {
      "table": "TradeType",
      "filename": "TradeType.txt",
      "raw_schema": "tt_id STRING NOT NULL COMMENT 'Trade type code', tt_name STRING NOT NULL COMMENT 'Trade type description', tt_is_sell INT NOT NULL COMMENT 'Flag indicating a sale', tt_is_mrkt INT NOT NULL COMMENT 'Flag indicating a market order'"
    },
    {
      "table": "BatchDate",
      "path": "Batch*",
      "filename": "BatchDate.txt",
      "raw_schema": "batchdate DATE NOT NULL COMMENT 'Batch date'",
      "tgt_query": "*, cast(substring(_metadata.file_path FROM (position('/Batch', _metadata.file_path) + 6) FOR 1) as int) batchid",
      "add_tgt_schema": ", batchid INT NOT NULL COMMENT 'Batch ID when this record was inserted'"
    },
    {
      "table": "StatusType",
      "raw_schema": "st_id STRING NOT NULL COMMENT 'Status code', st_name STRING NOT NULL COMMENT 'Status description'",
      "filename": "StatusType.txt"
    },
    {
      "table": "Industry",
      "filename": "Industry.txt",
      "raw_schema": "in_id STRING NOT NULL COMMENT 'Industry code', in_name STRING NOT NULL COMMENT 'Industry description', in_sc_id STRING NOT NULL COMMENT 'Sector identifier'"
    },
    {
      "table": "finwire",
      "filename": "FINWIRE[0-9][0-9][0-9][0-9]Q[1-4]",
      "raw_schema": "value STRING COMMENT 'Pre-parsed String Values of all FinWire files'",
      "part": "PARTITIONED BY (rectype)",
      "add_tgt_schema": ", rectype STRING COMMENT 'Indicates the type of table into which this record will eventually be parsed: CMP FIN or SEC'",
      "tgt_query": "*, substring(value, 16, 3) rectype"
    }
  ]
}

# COMMAND ----------

for table in config.get('raw_tables'):
  print(table.get('table'))
  print(table.get('tgt_query') or 'missing')

# COMMAND ----------

config = """{
  "raw_tables": [
    {
      "table": "TaxRate",
      "filename": "TaxRate.txt",
      "raw_schema": "tx_id STRING NOT NULL COMMENT 'Tax rate code', tx_name STRING NOT NULL COMMENT 'Tax rate description', tx_rate FLOAT NOT NULL COMMENT 'Tax rate'"
    },
    {
      "table": "DimTime",
      "filename": "Time.txt",
      "raw_schema": "sk_timeid BIGINT NOT NULL COMMENT 'Surrogate key for the time', timevalue STRING NOT NULL COMMENT 'The time stored appropriately for doing', hourid INT NOT NULL COMMENT 'Hour number as a number e.g. 01', hourdesc STRING NOT NULL COMMENT 'Hour number as text e.g. 01', minuteid INT NOT NULL COMMENT 'Minute as a number e.g. 23', minutedesc STRING NOT NULL COMMENT 'Minute as text e.g. 01:23', secondid INT NOT NULL COMMENT 'Second as a number e.g. 45', seconddesc STRING NOT NULL COMMENT 'Second as text e.g. 01:23:45', markethoursflag BOOLEAN COMMENT 'Indicates a time during market hours', officehoursflag BOOLEAN COMMENT 'Indicates a time during office hours'"
    },
    {
      "table": "DimDate",
      "filename": "Date.txt",
      "raw_schema": "sk_dateid BIGINT NOT NULL COMMENT 'Surrogate key for the date', datevalue DATE NOT NULL COMMENT 'The date stored appropriately for doing comparisons in the Data Warehouse', datedesc STRING NOT NULL COMMENT 'The date in full written form e.g. July 7 2004', calendaryearid INT NOT NULL COMMENT 'Year number as a number', calendaryeardesc STRING NOT NULL COMMENT 'Year number as text', calendarqtrid INT NOT NULL COMMENT 'Quarter as a number e.g. 20042', calendarqtrdesc STRING NOT NULL COMMENT 'Quarter as text e.g. 2004 Q2', calendarmonthid INT NOT NULL COMMENT 'Month as a number e.g. 20047', calendarmonthdesc STRING NOT NULL COMMENT 'Month as text e.g. 2004 July', calendarweekid INT NOT NULL COMMENT 'Week as a number e.g. 200428', calendarweekdesc STRING NOT NULL COMMENT 'Week as text e.g. 2004-W28', dayofweeknum INT NOT NULL COMMENT 'Day of week as a number e.g. 3', dayofweekdesc STRING NOT NULL COMMENT 'Day of week as text e.g. Wednesday', fiscalyearid INT NOT NULL COMMENT 'Fiscal year as a number e.g. 2005', fiscalyeardesc STRING NOT NULL COMMENT 'Fiscal year as text e.g. 2005', fiscalqtrid INT NOT NULL COMMENT 'Fiscal quarter as a number e.g. 20051', fiscalqtrdesc STRING NOT NULL COMMENT 'Fiscal quarter as text e.g. 2005 Q1', holidayflag BOOLEAN COMMENT 'Indicates holidays'"
    },
    {
      "table": "TradeType",
      "filename": "TradeType.txt",
      "raw_schema": "tt_id STRING NOT NULL COMMENT 'Trade type code', tt_name STRING NOT NULL COMMENT 'Trade type description', tt_is_sell INT NOT NULL COMMENT 'Flag indicating a sale', tt_is_mrkt INT NOT NULL COMMENT 'Flag indicating a market order'"
    },
    {
      "table": "BatchDate",
      "path": "Batch*",
      "filename": "BatchDate.txt",
      "raw_schema": "batchdate DATE NOT NULL COMMENT 'Batch date'",
      "tgt_query": "*, cast(substring(_metadata.file_path FROM (position('/Batch', _metadata.file_path) + 6) FOR 1) as int) batchid",
      "add_tgt_schema": ", batchid INT NOT NULL COMMENT 'Batch ID when this record was inserted'"
    },
    {
      "table": "StatusType",
      "raw_schema": "st_id STRING NOT NULL COMMENT 'Status code', st_name STRING NOT NULL COMMENT 'Status description'",
      "filename": "StatusType.txt"
    },
    {
      "table": "Industry",
      "filename": "Industry.txt",
      "raw_schema": "in_id STRING NOT NULL COMMENT 'Industry code', in_name STRING NOT NULL COMMENT 'Industry description', in_sc_id STRING NOT NULL COMMENT 'Sector identifier'"
    },
    {
      "table": "finwire",
      "filename": "FINWIRE[0-9][0-9][0-9][0-9]Q[1-4]",
      "raw_schema": "value STRING COMMENT 'Pre-parsed String Values of all FinWire files'",
      "part": "PARTITIONED BY (rectype)",
      "add_tgt_schema": ", rectype STRING COMMENT 'Indicates the type of table into which this record will eventually be parsed: CMP FIN or SEC'",
      "tgt_query": "*, substring(value, 16, 3) rectype"
    }
  ]
}"""

# COMMAND ----------

json.loads(config)

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
      return build_autoloader_stream(table_nm).selectExpr("*", "cast(substring(_metadata.file_path FROM (position('/Batch', _metadata.file_path) + 6) FOR 1) as int) batchid")
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
