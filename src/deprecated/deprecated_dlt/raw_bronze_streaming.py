# Databricks notebook source
import dlt

# COMMAND ----------

# MAGIC %md
# MAGIC # Import various schema variables from the DLT Job Spark Config

# COMMAND ----------

from pyspark.sql.functions import *

files_directory = spark.conf.get("files_directory")
scale_factor = spark.conf.get("scale_factor")
batchdateschema = spark.conf.get("batchdateschema")
dailymarketschema = spark.conf.get("dailymarketschema")
holdinghistoryschema = spark.conf.get("holdinghistoryschema")
prospectrawschema = spark.conf.get("prospectrawschema")
customerrawschema = spark.conf.get("customerrawschema")
accountrawschema = spark.conf.get("accountrawschema")
traderawschema = spark.conf.get("traderawschema")
tradehistoryrawschema = spark.conf.get("tradehistoryrawschema")
cashtransactionrawschema = spark.conf.get("cashtransactionrawschema")
watchhistoryrawschema = spark.conf.get("watchhistoryrawschema")
auditschema = spark.conf.get("auditschema")
hrschema = spark.conf.get("hrschema")
dimdateschema = spark.conf.get("dimdateschema")
industryschema = spark.conf.get("industryschema")
statustypeschema = spark.conf.get("statustypeschema")
taxrateschema = spark.conf.get("taxrateschema")
tradetypeschema = spark.conf.get("tradetypeschema")
dimtimeschema = spark.conf.get("dimtimeschema")
incremental_schema = "cdc_flag STRING COMMENT 'Denotes insert or update', cdc_dsn BIGINT COMMENT 'Database Sequence Number',"

# COMMAND ----------

# MAGIC %md
# MAGIC # Begin Defining Live Tables on RAW Files

# COMMAND ----------

# @dlt.table
# def Audit():
#   return spark.read.csv(f'{files_directory}/{scale_factor}/Batch*/*_audit.csv', schema=auditschema, header=True) \
#     .unionAll(spark.read.csv(f'{files_directory}/{scale_factor}/*_audit.csv', schema=auditschema, header=True))

# COMMAND ----------

@dlt.table
def HR():
  return spark.read.csv(f'{files_directory}/{scale_factor}/Batch1/HR.csv', schema=hrschema, header=False, inferSchema=False)

# COMMAND ----------

@dlt.table
def DimDate():
  return spark.read.csv(f'{files_directory}/{scale_factor}/Batch1/Date.txt', schema=dimdateschema, sep='|', header=False, inferSchema=False)

# COMMAND ----------

@dlt.table
def Industry():
  return spark.read.csv(f'{files_directory}/{scale_factor}/Batch1/Industry.txt', schema=industryschema, sep='|', header=False, inferSchema=False)

# COMMAND ----------

@dlt.table
def StatusType():
  return spark.read.csv(f'{files_directory}/{scale_factor}/Batch1/StatusType.txt', schema=statustypeschema, sep='|', header=False, inferSchema=False)

# COMMAND ----------

@dlt.table
def TaxRate():
  return spark.read.csv(f'{files_directory}/{scale_factor}/Batch1/TaxRate.txt', schema=taxrateschema, sep='|', header=False, inferSchema=False)

# COMMAND ----------

@dlt.table
def TradeType():
  return spark.read.csv(f'{files_directory}/{scale_factor}/Batch1/TradeType.txt', schema=tradetypeschema, sep='|', header=False, inferSchema=False)

# COMMAND ----------

@dlt.table
def DimTime():
  return spark.read.csv(f'{files_directory}/{scale_factor}/Batch1/Time.txt', schema=dimtimeschema, sep='|', header=False, inferSchema=False)

# COMMAND ----------

@dlt.table(partition_cols=["rectype"])
def FinWire():
  return spark.read.text(f'{files_directory}/{scale_factor}/Batch1/FINWIRE[0-9][0-9][0-9][0-9]Q[1-4]').withColumn('rectype', substring(col('value'), 16, 3))

# COMMAND ----------

@dlt.table(temporary=True)
def DailyMarketHistorical():
  return spark.read.csv(f'{files_directory}/{scale_factor}/Batch1/DailyMarket.txt', schema=dailymarketschema, sep='|', header=False, inferSchema=False).withColumn('batchid', lit(1))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exploring Perf for DIMTRADE

# COMMAND ----------

@dlt.table(temporary=True)
def TradeHistoryRaw():
  return spark.read.csv(f'{files_directory}/{scale_factor}/Batch1/TradeHistory.txt', schema=tradehistoryrawschema, sep='|', header=False, inferSchema=False)

# COMMAND ----------

@dlt.table()
def WatchHistory(temporary=True):
  return spark.read.csv(f'{files_directory}/{scale_factor}/Batch1/WatchHistory.txt', schema=watchhistoryrawschema, sep='|', header=False, inferSchema=False).withColumn('batchid', lit(1))

# COMMAND ----------

# @dlt.view
# def TradeBatch1():
#   return spark.read.csv(f'{files_directory}/{scale_factor}/Batch1/Trade.txt', schema=traderawschema, sep='|', header=False, inferSchema=False).withColumn('batchid', lit(1))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Incremental Tables that have file(s) land in each Batch - moved to streaming live table using autoloader

# COMMAND ----------

# @dlt.table
# def BatchDate():
#   return spark.read.csv(f'{files_directory}/{scale_factor}/Batch1/BatchDate.txt', schema=batchdateschema, sep='|', header=False, inferSchema=False).withColumn('batchid', lit(1)) \
#   .unionAll(spark.read.csv(f'{files_directory}/{scale_factor}/Batch2/BatchDate.txt', schema=batchdateschema, sep='|', header=False, inferSchema=False).withColumn('batchid', lit(2))) \
#   .unionAll(spark.read.csv(f'{files_directory}/{scale_factor}/Batch3/BatchDate.txt', schema=batchdateschema, sep='|', header=False, inferSchema=False).withColumn('batchid', lit(3)))

# @dlt.view
# def ProspectRaw():
#   return spark.read.csv(f'{files_directory}/{scale_factor}/Batch1/Prospect.csv', schema=prospectrawschema, header=False, inferSchema=False).withColumn('batchid', lit(1)) \
#   .unionAll(spark.read.csv(f'{files_directory}/{scale_factor}/Batch2/Prospect.csv', schema=prospectrawschema, header=False, inferSchema=False).withColumn('batchid', lit(2))) \
#   .unionAll(spark.read.csv(f'{files_directory}/{scale_factor}/Batch3/Prospect.csv', schema=prospectrawschema, header=False, inferSchema=False).withColumn('batchid', lit(3)))

# COMMAND ----------

# DBTITLE 1,SCD Type 2 Using Autoloader Revision in Incremental Notebook Now 
# @dlt.view
# def AccountRaw():
#   return spark.read.csv(f'{files_directory}/{scale_factor}/Batch[23]/Account.txt', schema=accountrawschema, sep='|', header=False, inferSchema=False) \
#     .selectExpr("* except(cdc_flag, cdc_dsn)", "cast(substring(input_file_name() FROM (position('/Batch', input_file_name()) + 6) FOR 1) as int) batchid")

# @dlt.view
# def CustomerRaw():
#   return spark.read.csv(f'{files_directory}/{scale_factor}/Batch[23]/Customer.txt', schema=customerrawschema, sep='|', header=False, inferSchema=False) \
#     .selectExpr("* except(cdc_flag, cdc_dsn)", "cast(substring(input_file_name() FROM (position('/Batch', input_file_name()) + 6) FOR 1) as int) batchid")

# @dlt.table(temporary=True)
# def DailyMarketHistorical():
#   return spark.read.csv(f'{files_directory}/{scale_factor}/Batch1/DailyMarket.txt', schema=dailymarketschema, sep='|', header=False, inferSchema=False) \
#     .unionAll(spark.read.csv(f'{files_directory}/{scale_factor}/Batch[23]/DailyMarket.txt', schema=f"{incremental_schema} {dailymarketschema}", sep='|', header=False, inferSchema=False).drop('cdc_flag', 'cdc_dsn')) \
#     .selectExpr("*", "cast(substring(input_file_name() FROM (position('/Batch', input_file_name()) + 6) FOR 1) as int) batchid")

# @dlt.view
# def TradeBatch1():
#   return spark.read.csv(f'{files_directory}/{scale_factor}/Batch1/Trade.txt', schema=traderawschema, sep='|', header=False, inferSchema=False).withColumn('batchid', lit(1))

# @dlt.view
# def TradeIncremental():
#   return spark.read.csv(f'{files_directory}/{scale_factor}/Batch[23]/Trade.txt', schema=f"{incremental_schema} {traderawschema}", sep='|', header=False, inferSchema=False) \
#     .selectExpr("* except(cdc_flag, cdc_dsn)", "cast(substring(input_file_name() FROM (position('/Batch', input_file_name()) + 6) FOR 1) as int) batchid")

# @dlt.table(temporary=True)
# def HoldingHistory():
#   return spark.read.csv(f'{files_directory}/{scale_factor}/Batch1/HoldingHistory.txt', schema=holdinghistoryschema, sep='|', header=False, inferSchema=False).unionAll( \
#     spark.read.csv(f'{files_directory}/{scale_factor}/Batch[23]/HoldingHistory.txt', schema=f"{incremental_schema} {holdinghistoryschema}", sep='|', header=False, inferSchema=False) \
#     .drop('cdc_flag', 'cdc_dsn')).selectExpr("*", "cast(substring(input_file_name() FROM (position('/Batch', input_file_name()) + 6) FOR 1) as int) batchid")

# @dlt.view
# def CashTransaction():
#   return spark.read.csv(f'{files_directory}/{scale_factor}/Batch1/CashTransaction.txt', schema=cashtransactionrawschema, sep='|', header=False, inferSchema=False).unionAll( \
#     spark.read.csv(f'{files_directory}/{scale_factor}/Batch2/CashTransaction.txt', schema=f"{incremental_schema} {cashtransactionrawschema}", sep='|', header=False, inferSchema=False) \
#     .drop('cdc_flag', 'cdc_dsn')).selectExpr("*", "cast(substring(input_file_name() FROM (position('/Batch', input_file_name()) + 6) FOR 1) as int) batchid")

# @dlt.view
# def CashTransaction():
#   return spark.read.csv(f'{files_directory}/{scale_factor}/Batch1/CashTransaction.txt', schema=cashtransactionrawschema, sep='|', header=False, inferSchema=False).unionAll( \
#     spark.read.csv(f'{files_directory}/{scale_factor}/Batch2/CashTransaction.txt', schema=f"{incremental_schema} {cashtransactionrawschema}", sep='|', header=False, inferSchema=False) \
#     .drop('cdc_flag', 'cdc_dsn')).selectExpr("*", "cast(substring(input_file_name() FROM (position('/Batch', input_file_name()) + 6) FOR 1) as int) batchid")

# @dlt.view
# def WatchHistory():
#   return spark.read.csv(f'{files_directory}/{scale_factor}/Batch1/WatchHistory.txt', schema=watchhistoryrawschema, sep='|', header=False, inferSchema=False).unionAll( \
#     spark.read.csv(f'{files_directory}/{scale_factor}/Batch2/WatchHistory.txt', schema=f"{incremental_schema} {watchhistoryrawschema}", sep='|', header=False, inferSchema=False) \
#     .drop('cdc_flag', 'cdc_dsn')).selectExpr("*", "cast(substring(input_file_name() FROM (position('/Batch', input_file_name()) + 6) FOR 1) as int) batchid")
