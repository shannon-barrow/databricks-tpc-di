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

@dlt.table
def Audit():
  return spark.read.csv(f'{files_directory}/{scale_factor}/Batch*/*_audit.csv', schema=auditschema, header=True) \
    .unionAll(spark.read.csv(f'{files_directory}/{scale_factor}/*_audit.csv', schema=auditschema, header=True))

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

@dlt.table(
  partition_cols=["rectype"],
  temporary=True)
def FinWire():
  return spark.read.text(f'{files_directory}/{scale_factor}/Batch1/FINWIRE[0-9][0-9][0-9][0-9]Q[1-4]').withColumn('rectype', substring(col('value'), 16, 3))

# COMMAND ----------

# MAGIC %md
# MAGIC ## The following cells were modified to enable the property: "spark.databricks.photon.allDataSources.enabled"
# MAGIC * At time of writing code, there is a bug in this property when reading files that causes the function input_file_name() to break, thus causing null values when trying to dynamically parse the batchid.
# MAGIC * To account for this property being used (which speeds up non-parquet reads using Photon signicantly) I have broken each batchid out into separate reads, manually set batchid, then unioned them together

# COMMAND ----------

@dlt.table
def BatchDate():
  return spark.read.csv(f'{files_directory}/{scale_factor}/Batch1/BatchDate.txt', schema=batchdateschema, sep='|', header=False, inferSchema=False).withColumn('batchid', lit(1)) \
  .unionAll(spark.read.csv(f'{files_directory}/{scale_factor}/Batch2/BatchDate.txt', schema=batchdateschema, sep='|', header=False, inferSchema=False).withColumn('batchid', lit(2))) \
  .unionAll(spark.read.csv(f'{files_directory}/{scale_factor}/Batch3/BatchDate.txt', schema=batchdateschema, sep='|', header=False, inferSchema=False).withColumn('batchid', lit(3)))
  #  .selectExpr("batchdate", "cast(substring(input_file_name() FROM (position('/Batch', input_file_name()) + 6) FOR 1) as int) batchid")

# COMMAND ----------

@dlt.view
def ProspectRaw():
  return spark.read.csv(f'{files_directory}/{scale_factor}/Batch1/Prospect.csv', schema=prospectrawschema, header=False, inferSchema=False).withColumn('batchid', lit(1)) \
  .unionAll(spark.read.csv(f'{files_directory}/{scale_factor}/Batch2/Prospect.csv', schema=prospectrawschema, header=False, inferSchema=False).withColumn('batchid', lit(2))) \
  .unionAll(spark.read.csv(f'{files_directory}/{scale_factor}/Batch3/Prospect.csv', schema=prospectrawschema, header=False, inferSchema=False).withColumn('batchid', lit(3)))
   # .selectExpr("*", "cast(substring(input_file_name() FROM (position('/Batch', input_file_name()) + 6) FOR 1) as int) batchid")

# COMMAND ----------

@dlt.view
def CustomerRaw():
  return spark.read.csv(f'{files_directory}/{scale_factor}/Batch2/Customer.txt', 
                        schema=customerrawschema, sep='|', header=False, inferSchema=False).drop('cdc_flag', 'cdc_dsn').withColumn('batchid', lit(2)) \
  .unionAll(spark.read.csv(f'{files_directory}/{scale_factor}/Batch3/Customer.txt', 
                        schema=customerrawschema, sep='|', header=False, inferSchema=False).drop('cdc_flag', 'cdc_dsn').withColumn('batchid', lit(3)))
#  .selectExpr("*", "cast(substring(input_file_name() FROM (position('/Batch', input_file_name()) + 6) FOR 1) as int) batchid")

# COMMAND ----------

@dlt.view
def TradeHistoryRaw():
  return spark.read.csv(f'{files_directory}/{scale_factor}/Batch1/TradeHistory.txt', schema=tradehistoryrawschema, sep='|', header=False, inferSchema=False)

# COMMAND ----------

@dlt.view
def TradeBatch1():
  return spark.read.csv(f'{files_directory}/{scale_factor}/Batch1/Trade.txt', schema=traderawschema, sep='|', header=False, inferSchema=False).withColumn('batchid', lit(1))
#  .selectExpr("*", "cast(substring(input_file_name() FROM (position('/Batch', input_file_name()) + 6) FOR 1) as int) batchid")

# COMMAND ----------

@dlt.view
def TradeIncremental():
  return spark.read.csv(f'{files_directory}/{scale_factor}/Batch2/Trade.txt', 
                        schema=f"{incremental_schema} {traderawschema}", 
                        sep='|', header=False, inferSchema=False).withColumn('batchid', lit(2)) \
  .unionAll(spark.read.csv(f'{files_directory}/{scale_factor}/Batch3/Trade.txt', 
                        schema=f"{incremental_schema} {traderawschema}", 
                        sep='|', header=False, inferSchema=False).withColumn('batchid', lit(3)))
 # .selectExpr("*", "cast(substring(input_file_name() FROM (position('/Batch', input_file_name()) + 6) FOR 1) as int) batchid")

# COMMAND ----------

@dlt.view
def AccountRaw():
  return spark.read.csv(f'{files_directory}/{scale_factor}/Batch2/Account.txt', 
                        schema=accountrawschema, sep='|', header=False, inferSchema=False).drop('cdc_flag', 'cdc_dsn').withColumn('batchid', lit(2)) \
  .unionAll(spark.read.csv(f'{files_directory}/{scale_factor}/Batch3/Account.txt', 
                        schema=accountrawschema, sep='|', header=False, inferSchema=False).drop('cdc_flag', 'cdc_dsn').withColumn('batchid', lit(3)))
#  .selectExpr("*", "cast(substring(input_file_name() FROM (position('/Batch', input_file_name()) + 6) FOR 1) as int) batchid")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Some tables have different schemas depending on whether it is batch 1 or incremental
# MAGIC * The following tables union batch 1 with the other batches, each reading per its own schema

# COMMAND ----------

@dlt.table(temporary=True)
def DailyMarketHistorical():
  return spark.read.csv(f'{files_directory}/{scale_factor}/Batch1/DailyMarket.txt', schema=dailymarketschema, sep='|', header=False, inferSchema=False).withColumn('batchid', lit(1)) \
  .unionAll(spark.read.csv(f'{files_directory}/{scale_factor}/Batch2/DailyMarket.txt', 
                           schema=f"{incremental_schema} {dailymarketschema}", 
                           sep='|', header=False, inferSchema=False).drop('cdc_flag', 'cdc_dsn').withColumn('batchid', lit(2))) \
  .unionAll(spark.read.csv(f'{files_directory}/{scale_factor}/Batch3/DailyMarket.txt', 
                           schema=f"{incremental_schema} {dailymarketschema}", 
                           sep='|', header=False, inferSchema=False).drop('cdc_flag', 'cdc_dsn').withColumn('batchid', lit(3)))
 # .selectExpr("*", "cast(substring(input_file_name() FROM (position('/Batch', input_file_name()) + 6) FOR 1) as int) batchid")

# COMMAND ----------

@dlt.table(temporary=True)
def HoldingHistory():
  return spark.read.csv(f'{files_directory}/{scale_factor}/Batch1/HoldingHistory.txt', schema=holdinghistoryschema, sep='|', header=False, inferSchema=False).withColumn('batchid', lit(1)) \
  .unionAll(spark.read.csv(f'{files_directory}/{scale_factor}/Batch2/HoldingHistory.txt', 
                           schema=f"{incremental_schema}  {holdinghistoryschema}", 
                           sep='|', header=False, inferSchema=False).drop('cdc_flag', 'cdc_dsn').withColumn('batchid', lit(2))) \
  .unionAll(spark.read.csv(f'{files_directory}/{scale_factor}/Batch3/HoldingHistory.txt', 
                           schema=f"{incremental_schema}  {holdinghistoryschema}", 
                           sep='|', header=False, inferSchema=False).drop('cdc_flag', 'cdc_dsn').withColumn('batchid', lit(3)))
 # .selectExpr("*", "cast(substring(input_file_name() FROM (position('/Batch', input_file_name()) + 6) FOR 1) as int) batchid")

# COMMAND ----------

@dlt.view
def CashTransaction():
  return spark.read.csv(f'{files_directory}/{scale_factor}/Batch1/CashTransaction.txt', schema=cashtransactionrawschema, sep='|', header=False, inferSchema=False).withColumn('batchid', lit(1)) \
  .unionAll(spark.read.csv(f'{files_directory}/{scale_factor}/Batch2/CashTransaction.txt', 
                           schema=f"{incremental_schema}  {cashtransactionrawschema}", 
                           sep='|', header=False, inferSchema=False).drop('cdc_flag', 'cdc_dsn').withColumn('batchid', lit(2))) \
  .unionAll(spark.read.csv(f'{files_directory}/{scale_factor}/Batch3/CashTransaction.txt', 
                           schema=f"{incremental_schema}  {cashtransactionrawschema}", 
                           sep='|', header=False, inferSchema=False).drop('cdc_flag', 'cdc_dsn').withColumn('batchid', lit(3)))
 # .selectExpr("*", "cast(substring(input_file_name() FROM (position('/Batch', input_file_name()) + 6) FOR 1) as int) batchid")

# COMMAND ----------

@dlt.view
def WatchHistory():
  return spark.read.csv(f'{files_directory}/{scale_factor}/Batch1/WatchHistory.txt', schema=watchhistoryrawschema, sep='|', header=False, inferSchema=False).withColumn('batchid', lit(1)) \
  .unionAll(spark.read.csv(f'{files_directory}/{scale_factor}/Batch2/WatchHistory.txt', 
                           schema=f"{incremental_schema}  {watchhistoryrawschema}", 
                           sep='|', header=False, inferSchema=False).drop('cdc_flag', 'cdc_dsn').withColumn('batchid', lit(2))) \
  .unionAll(spark.read.csv(f'{files_directory}/{scale_factor}/Batch3/WatchHistory.txt', 
                           schema=f"{incremental_schema}  {watchhistoryrawschema}", 
                           sep='|', header=False, inferSchema=False).drop('cdc_flag', 'cdc_dsn').withColumn('batchid', lit(3)))
 # .selectExpr("*", "cast(substring(input_file_name() FROM (position('/Batch', input_file_name()) + 6) FOR 1) as int) batchid")
