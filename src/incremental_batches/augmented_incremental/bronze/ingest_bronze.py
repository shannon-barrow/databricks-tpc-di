# Databricks notebook source
tbl_ls = ['account', 'cashtransaction', 'customer', 'dailymarket', 'holdings', 'trade', 'watches']
sf_ls = ["10", "100", "1000", "5000", "10000", "20000"]

dbutils.widgets.dropdown("Table", tbl_ls[0], tbl_ls)
dbutils.widgets.dropdown("scale_factor", sf_ls[0], sf_ls)
dbutils.widgets.text("tpcdi_directory", "/Volumes/tpcdi/tpcdi_raw_data/tpcdi_volume/")
dbutils.widgets.text("catalog", "tpcdi")
dbutils.widgets.text("wh_db", "")

table           = dbutils.widgets.get("Table")
catalog         = dbutils.widgets.get("catalog")
scale_factor    = dbutils.widgets.get("scale_factor")
tpcdi_directory = dbutils.widgets.get("tpcdi_directory")
wh_db           = dbutils.widgets.get("wh_db")
tgt_db          = f"{wh_db}_{scale_factor}"
batches_dir     = f"{tpcdi_directory}augmented_incremental/_dailybatches/{tgt_db}"
checkpoint_dir  = f"{tpcdi_directory}augmented_incremental/_checkpoints/{tgt_db}/bronze{table}"
tablename       = f"{catalog}.{tgt_db}.bronze{table}"

schemas = {
  "account": "cdc_flag STRING, cdc_dsn BIGINT, accountid BIGINT, brokerid BIGINT, customerid BIGINT, accountdesc STRING, taxstatus TINYINT, status STRING, update_dt DATE",
  "cashtransaction": "cdc_flag STRING, cdc_dsn BIGINT, accountid BIGINT, ct_dts TIMESTAMP, ct_amt DOUBLE, ct_name STRING, event_dt DATE",
  "customer": "cdc_flag STRING, cdc_dsn BIGINT, customerid BIGINT, taxid STRING, status STRING, lastname STRING, firstname STRING, middleinitial STRING, gender STRING, tier TINYINT, dob DATE, addressline1 STRING, addressline2 STRING, postalcode STRING, city STRING, stateprov STRING, country STRING, c_ctry_1 STRING, c_area_1 STRING, c_local_1 STRING, c_ext_1 STRING, c_ctry_2 STRING, c_area_2 STRING, c_local_2 STRING, c_ext_2 STRING, c_ctry_3 STRING, c_area_3 STRING, c_local_3 STRING, c_ext_3 STRING, email1 STRING, email2 STRING, lcl_tx_id STRING, nat_tx_id STRING, update_dt DATE",
  "dailymarket": "cdc_flag STRING, cdc_dsn BIGINT, dm_date DATE, dm_s_symb STRING, dm_close DOUBLE, dm_high DOUBLE, dm_low DOUBLE, dm_vol INT",
  "holdings": "cdc_flag STRING, cdc_dsn BIGINT, hh_h_t_id BIGINT, hh_t_id BIGINT, hh_before_qty INT, hh_after_qty INT, event_dt DATE",
  "trade": "cdc_flag STRING, cdc_dsn BIGINT, tradeid BIGINT, t_dts TIMESTAMP, status STRING, t_tt_id STRING, cashflag TINYINT, t_s_symb STRING, quantity INT, bidprice DOUBLE, t_ca_id BIGINT, executedby STRING, tradeprice DOUBLE, fee DOUBLE, commission DOUBLE, tax DOUBLE, event_dt DATE",
  "watches": "cdc_flag STRING, cdc_dsn BIGINT, w_c_id BIGINT, w_s_symb STRING, w_dts TIMESTAMP, w_action STRING, event_dt DATE"
}

# stage_files writes per-date files numbered `Base_1.txt`, `Base_2.txt`, …
# so multi-part dates (~1GB/file at SF=20000 when Spark splits a partition)
# don't collide. Glob matches the numbered form only.
file_names = {
  "account": "Account_[0-9]*.txt",
  "cashtransaction": "CashTransaction_[0-9]*.txt",
  "customer": "Customer_[0-9]*.txt",
  "dailymarket": "DailyMarket_[0-9]*.txt",
  "holdings": "HoldingHistory_[0-9]*.txt",
  "trade": "Trade_[0-9]*.txt",
  "watches": "WatchHistory_[0-9]*.txt"
}

schema = schemas.get(table)
filename_format = file_names.get(table)

# COMMAND ----------

# ONLY Excecute this code if you need to restart the stream over
# dbutils.fs.rm(f"{checkpoint_dir}", recurse=True)
# dbutils.fs.mkdirs(f"{checkpoint_dir}")

# COMMAND ----------

spark.readStream.format("cloudFiles") \
  .option("cloudFiles.format", "csv") \
  .option("header", "false") \
  .option("sep", "|") \
  .schema(schema) \
  .option("pathGlobfilter", filename_format) \
  .option("cloudFiles.maxFilesPerTrigger", 1) \
  .load(batches_dir) \
  .writeStream \
  .option("mergeSchema", "false") \
  .option("checkpointLocation", checkpoint_dir) \
  .trigger(availableNow=True) \
  .toTable(tablename)