# Databricks notebook source
import dlt

# spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 262144000)
# spark.conf.set("spark.databricks.adaptive.autoBroadcastJoinThreshold", 262144000)

# COMMAND ----------

tbl_ls          = ['account', 'cashtransaction', 'customer', 'dailymarket', 'holdings', 'trade', 'watches']
tpcdi_directory = spark.conf.get('tpcdi_directory') # Place this in the DLT JSON Config
scale_factor    = spark.conf.get('scale_factor')    # Place this in the DLT JSON Config
wh_db           = spark.conf.get('wh_db')           # Place this in the DLT JSON Config
batches_dir     = f"{tpcdi_directory}augmented_incremental/_dailybatches/{wh_db}_{scale_factor}"

schemas = {
  "account": "cdc_flag STRING, cdc_dsn BIGINT, accountid BIGINT, brokerid BIGINT, customerid BIGINT, accountdesc STRING, taxstatus TINYINT, status STRING, update_dt DATE",
  "cashtransaction": "cdc_flag STRING, cdc_dsn BIGINT, accountid BIGINT, ct_dts TIMESTAMP, ct_amt DOUBLE, ct_name STRING, event_dt DATE",
  "customer": "cdc_flag STRING, cdc_dsn BIGINT, customerid BIGINT, taxid STRING, status STRING, lastname STRING, firstname STRING, middleinitial STRING, gender STRING, tier TINYINT, dob DATE, addressline1 STRING, addressline2 STRING, postalcode STRING, city STRING, stateprov STRING, country STRING, c_ctry_1 STRING, c_area_1 STRING, c_local_1 STRING, c_ext_1 STRING, c_ctry_2 STRING, c_area_2 STRING, c_local_2 STRING, c_ext_2 STRING, c_ctry_3 STRING, c_area_3 STRING, c_local_3 STRING, c_ext_3 STRING, email1 STRING, email2 STRING, lcl_tx_id STRING, nat_tx_id STRING, update_dt DATE",
  "dailymarket": "cdc_flag STRING, cdc_dsn BIGINT, dm_date DATE, dm_s_symb STRING, dm_close DOUBLE, dm_high DOUBLE, dm_low DOUBLE, dm_vol INT",
  "holdings": "cdc_flag STRING, cdc_dsn BIGINT, hh_h_t_id BIGINT, hh_t_id BIGINT, hh_before_qty INT, hh_after_qty INT, event_dt DATE",
  "trade": "cdc_flag STRING, cdc_dsn BIGINT, tradeid BIGINT, t_dts TIMESTAMP, status STRING, t_tt_id STRING, cashflag TINYINT, t_s_symb STRING, quantity INT, bidprice DOUBLE, t_ca_id BIGINT, executedby STRING, tradeprice DOUBLE, fee DOUBLE, commission DOUBLE, tax DOUBLE, event_dt DATE",
  "watches": "cdc_flag STRING, cdc_dsn BIGINT, w_c_id BIGINT, w_s_symb STRING, w_dts TIMESTAMP, w_action STRING, event_dt DATE"
}

# simulate_filedrops moves per-day part files into the watch dir with
# renamed targets `{dataset}_{i}.txt` (lowercase dataset stem). Glob matches
# that pattern only.
file_names = {
  "account": "account_[0-9]*.txt",
  "cashtransaction": "cashtransaction_[0-9]*.txt",
  "customer": "customer_[0-9]*.txt",
  "dailymarket": "dailymarket_[0-9]*.txt",
  "holdings": "holdinghistory_[0-9]*.txt",
  "trade": "trade_[0-9]*.txt",
  "watches": "watchhistory_[0-9]*.txt"
}

partitions = {
  "account": ["update_dt"],
  "cashtransaction": ["event_dt"],
  "customer": ["update_dt"],
  "dailymarket": ["dm_date"],
  "holdings": ["event_dt"],
  "trade": ["event_dt"],
  "watches": ["event_dt"]
}

# COMMAND ----------

def build_autoloader_stream(tbl):
  return (
    spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "csv")
      .option("header", "false")
      .option("sep", "|")
      .schema(schemas.get(tbl))
      .option("pathGlobfilter", file_names.get(tbl))
      #.option("cloudFiles.maxFilesPerTrigger", 1)
      .load(batches_dir)
  )

def generate_tables(tbl):
  tbl_name = (f"bronze{tbl}")
  @dlt.table(
    name=tbl_name,
    partition_cols=partitions.get(tbl)
  )
  def create_table(): 
    return build_autoloader_stream(tbl)

# COMMAND ----------

for table in tbl_ls:
  generate_tables(table)

# COMMAND ----------

@dlt.append_flow(
  target = "bronzecashtransaction",
  once = True,
  name = 'bronzecashtransaction_backfill')
def backfill():
  return spark.sql(f"""
    select *
    from main.tpcdi_raw_data.rawcashtransaction{scale_factor}
    where event_dt < '2015-07-06'
  """)