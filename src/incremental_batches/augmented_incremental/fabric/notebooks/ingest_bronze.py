# Fabric Spark notebook — bronze ingestion (structured streaming)
#
# Port of augmented_incremental/bronze/ingest_bronze.py to Microsoft Fabric.
# The ONLY substantive change vs Databricks is the ingestion source: Fabric has
# no Autoloader (`cloudFiles`), so we use Spark's native file streaming source.
# Everything else (Trigger.AvailableNow, checkpoint, one-file-per-trigger,
# writeStream.toTable) is standard Apache Spark and ports 1:1.
#
# Deployment: this .py becomes a Fabric Notebook item with a default lakehouse
# attached (so relative Files/Tables paths resolve). The first cell below is the
# Fabric PARAMETER CELL — Fabric overrides these variables at run time via the
# Job Scheduler `parameters` payload.

# --- PARAMETER CELL (Fabric injects overrides here) ---------------------------
table            = "customer"          # one of: account cashtransaction customer dailymarket holdings trade watches
scale_factor     = "10"
wh_db            = ""                   # target db/schema prefix; target schema = f"{wh_db}_{scale_factor}"
# Landing base: a OneLake Files shortcut that points at the ADLS _dailybatches dir.
# Relative "Files/..." resolves against the attached default lakehouse.
landing_base     = "Files/augmented_incremental/_dailybatches"
checkpoint_base  = "Files/augmented_incremental/_checkpoints"
# ------------------------------------------------------------------------------

tgt_db          = f"{wh_db}_{scale_factor}"
batches_dir     = f"{landing_base}/{tgt_db}"
checkpoint_dir  = f"{checkpoint_base}/{tgt_db}/bronze{table}"
# Attached lakehouse is schema-enabled; write bronze tables into the run schema.
# (If the lakehouse is not schema-enabled, drop the "{tgt_db}." prefix.)
target_table    = f"{tgt_db}.bronze{table}"

# Per-table schemas — identical to the Databricks source (engine-agnostic).
schemas = {
  "account":         "cdc_flag STRING, cdc_dsn BIGINT, accountid BIGINT, brokerid BIGINT, customerid BIGINT, accountdesc STRING, taxstatus TINYINT, status STRING, update_dt DATE",
  "cashtransaction": "cdc_flag STRING, cdc_dsn BIGINT, accountid BIGINT, ct_dts TIMESTAMP, ct_amt DOUBLE, ct_name STRING, event_dt DATE",
  "customer":        "cdc_flag STRING, cdc_dsn BIGINT, customerid BIGINT, taxid STRING, status STRING, lastname STRING, firstname STRING, middleinitial STRING, gender STRING, tier TINYINT, dob DATE, addressline1 STRING, addressline2 STRING, postalcode STRING, city STRING, stateprov STRING, country STRING, c_ctry_1 STRING, c_area_1 STRING, c_local_1 STRING, c_ext_1 STRING, c_ctry_2 STRING, c_area_2 STRING, c_local_2 STRING, c_ext_2 STRING, c_ctry_3 STRING, c_area_3 STRING, c_local_3 STRING, c_ext_3 STRING, email1 STRING, email2 STRING, lcl_tx_id STRING, nat_tx_id STRING, update_dt DATE",
  "dailymarket":     "cdc_flag STRING, cdc_dsn BIGINT, dm_date DATE, dm_s_symb STRING, dm_close DOUBLE, dm_high DOUBLE, dm_low DOUBLE, dm_vol INT",
  "holdings":        "cdc_flag STRING, cdc_dsn BIGINT, hh_h_t_id BIGINT, hh_t_id BIGINT, hh_before_qty INT, hh_after_qty INT, event_dt DATE",
  "trade":           "cdc_flag STRING, cdc_dsn BIGINT, tradeid BIGINT, t_dts TIMESTAMP, status STRING, t_tt_id STRING, cashflag TINYINT, t_s_symb STRING, quantity INT, bidprice DOUBLE, t_ca_id BIGINT, executedby STRING, tradeprice DOUBLE, fee DOUBLE, commission DOUBLE, tax DOUBLE, event_dt DATE",
  "watches":         "cdc_flag STRING, cdc_dsn BIGINT, w_c_id BIGINT, w_s_symb STRING, w_dts TIMESTAMP, w_action STRING, event_dt DATE",
}

# simulate_filedrops renames the day's single part file to `{Dataset}.txt`, so
# one file lands per date and pathGlobFilter selects exactly it.
file_names = {
  "account": "Account.txt", "cashtransaction": "CashTransaction.txt",
  "customer": "Customer.txt", "dailymarket": "DailyMarket.txt",
  "holdings": "HoldingHistory.txt", "trade": "Trade.txt",
  "watches": "WatchHistory.txt",
}

schema          = schemas[table]
filename_format = file_names[table]

# --- streaming ingest ---------------------------------------------------------
# Fabric substitution for Databricks Autoloader:
#   readStream.format("cloudFiles").option("cloudFiles.format","csv")  ->  readStream.format("csv")
#   cloudFiles.maxFilesPerTrigger  ->  maxFilesPerTrigger   (standard Spark file-source option)
#   pathGlobfilter                 ->  pathGlobFilter        (standard Spark file-source option)
# Explicit schema is required (no schema inference/evolution like Autoloader gave us).
#
# recursiveFileLookup=true is REQUIRED: simulate_filedrops drops each batch's file
# into a per-date subdir ({batches_dir}/{batch_date}/{Dataset}.txt), and unlike
# Autoloader the native Spark file streaming source does NOT descend into
# subdirectories by default — without this it silently reads 0 files (verified:
# 0 rows without it, correct rows with it). The per-date subdir gives each batch a
# unique path so the checkpoint reprocesses it; pathGlobFilter still matches the leaf.
#
# awaitTermination() is REQUIRED on Fabric: toTable() returns immediately and the
# availableNow stream runs asynchronously — a triggered Fabric notebook reaches a
# terminal state without it, so the batch_runner DAG would mark this activity done
# before the bronze table is populated and downstream dims would read empty input.
# (Databricks job tasks auto-wait for active streams; Fabric does not.)
q = (spark.readStream
   .format("csv")
   .schema(schema)
   .option("header", "false")
   .option("sep", "|")
   .option("pathGlobFilter", filename_format)
   .option("recursiveFileLookup", "true")
   .option("maxFilesPerTrigger", 1)
   .load(batches_dir)
 .writeStream
   .option("mergeSchema", "false")
   .option("checkpointLocation", checkpoint_dir)
   .trigger(availableNow=True)
   .toTable(target_table))
q.awaitTermination()
