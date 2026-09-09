# Fabric Spark NEE notebook — BATCH port of the fabric_ss bronze ingestion.
# NEE cannot run Structured Streaming, so bronze is a plain batch read of THIS batch's
# per-date file rather than an availableNow file stream. See PORT_NOTES.md §7.
# MUST run on the NEE-accelerated environment (tpcdi_fabric_nee, spark.native.enabled=true).
#
# Batch-scoping (§7): every bronze source is CURRENT-BATCH-ONLY (overwrite per batch)
# EXCEPT bronzedailymarket, which must ACCUMULATE — FactMarketHistory's 52-week window
# reads the full table. So dailymarket APPENDS to the setup-seeded prior year; the rest
# OVERWRITE. Writes go through INSERT [OVERWRITE|INTO] SELECT so the setup-created table's
# Liquid CLUSTER BY + TBLPROPERTIES are preserved (a saveAsTable(overwrite) would drop them).

# --- PARAMETER CELL (Fabric injects overrides here) ---------------------------
table            = "customer"          # one of: account cashtransaction customer dailymarket holdings trade watches
scale_factor     = "10"
wh_db            = ""                   # target db/schema prefix; target schema = f"{wh_db}_{scale_factor}"
batch_date       = ""                   # NEE loads exactly this batch's per-date subdir
landing_base     = "Files/augmented_incremental/_dailybatches"
# ------------------------------------------------------------------------------

tgt_db          = f"{wh_db}_{scale_factor}"
batches_dir     = f"{landing_base}/{tgt_db}"
# simulate_filedrops drops the day's single part file at {batches_dir}/{batch_date}/{Dataset}.txt.
# In batch mode we know the date, so load exactly that subdir (no checkpoint / no
# recursiveFileLookup needed — we're not scanning across all dates like the stream did).
batch_dir       = f"{batches_dir}/{batch_date}"
target_table    = f"{tgt_db}.bronze{table}"

# Per-table schemas — identical to the Databricks/fabric_ss source (engine-agnostic).
schemas = {
  "account":         "cdc_flag STRING, cdc_dsn BIGINT, accountid BIGINT, brokerid BIGINT, customerid BIGINT, accountdesc STRING, taxstatus TINYINT, status STRING, update_dt DATE",
  "cashtransaction": "cdc_flag STRING, cdc_dsn BIGINT, accountid BIGINT, ct_dts TIMESTAMP, ct_amt DOUBLE, ct_name STRING, event_dt DATE",
  "customer":        "cdc_flag STRING, cdc_dsn BIGINT, customerid BIGINT, taxid STRING, status STRING, lastname STRING, firstname STRING, middleinitial STRING, gender STRING, tier TINYINT, dob DATE, addressline1 STRING, addressline2 STRING, postalcode STRING, city STRING, stateprov STRING, country STRING, c_ctry_1 STRING, c_area_1 STRING, c_local_1 STRING, c_ext_1 STRING, c_ctry_2 STRING, c_area_2 STRING, c_local_2 STRING, c_ext_2 STRING, c_ctry_3 STRING, c_area_3 STRING, c_local_3 STRING, c_ext_3 STRING, email1 STRING, email2 STRING, lcl_tx_id STRING, nat_tx_id STRING, update_dt DATE",
  "dailymarket":     "cdc_flag STRING, cdc_dsn BIGINT, dm_date DATE, dm_s_symb STRING, dm_close DOUBLE, dm_high DOUBLE, dm_low DOUBLE, dm_vol INT",
  "holdings":        "cdc_flag STRING, cdc_dsn BIGINT, hh_h_t_id BIGINT, hh_t_id BIGINT, hh_before_qty INT, hh_after_qty INT, event_dt DATE",
  "trade":           "cdc_flag STRING, cdc_dsn BIGINT, tradeid BIGINT, t_dts TIMESTAMP, status STRING, t_tt_id STRING, cashflag TINYINT, t_s_symb STRING, quantity INT, bidprice DOUBLE, t_ca_id BIGINT, executedby STRING, tradeprice DOUBLE, fee DOUBLE, commission DOUBLE, tax DOUBLE, event_dt DATE",
  "watches":         "cdc_flag STRING, cdc_dsn BIGINT, w_c_id BIGINT, w_s_symb STRING, w_dts TIMESTAMP, w_action STRING, event_dt DATE",
}

# simulate_filedrops renames the day's single part file to `{Dataset}.txt`.
file_names = {
  "account": "Account.txt", "cashtransaction": "CashTransaction.txt",
  "customer": "Customer.txt", "dailymarket": "DailyMarket.txt",
  "holdings": "HoldingHistory.txt", "trade": "Trade.txt",
  "watches": "WatchHistory.txt",
}

schema          = schemas[table]
filename_format = file_names[table]

# --- batch ingest -------------------------------------------------------------
# NEE accelerates the vectorized CSV read on Runtime 2.0. Explicit schema (no inference).
df = (spark.read
   .format("csv")
   .schema(schema)
   .option("header", "false")
   .option("sep", "|")
   .option("pathGlobFilter", filename_format)
   .load(batch_dir))

_tmp = f"_bronze_ingest_{table}"
df.createOrReplaceTempView(_tmp)
# ALL bronze tables ACCUMULATE (append), matching the dbt variant's bronze models
# (+materialized=incremental, +incremental_strategy=append). The per-batch scoping is done
# DOWNSTREAM: each silver/gold transform filters the accumulated bronze by
# `<date_col> = batch_date` (dbt's `new_events`-style CTE) — not by overwriting bronze here.
# INSERT INTO SELECT preserves the setup-created CLUSTER BY + delta.dataSkippingNumIndexedCols=34.
spark.sql(f"INSERT INTO {target_table} SELECT * FROM {_tmp}")

notebookutils.notebook.exit(f"ingest_ok:{table}:{batch_date}")
