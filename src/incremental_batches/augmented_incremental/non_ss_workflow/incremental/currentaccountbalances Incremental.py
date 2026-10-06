# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# Non-SS batch currentaccountbalances (mirrors dbt gold/currentaccountbalances). Per-account
# running cash balance: union THIS batch's cash transactions with the existing balances, then
# re-aggregate and INSERT OVERWRITE the whole (small) table. bronzecashtransaction ACCUMULATES
# — scope to the batch with `new_txns = bronzecashtransaction where event_dt = batch_date`.
# latest_batch flags this batch's rows so FactCashBalances knows which accounts were touched.
# No manual autoBroadcastJoinThreshold bump (a 250MB bump drove an 8.2 GiB broadcast at SF=20000).
sf_ls = ["10", "100", "1000", "5000", "10000", "20000"]
dbutils.widgets.dropdown("scale_factor", sf_ls[0], sf_ls)
dbutils.widgets.text("tpcdi_directory", "/Volumes/tpcdi/tpcdi_raw_data/tpcdi_volume/")
dbutils.widgets.text("catalog", "tpcdi")
dbutils.widgets.text("wh_db", "")
dbutils.widgets.text("batch_date", "")

catalog         = dbutils.widgets.get("catalog")
scale_factor    = dbutils.widgets.get("scale_factor")
tpcdi_directory = dbutils.widgets.get("tpcdi_directory")
wh_db           = dbutils.widgets.get("wh_db")
batch_date      = dbutils.widgets.get("batch_date")
tgt_db          = f"{wh_db}_{scale_factor}"
table           = "currentaccountbalances"
tgt_table       = f"{catalog}.{tgt_db}.{table}"

# COMMAND ----------

spark.sql(f"""
  INSERT OVERWRITE {tgt_table}
  with new_txns as (
    -- scope the accumulated bronze to THIS batch (dbt models/gold/currentaccountbalances.sql)
    SELECT
      to_date(ct_dts) ct_date,
      accountid,
      ct_amt,
      True latest_batch
    FROM {catalog}.{tgt_db}.bronzecashtransaction
    where event_dt = cast('{batch_date}' as date)
  ),
  c as (
    SELECT ct_date, accountid, ct_amt, latest_batch FROM new_txns
    UNION ALL
    SELECT
      ct_date,
      accountid,
      current_account_cash,
      False latest_batch
    FROM {tgt_table}
  )
  SELECT
    max(ct_date) ct_date,
    accountid,
    cast(sum(ct_amt) as DECIMAL(15,2)) current_account_cash,
    max(latest_batch) latest_batch
  FROM c
  GROUP BY ALL
""")
