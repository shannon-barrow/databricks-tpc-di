# Fabric Spark NEE notebook — BATCH currentaccountbalances (mirrors dbt gold/currentaccountbalances).
# Per-account running cash balance: union THIS batch's cash transactions with the existing
# balances, then re-aggregate and INSERT OVERWRITE the whole (small) table. NEE is not
# streaming, so bronzecashtransaction ACCUMULATES — scope to the batch with
# `where event_dt = batch_date` (dbt's `new_txns` filter). latest_batch flags this batch's
# rows so FactCashBalances knows which accounts were touched.
#
# NOTE: no manual autoBroadcastJoinThreshold bump (a 250MB bump drove an 8.2 GiB broadcast
# at SF=20000). Use the platform default.
# MUST run on the NEE env (spark.native.enabled=true).

# --- PARAMETER CELL (Fabric injects overrides) ---
scale_factor    = "10"
wh_db           = ""
batch_date      = ""
# -------------------------------------------------

if not wh_db:      raise ValueError("wh_db is required")
if not batch_date: raise ValueError("batch_date is required")

tgt_db    = f"{wh_db}_{scale_factor}"
tgt_table = f"{tgt_db}.currentaccountbalances"

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
    FROM {tgt_db}.bronzecashtransaction
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

notebookutils.notebook.exit(f"currentaccountbalances_ok:{batch_date}")
