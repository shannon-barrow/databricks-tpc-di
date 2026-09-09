# Fabric Spark NEE notebook — BATCH FactHoldings (mirrors the dbt gold/factholdings model).
# Append-only fact: one row per holding-history event whose trade closed in this batch.
# NEE is not streaming, so bronzeholdings ACCUMULATES — scope to the batch with
# `where event_dt = batch_date` (dbt's `new_events`); without it this INSERT INTO would
# re-append all history every batch. The sk_closedateid predicate stays in the ON clause
# (references per-row event_dt) so it matches the SDP flow and prunes dimtrade via its
# Liquid CLUSTER BY (sk_closedateid). MUST run on the NEE env (spark.native.enabled=true).

# --- PARAMETER CELL (Fabric injects overrides) ---
scale_factor    = "10"
wh_db           = ""
batch_date      = ""
# -------------------------------------------------

if not wh_db:      raise ValueError("wh_db is required")
if not batch_date: raise ValueError("batch_date is required")

tgt_db    = f"{wh_db}_{scale_factor}"
tgt_table = f"{tgt_db}.factholdings"

# COMMAND ----------

spark.sql(f"""
  INSERT INTO {tgt_table}
  with new_events as (
    -- scope the accumulated bronze to THIS batch (dbt models/gold/factholdings.sql)
    SELECT
      h.hh_h_t_id tradeid,
      h.hh_t_id currenttradeid,
      h.hh_after_qty currentholding,
      h.event_dt
    FROM {tgt_db}.bronzeholdings h
    where h.event_dt = cast('{batch_date}' as date)
  )
  SELECT
    h.tradeid,
    currenttradeid,
    t.sk_customerid,
    t.sk_accountid,
    t.sk_securityid,
    t.sk_companyid,
    t.sk_closedateid sk_dateid,
    t.sk_closetimeid sk_timeid,
    t.tradeprice currentprice,
    currentholding
  FROM new_events h
  JOIN {tgt_db}.dimtrade t
    ON t.tradeid = h.tradeid
   AND t.sk_closedateid = bigint(date_format(h.event_dt, 'yyyyMMdd'))
""")

notebookutils.notebook.exit(f"factholdings_ok:{batch_date}")
