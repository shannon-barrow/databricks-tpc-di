# Fabric Spark NEE notebook — BATCH FactWatches (mirrors the dbt silver/factwatches model).
# NEE is not streaming, so bronzewatches ACCUMULATES and this transform scopes to the batch
# with a date filter on the source (dbt: `where event_dt = batch_date`). Within the batch we
# derive place/remove dates; the MERGE marks a watch removed when its CNCL lands.
# MUST run on the NEE env (spark.native.enabled=true).

# --- PARAMETER CELL (Fabric injects overrides) ---
scale_factor    = "10"
wh_db           = ""
batch_date      = ""
# -------------------------------------------------

if not wh_db:      raise ValueError("wh_db is required")
if not batch_date: raise ValueError("batch_date is required")

tgt_db    = f"{wh_db}_{scale_factor}"
tgt_table = f"{tgt_db}.factwatches"

# COMMAND ----------

spark.sql(f"""
  with new_events as (
    -- scope the accumulated bronze to THIS batch (dbt models/silver/factwatches.sql)
    select * from {tgt_db}.bronzewatches
    where event_dt = cast('{batch_date}' as date)
  ),
  w as (
    SELECT
      w_c_id customerid,
      w_s_symb symbol,
      date(min(if(w_action != 'CNCL', w_dts, cast(null as timestamp)))) dateplaced,
      date(max(if(w_action = 'CNCL', w_dts, cast(null as timestamp)))) dateremoved
    FROM new_events
    group by all
  ),
  stage as (
    SELECT
      c.sk_customerid sk_customerid,
      s.sk_securityid sk_securityid,
      w.customerid,
      w.symbol,
      bigint(date_format(w.dateplaced, 'yyyyMMdd')) sk_dateid_dateplaced,
      bigint(date_format(w.dateremoved, 'yyyyMMdd')) sk_dateid_dateremoved,
      nvl2(w.dateremoved, True, False) removed
    from w
    JOIN {tgt_db}.dimsecurity s
      ON
        s.symbol = w.symbol
        AND s.iscurrent
    JOIN {tgt_db}.dimcustomer c
      ON
        w.customerid = c.customerid
        AND c.iscurrent
  )
  MERGE INTO {tgt_table} t
  USING stage s
  ON
    !t.removed
    AND t.symbol = s.symbol
    AND t.customerid = s.customerid
    AND t.sk_dateid_dateremoved IS NULL
  WHEN MATCHED THEN UPDATE SET
    t.sk_dateid_dateremoved = s.sk_dateid_dateremoved,
    t.removed = True
  WHEN NOT MATCHED THEN
  INSERT (sk_customerid, sk_securityid, customerid, symbol, sk_dateid_dateplaced, sk_dateid_dateremoved, removed)
  VALUES (sk_customerid, sk_securityid, customerid, symbol, sk_dateid_dateplaced, sk_dateid_dateremoved, removed)
""")

notebookutils.notebook.exit(f"factwatches_ok:{batch_date}")
