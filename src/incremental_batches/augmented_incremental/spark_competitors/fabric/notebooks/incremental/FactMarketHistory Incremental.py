# Fabric Spark NEE notebook — BATCH FactMarketHistory (mirrors the dbt gold/factmarkethistory model).
# NEE is not streaming. bronzedailymarket ACCUMULATES (setup seeds the prior year; each batch
# appends the day). Two source scopes, both keyed off batch_date (dbt's `new_dm` + `sym_min_max`):
#   new_dm      = bronzedailymarket WHERE dm_date  = batch_date            (today's output rows)
#   sym_min_max = bronzedailymarket WHERE dm_date  > batch_date - 365 days (rolling 52-week hi/lo)
# Fabric Spark 4.1 has no `INSERT INTO … REPLACE USING`, so we build the batch's rows then
# DELETE matching sk_dateid + INSERT (portable Delta 4.2 selective overwrite).
# MUST run on the NEE env (spark.native.enabled=true).

# --- PARAMETER CELL (Fabric injects overrides) ---
scale_factor    = "10"
wh_db           = ""
batch_date      = ""       # this batch's date; new_dm = it, the 52-week window ends at it
# -------------------------------------------------

if not wh_db:      raise ValueError("wh_db is required")
if not batch_date: raise ValueError("batch_date is required")

tgt_db    = f"{wh_db}_{scale_factor}"
src_table = f"{tgt_db}.bronzedailymarket"
tgt_table = f"{tgt_db}.factmarkethistory"

# COMMAND ----------

_src = spark.sql(f"""
  with new_dm as (
    -- today's bronze rows only (dbt models/gold/factmarkethistory.sql `new_dm`)
    select * from {src_table}
    where dm_date = cast('{batch_date}' as date)
  ),
  sym_min_max as (
    -- rolling 365-day window over the FULL accumulated bronze (recomputed each batch)
    SELECT
      dm_s_symb,
      min_by(struct(dm_low, dm_date), dm_low) fiftytwoweeklow,
      max_by(struct(dm_high, dm_date), dm_high) fiftytwoweekhigh
    FROM {src_table}
    where dm_date > date_sub(cast('{batch_date}' as date), 365)
    group by all
  )
  -- No BROADCAST(f) hint: companyyeareps is ~950M rows at SF=20000 and Fabric Spark 4.1
  -- honors a broadcast hint literally (Databricks/Photon demotes it), blowing
  -- spark.driver.maxResultSize. Let AQE pick the join strategy.
  SELECT
    s.sk_securityid,
    s.sk_companyid,
    bigint(date_format(dm.dm_date, 'yyyyMMdd')) sk_dateid,
    try_divide(dm.dm_close, f.prev_year_basic_eps) AS peratio,
    (try_divide(s.dividend, dm.dm_close)) / 100 yield,
    agg.fiftytwoweekhigh.dm_high fiftytwoweekhigh,
    bigint(date_format(agg.fiftytwoweekhigh.dm_date, 'yyyyMMdd')) sk_fiftytwoweekhighdate,
    agg.fiftytwoweeklow.dm_low fiftytwoweeklow,
    bigint(date_format(agg.fiftytwoweeklow.dm_date, 'yyyyMMdd')) sk_fiftytwoweeklowdate,
    dm.dm_close closeprice,
    dm.dm_high dayhigh,
    dm.dm_low daylow,
    dm.dm_vol volume
  FROM new_dm dm
  JOIN sym_min_max agg
    ON
      dm.dm_s_symb = agg.dm_s_symb
  JOIN {tgt_db}.dimsecurity s
    ON
      s.symbol = dm.dm_s_symb
      AND dm.dm_date >= s.effectivedate
      AND dm.dm_date < s.enddate
  LEFT JOIN {tgt_db}.companyyeareps f
    ON
      f.sk_companyid = s.sk_companyid
      AND quarter(dm.dm_date) = quarter(f.qtr_start_date)
      AND year(dm.dm_date) = year(f.qtr_start_date)
""")
_src.createOrReplaceTempView("_fmh_src")

# Delta on Fabric rejects subqueries in DELETE — collect this batch's distinct sk_dateids
# and DELETE with a literal IN-list (tiny per batch), then INSERT the built rows.
_dateids = [int(r[0]) for r in _src.select("sk_dateid").distinct().collect() if r[0] is not None]
if _dateids:
    spark.sql(f"DELETE FROM {tgt_table} WHERE sk_dateid IN ({','.join(map(str, _dateids))})")
spark.sql(f"INSERT INTO {tgt_table} SELECT * FROM _fmh_src")

notebookutils.notebook.exit(f"factmarkethistory_ok:{batch_date}")
