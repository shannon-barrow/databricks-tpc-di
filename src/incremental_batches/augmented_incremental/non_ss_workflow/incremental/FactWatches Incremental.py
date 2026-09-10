# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# Non-SS batch FactWatches (mirrors dbt silver/factwatches). bronzewatches ACCUMULATES; scope
# to the batch with `new_events = bronzewatches where event_dt = batch_date`. Within the batch
# we derive place/remove dates; the MERGE marks a watch removed when its CNCL lands.
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
table           = "factwatches"
tgt_table       = f"{catalog}.{tgt_db}.{table}"

# COMMAND ----------

spark.sql(f"""
  with new_events as (
    -- scope the accumulated bronze to THIS batch (dbt models/silver/factwatches.sql)
    select * from {catalog}.{tgt_db}.bronzewatches
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
    JOIN {catalog}.{tgt_db}.dimsecurity s
      ON
        s.symbol = w.symbol
        AND s.iscurrent
    JOIN {catalog}.{tgt_db}.dimcustomer c
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
