# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# Non-SS batch FactHoldings (mirrors dbt gold/factholdings). Append-only fact: one row per
# holding-history event whose trade closed in this batch. bronzeholdings ACCUMULATES — scope
# to the batch with `new_events = bronzeholdings where event_dt = batch_date`; without it this
# INSERT INTO would re-append all history every batch. The sk_closedateid predicate stays in
# the ON clause (references per-row event_dt) so it prunes dimtrade via its CLUSTER BY
# (sk_closedateid).
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
table           = "factholdings"
tgt_table       = f"{catalog}.{tgt_db}.{table}"

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
    FROM {catalog}.{tgt_db}.bronzeholdings h
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
  JOIN {catalog}.{tgt_db}.dimtrade t
    ON t.tradeid = h.tradeid
   AND t.sk_closedateid = bigint(date_format(h.event_dt, 'yyyyMMdd'))
""")
