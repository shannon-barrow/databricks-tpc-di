# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# Non-SS batch account_updates_from_customer (mirrors dbt bronze/account_updates_from_customer).
# For each 'U' (update) customer row in THIS batch, derive the matching account row (joined to
# the current DimAccount SCD2 record) and append it into bronzeaccount so DimAccount picks up
# customer-driven account changes. bronzecustomer ACCUMULATES, so scope to the batch with
# `where update_dt = batch_date and cdc_flag = 'U'`. Appended rows carry update_dt = batch_date,
# so DimAccount's own batch filter picks them up.
#
# DAG ORDERING: appends to bronzeaccount, so it must run AFTER the bronzeaccount ingest and
# BEFORE DimAccount.
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
tgt_table       = f"{catalog}.{tgt_db}.bronzeaccount"

# COMMAND ----------

spark.sql(f"""
  INSERT INTO {tgt_table}
  with new_events as (
    -- scope the accumulated bronze to THIS batch's customer updates
    select * from {catalog}.{tgt_db}.bronzecustomer
    where update_dt = cast('{batch_date}' as date)
      and cdc_flag = 'U'
  )
  SELECT
    "cust_update" cdc_flag,
    -1 cdc_dsn,
    a.accountid,
    a.sk_brokerid brokerid,
    c.customerid,
    a.accountdesc,
    a.taxstatus,
    a.status,
    c.update_dt
  FROM new_events c
  JOIN {catalog}.{tgt_db}.DimAccount a
    ON
      c.customerid = substring(cast(a.sk_customerid as string), 9)
      and a.iscurrent
      and c.update_dt > a.effectivedate
""")
