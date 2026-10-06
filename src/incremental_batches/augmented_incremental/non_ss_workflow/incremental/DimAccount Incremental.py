# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# Non-SS batch DimAccount (mirrors dbt silver/dimaccount). bronzeaccount ACCUMULATES and holds
# both the ingested account CDC and the account_updates_from_customer-appended rows for this
# batch (both carry update_dt = batch_date); scope to the batch with `new_events`.
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
table           = "dimaccount"
tgt_table       = f"{catalog}.{tgt_db}.{table}"

# COMMAND ----------

spark.sql(f"""
  with new_events as (
    -- scope the accumulated bronze to THIS batch (dbt models/silver/dimaccount.sql)
    select * from {catalog}.{tgt_db}.bronzeaccount
    where update_dt = cast('{batch_date}' as date)
  ),
  accounts as (
    SELECT * except(cdc_dsn, _rn) FROM (
      SELECT *,
        row_number() over (partition by update_dt, accountid order by cdc_flag desc) as _rn
      FROM new_events a
    ) WHERE _rn = 1
  ),
  all_incr_updates as (
    SELECT
      bigint(concat(date_format(a.update_dt, 'yyyyMMdd'), a.accountid)) sk_accountid,
      accountid,
      brokerid sk_brokerid,
      dc.sk_customerid,
      accountdesc,
      taxstatus,
      decode(a.status,
        'ACTV',	'Active',
        'CMPT','Completed',
        'CNCL','Canceled',
        'PNDG','Pending',
        'SBMT','Submitted',
        'INAC','Inactive',
        a.status) status,
      true iscurrent,
      update_dt effectivedate,
      date('9999-12-31') enddate
    FROM accounts a
    JOIN {catalog}.{tgt_db}.dimcustomer dc
      ON
        dc.iscurrent
        and dc.customerid = a.customerid
  ),
  matched_accts as (
    SELECT
      s.*
    FROM all_incr_updates s
    JOIN {catalog}.{tgt_db}.DimAccount t
      ON s.accountid = t.accountid
    WHERE t.iscurrent
  )
  MERGE INTO {tgt_table} t USING (
    SELECT
      CAST(NULL AS BIGINT) AS mergeKey,
      *
    FROM all_incr_updates
    UNION ALL
    SELECT
      accountid mergeKey,
      *
    FROM matched_accts
  ) s
  ON t.accountid = s.mergeKey AND t.iscurrent
  WHEN MATCHED AND t.iscurrent THEN UPDATE SET
    t.iscurrent = false,
    t.enddate = s.effectivedate
  WHEN NOT MATCHED THEN INSERT (sk_accountid, accountid, sk_brokerid, sk_customerid, accountdesc, TaxStatus, status, iscurrent, effectivedate, enddate)
  VALUES (sk_accountid, accountid, sk_brokerid, sk_customerid, accountdesc, TaxStatus, status, iscurrent, effectivedate, enddate)
""")
