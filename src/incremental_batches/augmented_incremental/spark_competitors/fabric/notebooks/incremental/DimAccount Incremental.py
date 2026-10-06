# Fabric Spark NEE notebook — BATCH DimAccount (mirrors the dbt silver/dimaccount model).
# NEE is not streaming, so bronzeaccount ACCUMULATES and this transform scopes to the batch
# with a date filter on the source (dbt: `where update_dt = batch_date`). bronzeaccount holds
# both the ingested account CDC and the account_updates_from_customer-appended rows for this
# batch (both carry update_dt = batch_date). MUST run on the NEE env (spark.native.enabled=true).

# --- PARAMETER CELL (Fabric injects overrides) ---
scale_factor    = "10"
wh_db           = ""
batch_date      = ""
# -------------------------------------------------

if not wh_db:      raise ValueError("wh_db is required")
if not batch_date: raise ValueError("batch_date is required")

tgt_db    = f"{wh_db}_{scale_factor}"
tgt_table = f"{tgt_db}.dimaccount"

# COMMAND ----------

spark.sql(f"""
  with new_events as (
    -- scope the accumulated bronze to THIS batch (dbt models/silver/dimaccount.sql)
    select * from {tgt_db}.bronzeaccount
    where update_dt = cast('{batch_date}' as date)
  ),
  accounts as (
    -- Fabric Spark 4.1 has no QUALIFY (Databricks/Snowflake extension) — use the
    -- standard-Spark equivalent: row_number() in a subquery + WHERE rn = 1.
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
    JOIN {tgt_db}.dimcustomer dc
      ON
        dc.iscurrent
        and dc.customerid = a.customerid
  ),
  matched_accts as (
    SELECT
      s.*
    FROM all_incr_updates s
    JOIN {tgt_db}.DimAccount t
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

notebookutils.notebook.exit(f"dimaccount_ok:{batch_date}")
