# Fabric Spark NEE notebook — BATCH account_updates_from_customer (mirrors dbt
# bronze/account_updates_from_customer). For each 'U' (update) customer row in THIS batch,
# derive the matching account row (joined to the current DimAccount SCD2 record) and append
# it into bronzeaccount so DimAccount picks up customer-driven account changes.
# NEE is not streaming: bronzecustomer ACCUMULATES, so scope to the batch with
# `where update_dt = batch_date and cdc_flag = 'U'` (dbt's filter). The appended rows carry
# update_dt = batch_date, so DimAccount's own batch filter picks them up.
#
# DAG ORDERING: this appends to bronzeaccount, so it must run AFTER the bronzeaccount ingest
# and BEFORE DimAccount (batch_runner encodes both edges).
# MUST run on the NEE-accelerated environment (tpcdi_fabric_nee, spark.native.enabled=true).

# --- PARAMETER CELL (Fabric injects overrides) ---
scale_factor    = "10"
wh_db           = ""
batch_date      = ""
# -------------------------------------------------

if not wh_db:      raise ValueError("wh_db is required")
if not batch_date: raise ValueError("batch_date is required")

tgt_db    = f"{wh_db}_{scale_factor}"
tgt_table = f"{tgt_db}.bronzeaccount"

# COMMAND ----------

spark.sql(f"""
  INSERT INTO {tgt_table}
  with new_events as (
    -- scope the accumulated bronze to THIS batch's customer updates
    select * from {tgt_db}.bronzecustomer
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
  JOIN {tgt_db}.DimAccount a
    ON
      c.customerid = substring(cast(a.sk_customerid as string), 9)
      and a.iscurrent
      and c.update_dt > a.effectivedate
""")

notebookutils.notebook.exit(f"account_updates_ok:{batch_date}")
