{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'accountid',
    on_schema_change = 'ignore',
    full_refresh = false,
  )
}}

{# BQ variant of currentaccountbalances. dbt-bigquery doesn't support
   `delete+insert` (Snowflake/Postgres only). Use `merge` on accountid —
   the model SELECT produces a fresh snapshot per touched account, the
   merge UPSERTs into the target. Untouched accountids remain.

   Translations from Snowflake:
     - to_date(ts)                 -> DATE(ts)
     - ::number(15,2)              -> CAST(... AS NUMERIC) (BQ NUMERIC is 38,9 — sufficient)
     - cast(null as number(15,2))  -> CAST(NULL AS NUMERIC)
#}

with new_txns as (
  select
    DATE(ct_dts) as ct_date,
    accountid,
    ct_amt,
    true as latest_batch
  from {{ ref('bronzecashtransaction') }}
  where event_dt = DATE '{{ var("batch_date") }}'
),

prior as (
  {% if is_incremental() %}
  select ct_date, accountid, current_account_cash as ct_amt, false as latest_batch
  from {{ this }}
  {% else %}
  -- First run: no prior state to carry over.
  select CAST(NULL AS DATE) ct_date, CAST(NULL AS INT64) accountid,
         CAST(NULL AS NUMERIC) ct_amt, false as latest_batch
  from (select 1) where 1 = 0
  {% endif %}
),

unioned as (
  select * from new_txns
  union all
  select * from prior
)

select
  MAX(ct_date) as ct_date,
  accountid,
  CAST(SUM(ct_amt) AS NUMERIC) as current_account_cash,
  MAX(latest_batch)            as latest_batch
from unioned
group by accountid
