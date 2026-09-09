{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'accountid',
    on_schema_change = 'ignore',
    full_refresh = false,
  )
}}

{# Redshift -> Fabric T-SQL — running cumulative cash per account. The SELECT
   emits a fresh snapshot per touched account (today's deltas + prior carry-
   over); merge UPSERTs on accountid, untouched accounts remain.
     - NUMERIC(p,s)      -> DECIMAL(p,s)
     - boolean true/false -> CAST(1 AS BIT) / CAST(0 AS BIT)
     - BOOL_OR(x)        -> CAST(MAX(CASE WHEN x = 1 THEN 1 ELSE 0 END) AS BIT)
#}

with new_txns as (
  select
    CAST(ct_dts AS DATE) as ct_date,
    accountid,
    ct_amt,
    CAST(1 AS BIT) as latest_batch
  from {{ ref('bronzecashtransaction') }}
  where event_dt = CAST('{{ var("batch_date") }}' AS DATE)
),

prior as (
  {% if is_incremental() %}
  select ct_date, accountid, current_account_cash as ct_amt, CAST(0 AS BIT) as latest_batch
  from {{ this }}
  {% else %}
  -- First run: no prior state to carry over.
  select CAST(NULL AS DATE) ct_date, CAST(NULL AS BIGINT) accountid,
         CAST(NULL AS DECIMAL(38, 9)) ct_amt, CAST(0 AS BIT) as latest_batch
  where 1 = 0
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
  CAST(SUM(ct_amt) AS DECIMAL(38, 9)) as current_account_cash,
  CAST(MAX(CASE WHEN latest_batch = 1 THEN 1 ELSE 0 END) AS BIT) as latest_batch
from unioned
group by accountid
