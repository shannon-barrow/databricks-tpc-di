{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'accountid',
    on_schema_change = 'ignore',
    full_refresh = false,
  )
}}

{# Redshift variant of currentaccountbalances. Same merge contract as BQ:
   the SELECT below emits a fresh snapshot per touched account (sum of
   today's deltas + prior carry-over), and the merge UPSERTs into the
   target. Untouched accountids remain.

   Translations from BQ:
     - DATE(ts)                    -> CAST(ts AS DATE)
     - CAST(... AS NUMERIC)        -> CAST(... AS NUMERIC(38, 9))
                                       (Redshift NUMERIC default precision/scale
                                       is too small; spell out a wide one)
     - CAST(... AS INT64)          -> CAST(... AS BIGINT)
     - CAST(NULL AS DATE)          -> CAST(NULL AS DATE) (same)
     - `(select 1) where 1 = 0`    -> kept (Redshift supports inline scalar
                                       subqueries; the where 1=0 returns 0
                                       rows for the first-run empty-prior CTE)
#}

with new_txns as (
  select
    CAST(ct_dts AS DATE) as ct_date,
    accountid,
    ct_amt,
    true as latest_batch
  from {{ ref('bronzecashtransaction') }}
  where event_dt = CAST('{{ var("batch_date") }}' AS DATE)
),

prior as (
  {% if is_incremental() %}
  select ct_date, accountid, current_account_cash as ct_amt, false as latest_batch
  from {{ this }}
  {% else %}
  -- First run: no prior state to carry over.
  select CAST(NULL AS DATE) ct_date, CAST(NULL AS BIGINT) accountid,
         CAST(NULL AS NUMERIC(38, 9)) ct_amt, false as latest_batch
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
  CAST(SUM(ct_amt) AS NUMERIC(38, 9)) as current_account_cash,
  BOOL_OR(latest_batch)        as latest_batch    {# Redshift has no MAX(boolean); BOOL_OR is the analog #}
from unioned
group by accountid
