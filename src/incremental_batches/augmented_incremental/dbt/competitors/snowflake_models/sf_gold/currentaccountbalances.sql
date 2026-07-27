{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'accountid',
    on_schema_change = 'ignore',
    full_refresh = false,
  )
}}

{# Snowflake variant of currentaccountbalances. Per-account cumulative
   balance — SELECT result is a fresh snapshot of every accountid that's
   ever been touched (new transactions + prior balances UNIONed +
   aggregated). dbt-snowflake's `delete+insert` strategy matches the
   Databricks `insert_overwrite` semantics: DELETE rows whose accountid
   appears in the new result, then INSERT the new result rows. End state
   is the same as CREATE OR REPLACE TABLE AS.

   Translations from Databricks:
     - to_date(ts)                 -> Snowflake supports natively
     - decimal(15,2)               -> NUMBER(15,2)
     - cast(... as decimal(15,2))  -> ::number(15,2) #}

with new_txns as (
  select
    to_date(ct_dts) as ct_date,
    accountid,
    ct_amt,
    true as latest_batch
  from {{ ref('bronzecashtransaction') }}
  where event_dt = cast('{{ var("batch_date") }}' as date)
),

prior as (
  {% if is_incremental() %}
  select ct_date, accountid, current_account_cash as ct_amt, false as latest_batch
  from {{ this }}
  {% else %}
  -- First run: no prior state to carry over.
  select cast(null as date) ct_date, cast(null as bigint) accountid,
         cast(null as number(15,2)) ct_amt, false as latest_batch
  where 1 = 0
  {% endif %}
),

unioned as (
  select * from new_txns
  union all
  select * from prior
)

select
  max(ct_date) as ct_date,
  accountid,
  sum(ct_amt)::number(15,2) as current_account_cash,
  max(latest_batch)         as latest_batch
from unioned
group by accountid
