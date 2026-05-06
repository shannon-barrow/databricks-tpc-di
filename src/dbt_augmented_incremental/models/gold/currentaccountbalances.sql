{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'accountid',
    on_schema_change = 'ignore',
    file_format = 'delta',
    full_refresh = false,
  )
}}

{# Per-account cumulative cash balance. The model body unions new bronze
   transactions with the existing target rows, then aggregates per
   accountid. dbt's delete+insert (keyed on accountid) replaces every
   account row that appears in the source — i.e. every account that
   either had a new transaction or is already in the target. The
   `latest_batch` flag distinguishes accounts touched in this batch from
   those carried over unchanged; downstream factcashbalances filters
   `where latest_batch`. #}

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
         cast(null as decimal(15,2)) ct_amt, false as latest_batch
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
  cast(sum(ct_amt) as decimal(15,2)) as current_account_cash,
  max(latest_batch) as latest_batch
from unioned
group by accountid
