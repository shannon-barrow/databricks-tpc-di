{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'insert_overwrite',
    on_schema_change = 'ignore',
    file_format = 'delta',
    full_refresh = false,
  )
}}

{# Per-account cumulative cash balance. Mirrors the Classic build's
   INSERT OVERWRITE pattern (incremental/currentaccountbalances Incremental.py):
   union new bronze transactions with the existing target rows, aggregate
   per accountid taking max(ct_date) + sum(ct_amt) + max(latest_batch),
   then INSERT OVERWRITE the entire table.

   No `partition_by` (the legacy boolean `latest_batch` partition is gone —
   it was useless as a Liquid cluster key and is no longer needed for
   downstream pruning). Without `partition_by`, insert_overwrite degrades
   to CREATE OR REPLACE TABLE AS SELECT — which is exactly what this
   model already does (its body reads {{ this }} BEFORE the replace via
   the prior CTE, so balances carry through the union). The table is
   small (one row per touched account) so unclustered is fine. #}

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
