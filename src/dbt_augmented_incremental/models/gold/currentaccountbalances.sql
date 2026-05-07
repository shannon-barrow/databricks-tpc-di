{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by = 'latest_batch',
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

   Partitioned by `latest_batch` so the downstream factcashbalances model
   (which filters `where latest_batch`) prunes to the single TRUE
   partition — fast point-read for the few thousand accounts touched in
   the current batch, instead of scanning the full multi-million-row
   account-balance set.

   On a SQL Warehouse, dbt-databricks's insert_overwrite degrades to
   `CREATE OR REPLACE TABLE AS SELECT` (full table replace). The model
   body reads {{ this }} BEFORE the replace happens, so prior balances
   carry through the union — same outcome as Classic's INSERT OVERWRITE. #}

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
