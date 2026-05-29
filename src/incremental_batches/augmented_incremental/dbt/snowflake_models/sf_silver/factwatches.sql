{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = ['symbol', 'customerid'],
    incremental_predicates = [
      'DBT_INTERNAL_DEST.removed = false',
      'DBT_INTERNAL_DEST.sk_dateid_dateremoved IS NULL',
    ],
    merge_update_columns = ['sk_dateid_dateremoved', 'removed'],
    on_schema_change = 'ignore',
    full_refresh = false,
  )
}}

{# Snowflake variant of factwatches. SCD1 (one row per customer×symbol,
   updated when CNCL arrives). Same incremental_predicates contract;
   merge-prune relies on the table being clustered on
   sk_dateid_dateremoved (set in setup, not here).

   Translations from Databricks:
     - if(cond, t, NULL)           -> iff(cond, t, NULL)
     - date(ts)                    -> to_date(ts)
     - date_format(d, 'yyyyMMdd')  -> to_char(d, 'YYYYMMDD')
     - nvl2 / group by all         -> Snowflake supports natively
     - cast(... as bigint)         -> ::number #}

with new_events as (
  select * from {{ ref('bronzewatches') }}
  where event_dt = cast('{{ var("batch_date") }}' as date)
),

w as (
  select
    w_c_id   as customerid,
    w_s_symb as symbol,
    to_date(min(iff(w_action != 'CNCL', w_dts, cast(null as timestamp)))) as dateplaced,
    to_date(max(iff(w_action  = 'CNCL', w_dts, cast(null as timestamp)))) as dateremoved
  from new_events
  group by all
)

select
  c.sk_customerid,
  s.sk_securityid,
  w.customerid,
  w.symbol,
  to_char(w.dateplaced,  'YYYYMMDD')::number as sk_dateid_dateplaced,
  to_char(w.dateremoved, 'YYYYMMDD')::number as sk_dateid_dateremoved,
  nvl2(w.dateremoved, true, false) as removed
from w
join {{ source('run_schema', 'dimsecurity') }} s
  on s.symbol = w.symbol
 and s.iscurrent
join {{ ref('dimcustomer') }} c
  on w.customerid = c.customerid
 and c.iscurrent
