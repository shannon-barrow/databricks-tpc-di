{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = ['symbol', 'customerid'],
    incremental_predicates = ['DBT_INTERNAL_DEST.removed = false'],
    merge_update_columns = ['sk_dateid_dateremoved', 'removed'],
    on_schema_change = 'ignore',
    file_format = 'delta',
    full_refresh = false,
  )
}}

{# SCD1 watch-list state. Each (customerid, symbol) pair has at most one
   ACTV-PLACED row (removed=false) and optionally a CNCL (removed=true).
   When a CNCL event arrives, we MERGE-UPDATE the existing not-removed
   row to set removed=true and dateremoved. The incremental_predicates
   `DBT_INTERNAL_DEST.removed = false` keeps already-removed rows from
   being matched again — mirrors the Classic build's `!t.removed` ON
   condition. #}

with new_events as (
  select * from {{ ref('bronzewatches') }}
  {% if is_incremental() %}
  where event_dt > coalesce(
        (select max(event_dt) from {{ ref('bronzewatches') }} b
          where exists (select 1 from {{ this }} t
            where t.customerid = b.w_c_id and t.symbol = b.w_s_symb)),
        cast('1900-01-01' as date)
      )
  {% endif %}
),

w as (
  select
    w_c_id as customerid,
    w_s_symb as symbol,
    date(min(if(w_action != 'CNCL', w_dts, cast(null as timestamp)))) as dateplaced,
    date(max(if(w_action  = 'CNCL', w_dts, cast(null as timestamp)))) as dateremoved
  from new_events
  group by all
)

select
  c.sk_customerid,
  s.sk_securityid,
  w.customerid,
  w.symbol,
  cast(date_format(w.dateplaced, 'yyyyMMdd') as bigint) as sk_dateid_dateplaced,
  cast(date_format(w.dateremoved, 'yyyyMMdd') as bigint) as sk_dateid_dateremoved,
  nvl2(w.dateremoved, true, false) as removed
from w
join {{ source('run_schema', 'dimsecurity') }} s
  on s.symbol = w.symbol
 and s.iscurrent
join {{ ref('dimcustomer') }} c
  on w.customerid = c.customerid
 and c.iscurrent
