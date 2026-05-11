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
    file_format = 'delta',
    full_refresh = false,
  )
}}

{# SCD1 watch-list state. Each (customerid, symbol) pair has at most one
   ACTV-PLACED row (removed=false) and optionally a CNCL (removed=true).
   When a CNCL event arrives, we MERGE-UPDATE the existing not-removed
   row to set removed=true and dateremoved.

   incremental_predicates are ANDed onto the MERGE ON clause, mirroring
   the Classic-Liquid build's MERGE condition:
       `!t.removed AND t.sk_dateid_dateremoved IS NULL`
   - `removed = false`: keeps already-removed rows from being matched again.
   - `sk_dateid_dateremoved IS NULL`: redundant with the first (the two
     columns are kept in sync) but pinning the predicate on the Liquid
     cluster column (`sk_dateid_dateremoved`) lets Delta data-skipping
     prune the merge scan to only the files containing NULLs — the same
     prune the partitioned variant got from `PARTITIONED BY (removed)`. #}

with new_events as (
  select * from {{ ref('bronzewatches') }}
  where event_dt = cast('{{ var("batch_date") }}' as date)
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
