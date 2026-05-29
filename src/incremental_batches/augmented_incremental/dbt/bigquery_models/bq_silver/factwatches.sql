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

{# BQ variant of factwatches. SCD1 (one row per customer×symbol, updated
   when CNCL arrives). Same incremental_predicates contract.

   Translations from Snowflake:
     - iff(cond, t, NULL)          -> IF(cond, t, NULL)
     - to_date(ts)                 -> DATE(ts)
     - to_char(d, 'YYYYMMDD')      -> FORMAT_DATE('%Y%m%d', d)
     - nvl2(x, t, f)               -> IF(x IS NOT NULL, t, f)
     - ::number                    -> CAST(... AS INT64)
     - cast(null as timestamp)     -> CAST(NULL AS TIMESTAMP)
     - group by all                -> BQ supports GROUP BY ALL
#}

with new_events as (
  select * from {{ ref('bronzewatches') }}
  where event_dt = DATE '{{ var("batch_date") }}'
),

w as (
  select
    w_c_id   as customerid,
    w_s_symb as symbol,
    DATE(MIN(IF(w_action != 'CNCL', w_dts, CAST(NULL AS TIMESTAMP)))) as dateplaced,
    DATE(MAX(IF(w_action  = 'CNCL', w_dts, CAST(NULL AS TIMESTAMP)))) as dateremoved
  from new_events
  group by all
)

select
  c.sk_customerid,
  s.sk_securityid,
  w.customerid,
  w.symbol,
  CAST(FORMAT_DATE('%Y%m%d', w.dateplaced)  AS INT64) as sk_dateid_dateplaced,
  CAST(FORMAT_DATE('%Y%m%d', w.dateremoved) AS INT64) as sk_dateid_dateremoved,
  IF(w.dateremoved IS NOT NULL, true, false) as removed
from w
join {{ source('run_schema', 'dimsecurity') }} s
  on s.symbol = w.symbol
 and s.iscurrent
join {{ ref('dimcustomer') }} c
  on w.customerid = c.customerid
 and c.iscurrent
