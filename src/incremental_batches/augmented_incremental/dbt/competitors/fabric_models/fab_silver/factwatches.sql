{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = ['symbol', 'customerid'],
    incremental_predicates = [
      'DBT_INTERNAL_DEST.removed = 0',
      'DBT_INTERNAL_DEST.sk_dateid_dateremoved IS NULL',
    ],
    merge_update_columns = ['sk_dateid_dateremoved', 'removed'],
    on_schema_change = 'ignore',
    full_refresh = false,
  )
}}

{# Redshift -> Fabric T-SQL — SCD1 (one row per customer×symbol, updated when
   CNCL arrives). Same incremental_predicates contract, with the boolean
   predicate rewritten for BIT: `removed = false` -> `removed = 0`.
     - TIMESTAMP               -> DATETIME2
     - TO_CHAR(d, 'YYYYMMDD')  -> CONVERT(CHAR(8), d, 112)
     - boolean true/false      -> CAST(1 AS BIT) / CAST(0 AS BIT)
     - join <boolcol>          -> <boolcol> = 1
#}

with new_events as (
  select * from {{ ref('bronzewatches') }}
  where event_dt = CAST('{{ var("batch_date") }}' AS DATE)
),

w as (
  select
    w_c_id   as customerid,
    w_s_symb as symbol,
    CAST(MIN(CASE WHEN w_action <> 'CNCL' THEN w_dts ELSE CAST(NULL AS DATETIME2) END) AS DATE) as dateplaced,
    CAST(MAX(CASE WHEN w_action  = 'CNCL' THEN w_dts ELSE CAST(NULL AS DATETIME2) END) AS DATE) as dateremoved
  from new_events
  group by w_c_id, w_s_symb
)

select
  c.sk_customerid,
  s.sk_securityid,
  w.customerid,
  w.symbol,
  CAST(CONVERT(CHAR(8), w.dateplaced,  112) AS BIGINT) as sk_dateid_dateplaced,
  CAST(CONVERT(CHAR(8), w.dateremoved, 112) AS BIGINT) as sk_dateid_dateremoved,
  CASE WHEN w.dateremoved IS NOT NULL THEN CAST(1 AS BIT) ELSE CAST(0 AS BIT) END as removed
from w
join {{ source('run_schema', 'dimsecurity') }} s
  on s.symbol = w.symbol
 and s.iscurrent = 1
join {{ ref('dimcustomer') }} c
  on w.customerid = c.customerid
 and c.iscurrent = 1
