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

{# Redshift variant of factwatches. SCD1 (one row per customer×symbol,
   updated when CNCL arrives). Same incremental_predicates contract.

   Translations from BQ:
     - IF(cond, t, NULL)         -> CASE WHEN cond THEN t ELSE NULL END
     - DATE(ts)                  -> CAST(ts AS DATE)
     - FORMAT_DATE('%Y%m%d', d)  -> TO_CHAR(d, 'YYYYMMDD')
     - CAST(... AS INT64)        -> CAST(... AS BIGINT)
     - GROUP BY ALL              -> spell columns out (Redshift doesn't
                                     support GROUP BY ALL)
     - boolean literal           -> kept as Redshift BOOLEAN
#}

with new_events as (
  select * from {{ ref('bronzewatches') }}
  where event_dt = CAST('{{ var("batch_date") }}' AS DATE)
),

w as (
  select
    w_c_id   as customerid,
    w_s_symb as symbol,
    CAST(MIN(CASE WHEN w_action != 'CNCL' THEN w_dts ELSE CAST(NULL AS TIMESTAMP) END) AS DATE) as dateplaced,
    CAST(MAX(CASE WHEN w_action  = 'CNCL' THEN w_dts ELSE CAST(NULL AS TIMESTAMP) END) AS DATE) as dateremoved
  from new_events
  group by w_c_id, w_s_symb
)

select
  c.sk_customerid,
  s.sk_securityid,
  w.customerid,
  w.symbol,
  CAST(TO_CHAR(w.dateplaced,  'YYYYMMDD') AS BIGINT) as sk_dateid_dateplaced,
  CAST(TO_CHAR(w.dateremoved, 'YYYYMMDD') AS BIGINT) as sk_dateid_dateremoved,
  CASE WHEN w.dateremoved IS NOT NULL THEN true ELSE false END as removed
from w
join {{ source('run_schema', 'dimsecurity') }} s
  on s.symbol = w.symbol
 and s.iscurrent
join {{ ref('dimcustomer') }} c
  on w.customerid = c.customerid
 and c.iscurrent
