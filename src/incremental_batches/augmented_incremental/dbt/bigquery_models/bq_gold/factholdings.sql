{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'append',
    on_schema_change = 'ignore',
    full_refresh = false,
  )
}}

{# BQ variant of factholdings. Append-only fact, one row per holding-history
   event whose trade just closed today.

   Translations from Snowflake:
     - to_char(d, 'YYYYMMDD') -> FORMAT_DATE('%Y%m%d', d)
     - ::number               -> CAST(... AS INT64)
#}

with new_events as (
  select
    hh_h_t_id    as tradeid,
    hh_t_id      as currenttradeid,
    hh_after_qty as currentholding,
    event_dt
  from {{ ref('bronzeholdings') }}
  where event_dt = DATE '{{ var("batch_date") }}'
)

select
  h.tradeid,
  h.currenttradeid,
  t.sk_customerid,
  t.sk_accountid,
  t.sk_securityid,
  t.sk_companyid,
  t.sk_closedateid as sk_dateid,
  t.sk_closetimeid as sk_timeid,
  t.tradeprice     as currentprice,
  h.currentholding
from new_events h
join {{ ref('dimtrade') }} t
  on t.tradeid = h.tradeid
 and t.sk_closedateid = CAST(FORMAT_DATE('%Y%m%d', h.event_dt) AS INT64)
