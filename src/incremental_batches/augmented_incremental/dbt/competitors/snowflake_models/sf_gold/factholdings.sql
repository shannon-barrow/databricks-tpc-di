{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'append',
    on_schema_change = 'ignore',
    full_refresh = false,
  )
}}

{# Snowflake variant of factholdings. Append-only fact, one row per
   holding-history event whose trade just closed today. ON-clause shape
   matches the SDP flow so the cross-variant comparison stays
   apples-to-apples; Snowflake's optimizer should likewise push the
   per-row sk_closedateid predicate through the CTE join.

   Translations:
     - date_format(d, 'yyyyMMdd')  -> to_char(d, 'YYYYMMDD')
     - cast(... as bigint)         -> ::number #}

with new_events as (
  select
    hh_h_t_id    as tradeid,
    hh_t_id      as currenttradeid,
    hh_after_qty as currentholding,
    event_dt
  from {{ ref('bronzeholdings') }}
  where event_dt = cast('{{ var("batch_date") }}' as date)
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
 and t.sk_closedateid = to_char(h.event_dt, 'YYYYMMDD')::number
