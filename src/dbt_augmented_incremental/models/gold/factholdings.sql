{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'append',
    on_schema_change = 'ignore',
    file_format = 'delta',
    full_refresh = false,
  )
}}

{# Append-only fact. One row per holding-history event whose corresponding
   trade just closed in this batch (sk_closedateid = today). #}

with new_events as (
  select
    hh_h_t_id as tradeid,
    hh_t_id as currenttradeid,
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
  t.tradeprice as currentprice,
  h.currentholding
from new_events h
join {{ ref('dimtrade') }} t
  on t.tradeid = h.tradeid
where t.sk_closedateid = cast(date_format(cast('{{ var("batch_date") }}' as date), 'yyyyMMdd') as bigint)
