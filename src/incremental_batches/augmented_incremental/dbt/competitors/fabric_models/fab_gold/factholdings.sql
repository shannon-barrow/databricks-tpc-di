{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'append',
    on_schema_change = 'ignore',
    full_refresh = false,
  )
}}

{# Redshift -> Fabric T-SQL — append-only fact: one row per closed-trade +
   holding-event pair.

   STRATEGY DEVIATION from the Redshift model (which uses delete+insert on
   unique_key [tradeid, currenttradeid]): dbt-fabric's delete+insert issues a
   `DELETE ... WHERE (keys) IN (SELECT ... FROM staging)`, and Fabric Delta
   forbids subqueries in DELETE. FactHoldings is append-only (each batch adds
   new holding events, never updates prior ones — matches the Databricks
   models/gold append), so `append` is both correct and Fabric-safe. Re-running
   the same batch would double-insert; benchmark batches run once.
     - TO_CHAR(d, 'YYYYMMDD') -> CONVERT(CHAR(8), d, 112)
#}

with new_events as (
  select
    hh_h_t_id    as tradeid,
    hh_t_id      as currenttradeid,
    hh_after_qty as currentholding,
    event_dt
  from {{ ref('bronzeholdings') }}
  where event_dt = CAST('{{ var("batch_date") }}' AS DATE)
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
 and t.sk_closedateid = CAST(CONVERT(CHAR(8), h.event_dt, 112) AS BIGINT)
