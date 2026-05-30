{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by = {
      'field': 'sk_dateid',
      'data_type': 'int64',
      'range': {'start': 20160706, 'end': 20170706, 'interval': 1},
      'copy_partitions': true,
    },
    on_schema_change = 'ignore',
    full_refresh = false,
  )
}}

{# BQ variant of factholdings — BQ's idiomatic equivalent of SF/DBX's
   APPEND strategy. dbt-bigquery has no 'append' strategy; insert_overwrite
   with partition_by on sk_dateid (INT64) + copy_partitions=True does a
   metadata-only Copy Table API swap (zero compute cost — identical cost
   profile to an INSERT INTO append).

   Range is tight to our 365-day benchmark window (20160706..20170705).
   BQ's 10K-partition cap is the only reason we don't use a wider buffer;
   in a real production env, data older than ~27 years (10K days) would
   already be in cold storage.

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
