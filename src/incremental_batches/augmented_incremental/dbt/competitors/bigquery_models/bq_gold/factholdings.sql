{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by = {
      'field': 'sk_dateid',
      'data_type': 'int64',
      'range': {'start': 20160706, 'end': 20170703, 'interval': 1},
    },
    on_schema_change = 'ignore',
    full_refresh = false,
  )
}}

{# BQ variant of factholdings — BQ's idiomatic equivalent of SF/DBX's
   APPEND strategy. dbt-bigquery has no 'append' strategy; insert_overwrite
   with INT64 partition_by on sk_dateid mimics append: each batch overwrites
   only its own date's partition. (copy_partitions=true is NOT valid for
   INT64 range partitions — BQ falls back to a MERGE-based partition swap,
   functionally equivalent.)

   Range cap: BQ caps integer-range partitions at 10,000 buckets per table.
   GENERATE_ARRAY(20160706, 20170703, 1) produces 9,998 boundary points →
   9,999 buckets, under the cap. Benchmark dates 20170704/20170705 fall
   into the "over" bucket — still partition-pruneable, still atomic swap,
   just lumped together. For SF=10 single-batch (batch_date=20160706),
   each date gets its own clean bucket.

   No cluster_by: partition_by sk_dateid already prunes by date; a redundant
   cluster on the same column adds no pruning value. SF/DBX use cluster_by
   sk_dateid because they have no append-via-partition-swap equivalent —
   their clustering is functionally substituting for BQ's partition pruning.

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
