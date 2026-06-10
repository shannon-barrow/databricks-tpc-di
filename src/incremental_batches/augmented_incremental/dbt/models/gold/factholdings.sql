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
   trade just closed in this batch (sk_closedateid = today).

   Query shape intentionally mirrors the SDP factholdings_incremental flow
   so the cross-variant comparison stays apples-to-apples — the
   sk_closedateid predicate lives in the ON clause and references the
   per-row h.event_dt instead of a constant pulled from `batch_date`.
   DBSQL/Photon constant-folds h.event_dt = batch_date through the CTE
   into the join's sk_closedateid predicate. NOTE: dimtrade is now
   clustered on sk_customerid (for the PO/auto-cluster comparison), so
   this predicate is a logical filter rather than a cluster-aligned prune
   on dimtrade — the join scans dimtrade by sk_closedateid without the
   data-skipping it previously got from CLUSTER BY (sk_closedateid). Kept
   in the ON clause to match the plan SDP runs against. #}

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
 and t.sk_closedateid = cast(date_format(h.event_dt, 'yyyyMMdd') as bigint)
