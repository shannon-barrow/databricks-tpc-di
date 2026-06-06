{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['tradeid', 'currenttradeid'],
    on_schema_change = 'ignore',
    full_refresh = false,
  )
}}

{# Redshift variant of factholdings. BQ uses insert_overwrite +
   integer-range partition_by on sk_dateid as an idiomatic equivalent of
   APPEND (each batch overwrites only its own date partition). Redshift
   has no partition concept exposed to dbt; the analogous strategy is
   delete+insert, which deletes existing rows matching unique_key and
   then inserts the new batch.

   Note on unique_key: BQ's APPEND semantics don't require a uniqueness
   contract, but dbt-redshift's delete+insert needs unique_key to scope
   the delete. (tradeid, currenttradeid) is the natural composite key
   for one holding event (FactHoldings is keyed by H_T_ID, H_CT_ID in
   the TPC-DI spec). DistKey/SortKey on sk_dateid is set at CREATE
   time in setup_rs.py — no layout declaration here ("setup-owns-layout"
   pattern).

   Translations from BQ:
     - FORMAT_DATE('%Y%m%d', d) -> TO_CHAR(d, 'YYYYMMDD')
     - CAST(... AS INT64)       -> CAST(... AS BIGINT)
     - partition_by config       -> removed (Redshift uses DISTKEY/SORTKEY,
                                    owned by setup notebook)
     - copy_partitions          -> n/a (BQ-only optimization)
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
 and t.sk_closedateid = CAST(TO_CHAR(h.event_dt, 'YYYYMMDD') AS BIGINT)
