{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'tradeid',
    incremental_predicates = ['DBT_INTERNAL_DEST.sk_closedateid IS NULL'],
    merge_update_columns = [
      'sk_closedateid', 'sk_closetimeid', 'status', 'type', 'cashflag',
      'quantity', 'bidprice', 'executedby', 'tradeprice', 'fee',
      'commission', 'tax'
    ],
    on_schema_change = 'ignore',
    full_refresh = false,
  )
}}

{# Redshift variant of dimtrade. Same merge contract as the BQ/SF/Databricks
   variants (open-trades prune via incremental_predicates + scoped
   merge_update_columns).

   Translations from BQ:
     - MAX_BY(STRUCT(...), key)              -> Redshift has no MAX_BY and no
                                                STRUCT. Rewrite as
                                                ROW_NUMBER() OVER (PARTITION
                                                BY tradeid ORDER BY t_dts DESC)
                                                = 1 in a subquery, then aggregate
                                                create_ts separately and join
                                                back on tradeid.
     - STRUCT field access (current_record.x) -> direct column from the
                                                rn=1 row
     - FORMAT_TIMESTAMP('%Y%m%d', ts)         -> TO_CHAR(ts, 'YYYYMMDD')
     - FORMAT_TIMESTAMP('%H%M%S', ts)         -> TO_CHAR(ts, 'HH24MISS')
     - CAST(... AS INT64)                     -> CAST(... AS BIGINT)
     - CAST(... AS FLOAT64)                   -> CAST(... AS DOUBLE PRECISION)
     - DATE(ts)                               -> CAST(ts AS DATE)
     - IF(c, t, f)                            -> CASE WHEN c THEN t ELSE f END
#}

with new_events as (
  select * from {{ ref('bronzetrade') }}
  where event_dt = CAST('{{ var("batch_date") }}' AS DATE)
),

-- One row per tradeid: the latest record in this batch. Replaces BQ's
-- MAX_BY(STRUCT(...), t_dts).
latest_per_trade as (
  select
    tradeid, t_dts, status, t_tt_id, cashflag, t_s_symb, quantity, bidprice,
    t_ca_id, executedby, tradeprice, fee, commission, tax,
    row_number() over (partition by tradeid order by t_dts desc) as rn
  from new_events
),

-- Earliest create_ts per tradeid (only from 'I' rows in this batch).
create_ts_per_trade as (
  select
    tradeid,
    min(case when cdc_flag = 'I' then t_dts end) as create_ts
  from new_events
  group by tradeid
),

current_trades as (
  select
    l.tradeid,
    c.create_ts,
    CASE
      WHEN l.status IN ('CMPT', 'CNCL')
        THEN l.t_dts
    END as close_ts,
    CASE l.status
      WHEN 'ACTV' THEN 'Active'
      WHEN 'CMPT' THEN 'Completed'
      WHEN 'CNCL' THEN 'Canceled'
      WHEN 'PNDG' THEN 'Pending'
      WHEN 'SBMT' THEN 'Submitted'
      WHEN 'INAC' THEN 'Inactive'
    END as status,
    CASE l.t_tt_id
      WHEN 'TMB' THEN 'Market Buy'
      WHEN 'TMS' THEN 'Market Sell'
      WHEN 'TSL' THEN 'Stop Loss'
      WHEN 'TLS' THEN 'Limit Sell'
      WHEN 'TLB' THEN 'Limit Buy'
    END as type,
    CASE WHEN CAST(l.cashflag AS BIGINT) = 1 THEN true ELSE false END as cashflag,
    l.t_s_symb                                  as t_s_symb,
    CAST(l.quantity AS BIGINT)                  as quantity,
    CAST(l.bidprice AS DOUBLE PRECISION)        as bidprice,
    CAST(l.t_ca_id AS BIGINT)                   as t_ca_id,
    l.executedby                                as executedby,
    CAST(l.tradeprice AS DOUBLE PRECISION)      as tradeprice,
    CAST(l.fee AS DOUBLE PRECISION)             as fee,
    CAST(l.commission AS DOUBLE PRECISION)      as commission,
    CAST(l.tax AS DOUBLE PRECISION)             as tax,
    l.t_dts                                     as max_t_dts
  from latest_per_trade l
  join create_ts_per_trade c on c.tradeid = l.tradeid
  where l.rn = 1
)

select
  t.tradeid,
  da.sk_brokerid,
  CAST(TO_CHAR(create_ts, 'YYYYMMDD') AS BIGINT) as sk_createdateid,
  CAST(TO_CHAR(create_ts, 'HH24MISS') AS BIGINT) as sk_createtimeid,
  CAST(TO_CHAR(close_ts,  'YYYYMMDD') AS BIGINT) as sk_closedateid,
  CAST(TO_CHAR(close_ts,  'HH24MISS') AS BIGINT) as sk_closetimeid,
  t.status,
  t.type,
  t.cashflag,
  ds.sk_securityid,
  ds.sk_companyid,
  t.quantity,
  t.bidprice,
  da.sk_customerid,
  da.sk_accountid,
  t.executedby,
  t.tradeprice,
  t.fee,
  t.commission,
  t.tax
from current_trades t
join {{ source('run_schema', 'dimsecurity') }} ds
  on ds.symbol = t.t_s_symb
 and CAST(t.max_t_dts AS DATE) >= ds.effectivedate
 and CAST(t.max_t_dts AS DATE) <  ds.enddate
join {{ ref('dimaccount') }} da
  on t.t_ca_id = da.accountid
 and da.iscurrent
