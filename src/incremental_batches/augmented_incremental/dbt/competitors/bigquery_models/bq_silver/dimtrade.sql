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

{# BQ variant of dimtrade. Same merge contract as the SF/Databricks variants.

   Translations from Snowflake:
     - max_by(object_construct(...), key)  -> MAX_BY(STRUCT(... AS a, ...), key)
                                              (BQ supports MAX_BY since 2024)
     - current_record:field::type           -> CAST(current_record.field AS type)
                                              (BQ structs access fields with `.`)
     - to_char(ts, 'YYYYMMDD')              -> FORMAT_TIMESTAMP('%Y%m%d', ts)
     - to_char(ts, 'HH24MISS')              -> FORMAT_TIMESTAMP('%H%M%S', ts)
     - ::number                              -> CAST(... AS INT64)
     - ::float                              -> CAST(... AS FLOAT64)
     - ::tinyint                            -> CAST(... AS INT64) (BQ has no TINYINT)
     - to_date(ts)                          -> DATE(ts)
     - iff(cond, t, f)                      -> IF(cond, t, f)
     - decode(...)                          -> CASE WHEN END
#}

with new_events as (
  select * from {{ ref('bronzetrade') }}
  where event_dt = DATE '{{ var("batch_date") }}'
),

trades as (
  select
    tradeid,
    MIN(IF(cdc_flag = 'I', t_dts, NULL)) as create_ts,
    MAX_BY(STRUCT(
      t_dts      AS t_dts,
      status     AS status,
      t_tt_id    AS t_tt_id,
      cashflag   AS cashflag,
      t_s_symb   AS t_s_symb,
      quantity   AS quantity,
      bidprice   AS bidprice,
      t_ca_id    AS t_ca_id,
      executedby AS executedby,
      tradeprice AS tradeprice,
      fee        AS fee,
      commission AS commission,
      tax        AS tax
    ), t_dts) as current_record
  from new_events
  group by tradeid
),

current_trades as (
  select
    tradeid,
    create_ts,
    CASE
      WHEN current_record.status IN ('CMPT', 'CNCL')
        THEN current_record.t_dts
    END as close_ts,
    CASE current_record.status
      WHEN 'ACTV' THEN 'Active'
      WHEN 'CMPT' THEN 'Completed'
      WHEN 'CNCL' THEN 'Canceled'
      WHEN 'PNDG' THEN 'Pending'
      WHEN 'SBMT' THEN 'Submitted'
      WHEN 'INAC' THEN 'Inactive'
    END as status,
    CASE current_record.t_tt_id
      WHEN 'TMB' THEN 'Market Buy'
      WHEN 'TMS' THEN 'Market Sell'
      WHEN 'TSL' THEN 'Stop Loss'
      WHEN 'TLS' THEN 'Limit Sell'
      WHEN 'TLB' THEN 'Limit Buy'
    END as type,
    IF(CAST(current_record.cashflag AS INT64) = 1, true, false)        as cashflag,
    current_record.t_s_symb                                              as t_s_symb,
    CAST(current_record.quantity AS INT64)                              as quantity,
    CAST(current_record.bidprice AS FLOAT64)                            as bidprice,
    CAST(current_record.t_ca_id AS INT64)                               as t_ca_id,
    current_record.executedby                                            as executedby,
    CAST(current_record.tradeprice AS FLOAT64)                          as tradeprice,
    CAST(current_record.fee AS FLOAT64)                                 as fee,
    CAST(current_record.commission AS FLOAT64)                          as commission,
    CAST(current_record.tax AS FLOAT64)                                 as tax,
    current_record.t_dts                                                 as max_t_dts
  from trades
)

select
  t.tradeid,
  da.sk_brokerid,
  CAST(FORMAT_TIMESTAMP('%Y%m%d', create_ts) AS INT64) as sk_createdateid,
  CAST(FORMAT_TIMESTAMP('%H%M%S', create_ts) AS INT64) as sk_createtimeid,
  CAST(FORMAT_TIMESTAMP('%Y%m%d', close_ts)  AS INT64) as sk_closedateid,
  CAST(FORMAT_TIMESTAMP('%H%M%S', close_ts)  AS INT64) as sk_closetimeid,
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
 and DATE(t.max_t_dts) >= ds.effectivedate
 and DATE(t.max_t_dts) <  ds.enddate
join {{ ref('dimaccount') }} da
  on t.t_ca_id = da.accountid
 and da.iscurrent
