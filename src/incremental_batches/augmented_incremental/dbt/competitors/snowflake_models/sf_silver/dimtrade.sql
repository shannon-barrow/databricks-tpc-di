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

{# Snowflake variant of dimtrade. Same merge contract as Databricks
   (open-trades partition prune via incremental_predicates +
   merge_update_columns scope).

   Translations from Databricks:
     - max_by(struct(...), key)    -> max_by(object_construct(...), key)
                                       with subsequent field access via
                                       current_record:field::type
     - date_format(ts, 'yyyyMMdd') -> to_char(ts, 'YYYYMMDD')
     - date_format(ts, 'HHmmss')   -> to_char(ts, 'HH24MISS')
     - cast(... as bigint)         -> ::number
     - if(cond, t, f)              -> iff(cond, t, f)
     - decode/case                 -> Snowflake supports natively
     - date(ts)                    -> to_date(ts) #}

with new_events as (
  select * from {{ ref('bronzetrade') }}
  where event_dt = cast('{{ var("batch_date") }}' as date)
),

trades as (
  select
    tradeid,
    min(case when cdc_flag = 'I' then t_dts end) as create_ts,
    max_by(object_construct(
      't_dts',      t_dts,
      'status',     status,
      't_tt_id',    t_tt_id,
      'cashflag',   cashflag,
      't_s_symb',   t_s_symb,
      'quantity',   quantity,
      'bidprice',   bidprice,
      't_ca_id',    t_ca_id,
      'executedby', executedby,
      'tradeprice', tradeprice,
      'fee',        fee,
      'commission', commission,
      'tax',        tax
    ), t_dts) as current_record
  from new_events
  group by tradeid
),

current_trades as (
  select
    tradeid,
    create_ts,
    case
      when current_record:status::string in ('CMPT', 'CNCL')
        then current_record:t_dts::timestamp
    end as close_ts,
    decode(current_record:status::string,
      'ACTV', 'Active',
      'CMPT', 'Completed',
      'CNCL', 'Canceled',
      'PNDG', 'Pending',
      'SBMT', 'Submitted',
      'INAC', 'Inactive') as status,
    decode(current_record:t_tt_id::string,
      'TMB', 'Market Buy',
      'TMS', 'Market Sell',
      'TSL', 'Stop Loss',
      'TLS', 'Limit Sell',
      'TLB', 'Limit Buy') as type,
    iff(current_record:cashflag::tinyint = 1, true, false)         as cashflag,
    current_record:t_s_symb::string                                 as t_s_symb,
    current_record:quantity::int                                    as quantity,
    current_record:bidprice::float                                  as bidprice,
    current_record:t_ca_id::bigint                                  as t_ca_id,
    current_record:executedby::string                               as executedby,
    current_record:tradeprice::float                                as tradeprice,
    current_record:fee::float                                       as fee,
    current_record:commission::float                                as commission,
    current_record:tax::float                                       as tax,
    current_record:t_dts::timestamp                                 as max_t_dts
  from trades
)

select
  t.tradeid,
  da.sk_brokerid,
  to_char(create_ts, 'YYYYMMDD')::number as sk_createdateid,
  to_char(create_ts, 'HH24MISS')::number as sk_createtimeid,
  to_char(close_ts,  'YYYYMMDD')::number as sk_closedateid,
  to_char(close_ts,  'HH24MISS')::number as sk_closetimeid,
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
 and to_date(t.max_t_dts) >= ds.effectivedate
 and to_date(t.max_t_dts) <  ds.enddate
join {{ ref('dimaccount') }} da
  on t.t_ca_id = da.accountid
 and da.iscurrent
