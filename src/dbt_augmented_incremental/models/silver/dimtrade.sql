{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'tradeid',
    merge_update_columns = [
      'sk_closedateid', 'sk_closetimeid', 'status', 'type', 'cashflag',
      'quantity', 'bidprice', 'executedby', 'tradeprice', 'fee',
      'commission', 'tax'
    ],
    on_schema_change = 'ignore',
    file_format = 'delta',
    full_refresh = false,
  )
}}

{# Trade lifecycle: SBMT -> ACTV -> (CMPT | CNCL). One bronze event per
   transition. Aggregate per tradeid keeping create_ts (the 'I' event's
   timestamp) and the latest record (max_by t_dts). On MERGE: insert if
   not seen; otherwise only update the close-time + mutable fields,
   leaving the surrogate-key chain (sk_brokerid, sk_securityid, etc.)
   from the create event unchanged. #}

with new_events as (
  select * from {{ ref('bronzetrade') }}
  where event_dt = cast('{{ var("batch_date") }}' as date)
),

trades as (
  select
    tradeid,
    min(case when cdc_flag = 'I' then t_dts end) as create_ts,
    max_by(struct(
      t_dts, status, t_tt_id, cashflag, t_s_symb, quantity, bidprice,
      t_ca_id, executedby, tradeprice, fee, commission, tax
    ), t_dts) as current_record
  from new_events
  group by tradeid
),

current_trades as (
  select
    tradeid,
    create_ts,
    case when current_record.status in ('CMPT', 'CNCL') then current_record.t_dts end as close_ts,
    decode(current_record.status,
      'ACTV', 'Active',
      'CMPT', 'Completed',
      'CNCL', 'Canceled',
      'PNDG', 'Pending',
      'SBMT', 'Submitted',
      'INAC', 'Inactive') as status,
    decode(current_record.t_tt_id,
      'TMB', 'Market Buy',
      'TMS', 'Market Sell',
      'TSL', 'Stop Loss',
      'TLS', 'Limit Sell',
      'TLB', 'Limit Buy') as type,
    if(current_record.cashflag = 1, true, false) as cashflag,
    current_record.t_s_symb,
    current_record.quantity,
    current_record.bidprice,
    current_record.t_ca_id,
    current_record.executedby,
    current_record.tradeprice,
    current_record.fee,
    current_record.commission,
    current_record.tax,
    current_record.t_dts as max_t_dts
  from trades
)

select
  t.tradeid,
  da.sk_brokerid,
  cast(date_format(create_ts, 'yyyyMMdd') as bigint) as sk_createdateid,
  cast(date_format(create_ts, 'HHmmss') as bigint) as sk_createtimeid,
  cast(date_format(close_ts, 'yyyyMMdd') as bigint) as sk_closedateid,
  cast(date_format(close_ts, 'HHmmss') as bigint) as sk_closetimeid,
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
 and date(t.max_t_dts) >= ds.effectivedate
 and date(t.max_t_dts) <  ds.enddate
join {{ ref('dimaccount') }} da
  on t.t_ca_id = da.accountid
 and da.iscurrent
