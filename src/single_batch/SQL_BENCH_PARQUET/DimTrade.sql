-- Databricks notebook source
INSERT OVERWRITE ${catalog}.${wh_db}_${scale_factor}.DimTrade
WITH tradeincremental AS (
  SELECT
    tradeid,
    t_dts,
    status,
    t_tt_id,
    cashflag,
    t_s_symb,
    quantity,
    bidprice,
    t_ca_id,
    executedby,
    tradeprice,
    fee,
    commission,
    tax,
    cdc_flag,
    MIN(t_dts) OVER (PARTITION BY tradeid) AS create_ts,
    MIN(cdc_flag) OVER (PARTITION BY tradeid) AS min_cdc_flag,
    MIN(int(substring(_metadata.file_path FROM (position('/Batch', _metadata.file_path) + 6) FOR 1))) OVER (PARTITION BY tradeid) AS batchid
  FROM parquet.`${tpcdi_directory}sf=${scale_factor}/Batch{2,3}/Trade.parquet`
  QUALIFY ROW_NUMBER() OVER (PARTITION BY tradeid ORDER BY t_dts DESC)=1
),
TradeIncrementalHistory AS (
  SELECT
    tradeid,
    t_dts AS ts,
    status
  FROM tradeincremental
  WHERE min_cdc_flag = "U"
  UNION ALL
  SELECT 
    tradeid,
    th_dts AS ts,
    status
  FROM parquet.`${tpcdi_directory}sf=${scale_factor}/Batch1/TradeHistory*.parquet`
),
Current_Trades as (
  SELECT
    tradeid,
    MIN(ts) OVER (PARTITION BY tradeid) create_ts,
    status,
    ts
  FROM TradeIncrementalHistory
  QUALIFY ROW_NUMBER() OVER (PARTITION BY tradeid ORDER BY ts DESC) = 1
),
Trades_Final (
  SELECT
    ct.tradeid,
    ct.create_ts,
    CASE
      WHEN ct.status IN ("CMPT", "CNCL") THEN ct.ts 
      END close_ts,
    ct.status,
    t_is_cash cashflag,
    t_st_id,
    t_tt_id,
    t_s_symb,
    quantity,
    bidprice,
    t_ca_id,
    executedby,
    tradeprice,
    fee,
    commission,
    tax,
    1 batchid
  FROM parquet.`${tpcdi_directory}sf=${scale_factor}/Batch1/Trade_*.parquet` t
  JOIN Current_Trades ct
    ON t.t_id = ct.tradeid
  UNION ALL
  SELECT
    tradeid,
    create_ts,
    CASE
      WHEN status IN ("CMPT", "CNCL") THEN t_dts 
      END close_ts,
    status,
    cashflag,
    status,
    t_tt_id,
    t_s_symb,
    quantity,
    bidprice,
    t_ca_id,
    executedby,
    tradeprice,
    fee,
    commission,
    tax,
    batchid
  FROM tradeincremental
  WHERE min_cdc_flag = "I"
)
SELECT
  trade.tradeid,
  sk_brokerid,
  bigint(date_format(create_ts, 'yyyyMMdd')) sk_createdateid,
  bigint(date_format(create_ts, 'HHmmss')) sk_createtimeid,
  bigint(date_format(close_ts, 'yyyyMMdd')) sk_closedateid,
  bigint(date_format(close_ts, 'HHmmss')) sk_closetimeid,
  decode(trade.status, 
    'ACTV',	'Active',
    'CMPT','Completed',
    'CNCL','Canceled',
    'PNDG','Pending',
    'SBMT','Submitted',
    'INAC','Inactive') status,
  decode(t_tt_id,
    'TMB', 'Market Buy',
    'TMS', 'Market Sell',
    'TSL', 'Stop Loss',
    'TLS', 'Limit Sell',
    'TLB', 'Limit Buy'
  ) type,
  if(cashflag = 1, TRUE, FALSE) cashflag,
  sk_securityid,
  sk_companyid,
  trade.quantity,
  trade.bidprice,
  sk_customerid,
  sk_accountid,
  trade.executedby,
  trade.tradeprice,
  trade.fee,
  trade.commission,
  trade.tax,
  trade.batchid
FROM Trades_Final trade
JOIN ${catalog}.${wh_db}_${scale_factor}.DimSecurity ds
  ON 
    ds.symbol = trade.t_s_symb
    AND date(create_ts) >= ds.effectivedate 
    AND date(create_ts) < ds.enddate
JOIN ${catalog}.${wh_db}_${scale_factor}.DimAccount da
  ON 
    trade.t_ca_id = da.accountid 
    AND date(create_ts) >= da.effectivedate 
    AND date(create_ts) < da.enddate
