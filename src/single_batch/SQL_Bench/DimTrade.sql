-- Databricks notebook source
INSERT OVERWRITE ${catalog}.${wh_db}_${scale_factor}.DimTrade
WITH tradeincremental AS (
  SELECT
    *,
    int(substring(_metadata.file_path FROM (position('/Batch', _metadata.file_path) + 6) FOR 1)) batchid
  FROM read_files(
    "${tpcdi_directory}sf=${scale_factor}/Batch{2,3}",
    format => "csv",
    inferSchema => False,
    header => False,
    sep => "|",
    fileNamePattern => "Trade.txt",
    schema => "cdc_flag STRING, cdc_dsn BIGINT, tradeid BIGINT, t_dts TIMESTAMP, status STRING, t_tt_id STRING, cashflag TINYINT, t_s_symb STRING, quantity INT, bidprice DOUBLE, t_ca_id BIGINT, executedby STRING, tradeprice DOUBLE, fee DOUBLE, commission DOUBLE, tax DOUBLE"
  )
),
finaltrades AS (
  SELECT
    min(cdc_flag) cdc_flag,
    tradeid,
    min(t_dts) create_ts,
    max_by(
      struct(
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
        tax
      ),
      t_dts
    ) current_record,
    min(t.batchid) batchid
  FROM tradeincremental t
  group by tradeid
),
TradeIncrementalHistory AS (
  SELECT
    tradeid,
    current_record.t_dts ts,
    current_record.status
  FROM finaltrades
  WHERE cdc_flag = "U"
  UNION ALL
  SELECT *
  FROM read_files(
    "${tpcdi_directory}sf=${scale_factor}/Batch1",
    format => "csv",
    inferSchema => False,
    header => False,
    sep => "|",
    fileNamePattern => "TradeHistory.txt",
    schema => "tradeid BIGINT, th_dts TIMESTAMP, status STRING"
  )
),
Current_Trades as (
  SELECT
    tradeid,
    min(ts) create_ts,
    max_by(struct(ts, status), ts) current_status
  FROM TradeIncrementalHistory
  group by tradeid
),
Trades_Final (
  SELECT
    tradeid,
    create_ts,
    CASE
      WHEN current_status.status IN ("CMPT", "CNCL") THEN current_status.ts 
      END close_ts,
    current_status.status,
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
  FROM read_files(
    "${tpcdi_directory}sf=${scale_factor}/Batch1",
    format => "csv",
    inferSchema => False,
    header => False,
    sep => "|",
    fileNamePattern => "Trade.txt",
    schema => "t_id BIGINT, t_dts TIMESTAMP, t_st_id STRING, t_tt_id STRING, t_is_cash TINYINT, t_s_symb STRING, quantity INT, bidprice DOUBLE, t_ca_id BIGINT, executedby STRING, tradeprice DOUBLE, fee DOUBLE, commission DOUBLE, tax DOUBLE"
  ) t
  JOIN Current_Trades ct
    ON t.t_id = ct.tradeid
  UNION ALL
  SELECT
    tradeid,
    create_ts,
    CASE
      WHEN current_record.status IN ("CMPT", "CNCL") THEN current_record.t_dts 
      END close_ts,
    current_record.status,
    current_record.cashflag,
    current_record.status,
    current_record.t_tt_id,
    current_record.t_s_symb,
    current_record.quantity,
    current_record.bidprice,
    current_record.t_ca_id,
    current_record.executedby,
    current_record.tradeprice,
    current_record.fee,
    current_record.commission,
    current_record.tax,
    batchid
  FROM finaltrades
  WHERE cdc_flag = "I"
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
