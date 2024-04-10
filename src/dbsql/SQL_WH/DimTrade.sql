-- Databricks notebook source
INSERT INTO ${catalog}.${wh_db}.DimTrade
WITH TradeIncremental AS (
  SELECT
    min(cdc_flag) cdc_flag,
    t_id tradeid,
    min(t_dts) create_ts,
    max_by(
      struct(
        t_dts,
        t_st_id status,
        t_tt_id,
        t_is_cash cashflag,
        t_s_symb,
        t_qty quantity,
        t_bid_price bidprice,
        t_ca_id,
        t_exec_name executedby,
        t_trade_price tradeprice,
        t_chrg fee,
        t_comm commission,
        t_tax tax
      ),
      t_dts
    ) current_record,
    min(t.batchid) batchid
  FROM
    ${catalog}.${wh_db}_stage.v_TradeIncremental t
  group by
    t_id
),
TradeIncrementalHistory AS (
  SELECT
    tradeid,
    current_record.t_dts ts,
    current_record.status
  FROM
    TradeIncremental
  WHERE cdc_flag = "U"
  UNION ALL
  SELECT
    th_t_id tradeid,
    th_dts ts,
    th_st_id status
  FROM
    ${catalog}.${wh_db}_stage.v_TradeHistory
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
    t_qty quantity,
    t_bid_price bidprice,
    t_ca_id,
    t_exec_name executedby,
    t_trade_price tradeprice,
    t_chrg fee,
    t_comm commission,
    t_tax tax,
    1 batchid
  FROM 
    ${catalog}.${wh_db}_stage.v_Trade t
    JOIN 
      Current_Trades ct
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
  FROM
    TradeIncremental
  WHERE cdc_flag = "I"
)
SELECT
  trade.tradeid,
  sk_brokerid,
  bigint(date_format(create_ts, 'yyyyMMdd')) sk_createdateid,
  bigint(date_format(create_ts, 'HHmmss')) sk_createtimeid,
  bigint(date_format(close_ts, 'yyyyMMdd')) sk_closedateid,
  bigint(date_format(close_ts, 'HHmmss')) sk_closetimeid,
  st_name status,
  tt_name type,
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
JOIN ${catalog}.${wh_db}.StatusType status
  ON status.st_id = trade.status
JOIN ${catalog}.${wh_db}.TradeType tt
  ON tt.tt_id == trade.t_tt_id
JOIN ${catalog}.${wh_db}.DimSecurity ds
  ON 
    ds.symbol = trade.t_s_symb
    AND date(create_ts) >= ds.effectivedate 
    AND date(create_ts) < ds.enddate
JOIN ${catalog}.${wh_db}.DimAccount da
  ON 
    trade.t_ca_id = da.accountid 
    AND date(create_ts) >= da.effectivedate 
    AND date(create_ts) < da.enddate
