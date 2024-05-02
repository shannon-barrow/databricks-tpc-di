-- Databricks notebook source
-- CREATE WIDGET DROPDOWN wh_timezone DEFAULT "" CHOICES SELECT * FROM (VALUES (""), ("set timezone = GMT;"));
-- CREATE WIDGET DROPDOWN scale_factor DEFAULT "10" CHOICES SELECT * FROM (VALUES ("10"), ("100"), ("1000"), ("5000"), ("10000"));
-- CREATE WIDGET TEXT tpcdi_directory DEFAULT "/Volumes/tpcdi/tpcdi_raw_data/tpcdi_volume/";
-- CREATE WIDGET TEXT wh_db DEFAULT '';
-- CREATE WIDGET TEXT catalog DEFAULT 'tpcdi';

-- COMMAND ----------

-- ONLY use for DBSQL workflows since default for DBSQL is with localization and can cause issues with DST.
-- Pass empty string for a cluster - especially serverless workflows since serverless clusters do not accept this set command and will fail
${wh_timezone} 

-- COMMAND ----------

INSERT OVERWRITE ${catalog}.${wh_db}_${scale_factor}.DimTrade (tradeid, sk_brokerid, sk_createdateid, sk_createtimeid, sk_closedateid, sk_closetimeid, status, type, cashflag, sk_securityid, sk_companyid, quantity, bidprice, sk_customerid, sk_accountid, executedby, tradeprice, fee, commission, tax, batchid)
WITH TradeHistory AS (
  SELECT
    tradeid,
    min(th_dts) create_ts,
    max_by(struct(th_dts, status), th_dts) current_status
  FROM 
    ${catalog}.${wh_db}_${scale_factor}_stage.v_TradeHistory
  group by tradeid
),
Trades as (
  SELECT
    tradeid,
    create_ts,
    CASE
      WHEN current_status.status IN ("CMPT", "CNCL") THEN current_status.th_dts 
      END close_ts,
    current_status.status status,
    if(t_is_cash = 1, TRUE, FALSE) cashflag,
    t_tt_id,
    t_s_symb,
    quantity,
    bidprice,
    t_ca_id,
    executedby,
    tradeprice,
    fee,
    commission,
    tax
  FROM 
    ${catalog}.${wh_db}_${scale_factor}_stage.v_Trade t
  JOIN TradeHistory ct
    ON t.t_id = ct.tradeid
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
  cashflag,
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
  1 batchid
FROM Trades trade
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

-- COMMAND ----------

INSERT INTO ${catalog}.${wh_db}_${scale_factor}.FactHoldings 
SELECT
  hh_h_t_id tradeid,
  hh_t_id currenttradeid,
  sk_customerid,
  sk_accountid,
  sk_securityid,
  sk_companyid,
  sk_closedateid sk_dateid,
  sk_closetimeid sk_timeid,
  tradeprice currentprice,
  hh_after_qty currentholding,
  1 batchid
FROM ${catalog}.${wh_db}_${scale_factor}_stage.v_HoldingHistory h
  JOIN ${catalog}.${wh_db}_${scale_factor}.DimTrade dt 
    ON tradeid = hh_t_id
