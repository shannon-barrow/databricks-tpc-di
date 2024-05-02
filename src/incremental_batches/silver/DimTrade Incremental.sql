-- Databricks notebook source
WITH Trades AS (
  SELECT
    tradeid,
    min(create_ts) create_ts,
    max_by(
      struct(
        close_ts,
        status,
        type,
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
    ) current_record
  FROM
    ${catalog}.${wh_db}_${scale_factor}_stage.TradeIncremental t
  WHERE
    batchid = cast(${batch_id} as int)
  group by
    tradeid
),
TradeIncremental as (
  SELECT
    trade.tradeid,
    sk_brokerid,
    bigint(date_format(create_ts, 'yyyyMMdd')) sk_createdateid,
    bigint(date_format(create_ts, 'HHmmss')) sk_createtimeid,
    bigint(date_format(close_ts, 'yyyyMMdd')) sk_closedateid,
    bigint(date_format(close_ts, 'HHmmss')) sk_closetimeid,
    trade.status,
    trade.type,
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
    cast(${batch_id} as int) batchid
  FROM (
    SELECT
      tradeid,
      create_ts,
      current_record.*
    FROM Trades
  ) trade
  JOIN ${catalog}.${wh_db}_${scale_factor}.DimSecurity ds
    ON 
      ds.symbol = trade.t_s_symb
      AND ds.iscurrent
  JOIN ${catalog}.${wh_db}_${scale_factor}.DimAccount da
    ON 
      trade.t_ca_id = da.accountid 
      AND da.iscurrent
)
MERGE INTO ${catalog}.${wh_db}_${scale_factor}.DimTrade t
USING TradeIncremental s
  ON t.tradeid = s.tradeid
  AND t.sk_closedateid is null
  AND t.sk_closetimeid is null
WHEN MATCHED THEN UPDATE SET
    sk_closedateid = s.sk_closedateid,
    sk_closetimeid = s.sk_closetimeid,
    status = s.status,
    type = s.type,
    cashflag = s.cashflag,
    quantity = s.quantity,
    bidprice = s.bidprice,
    executedby = s.executedby,
    tradeprice = s.tradeprice,
    fee = s.fee,
    commission = s.commission,
    tax = s.tax,
    batchid = s.batchid -- not sure if batchid is supposed to be updated or kept as the original batchid...
WHEN NOT MATCHED THEN INSERT (tradeid, sk_brokerid, sk_createdateid, sk_createtimeid, sk_closedateid, sk_closetimeid, status, type, cashflag, sk_securityid, sk_companyid, quantity, bidprice, sk_customerid, sk_accountid, executedby, tradeprice, fee, commission, tax, batchid)
VALUES (tradeid, sk_brokerid, sk_createdateid, sk_createtimeid, sk_closedateid, sk_closetimeid, status, type, cashflag, sk_securityid, sk_companyid, quantity, bidprice, sk_customerid, sk_accountid, executedby, tradeprice, fee, commission, tax, batchid)

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
  h.batchid
FROM ${catalog}.${wh_db}_${scale_factor}_stage.HoldingIncremental h
  JOIN ${catalog}.${wh_db}_${scale_factor}.DimTrade dt 
    ON 
      dt.batchid = cast(${batch_id} as int)
      and tradeid = hh_t_id
WHERE h.batchid = cast(${batch_id} as int)
