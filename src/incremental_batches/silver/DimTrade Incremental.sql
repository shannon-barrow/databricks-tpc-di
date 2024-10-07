-- Databricks notebook source
SET timezone = Etc/UTC;

-- COMMAND ----------

with tradeincrementalraw as (
  SELECT
    tradeid,
    t_dts,
    if(cdc_flag = 'I', t_dts, cast(NULL AS TIMESTAMP)) create_ts,
    if(status IN ("CMPT", "CNCL"), t_dts, cast(NULL AS TIMESTAMP)) close_ts,
    decode(status, 
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
    t_s_symb,
    quantity,
    bidprice,
    t_ca_id,
    executedby,
    tradeprice,
    fee,
    commission,
    tax
  FROM read_files(
    "${tpcdi_directory}sf=${scale_factor}/Batch${batch_id}",
    format => "csv",
    inferSchema => False,
    header => False,
    sep => "|",
    fileNamePattern => "Trade.txt",
    schema => "cdc_flag STRING, cdc_dsn BIGINT, tradeid BIGINT, t_dts TIMESTAMP, status STRING, t_tt_id STRING, cashflag TINYINT, t_s_symb STRING, quantity INT, bidprice DOUBLE, t_ca_id BIGINT, executedby STRING, tradeprice DOUBLE, fee DOUBLE, commission DOUBLE, tax DOUBLE"
  )
),
Trades AS (
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
  FROM tradeincrementalraw t
  group by tradeid
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
    int(${batch_id}) batchid
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
WITH Holdings as (
  SELECT
    * except(cdc_flag, cdc_dsn)
  FROM read_files(
    "${tpcdi_directory}sf=${scale_factor}/Batch${batch_id}",
    format => "csv",
    inferSchema => False,
    header => False,
    sep => "|",
    fileNamePattern => "HoldingHistory.txt",
    schema => "cdc_flag STRING, cdc_dsn BIGINT, hh_h_t_id INT, hh_t_id INT, hh_before_qty INT, hh_after_qty INT"
  )
)
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
  int(${batch_id}) batchid
FROM read_files(
  "${tpcdi_directory}sf=${scale_factor}/Batch${batch_id}",
  format => "csv",
  inferSchema => False,
  header => False,
  sep => "|",
  fileNamePattern => "HoldingHistory.txt",
  schema => "cdc_flag STRING, cdc_dsn BIGINT, hh_h_t_id INT, hh_t_id INT, hh_before_qty INT, hh_after_qty INT"
) h
JOIN ${catalog}.${wh_db}_${scale_factor}.DimTrade dt 
  ON 
    dt.batchid = int(${batch_id})
    and tradeid = hh_t_id
