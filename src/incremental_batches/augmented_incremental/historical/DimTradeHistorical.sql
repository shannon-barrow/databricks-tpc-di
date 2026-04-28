-- Databricks notebook source
-- CREATE WIDGET DROPDOWN scale_factor DEFAULT "10" CHOICES SELECT * FROM (VALUES ("10"), ("100"), ("1000"), ("5000"), ("10000"), ("20000"));
-- CREATE WIDGET TEXT tpcdi_directory DEFAULT "/Volumes/tpcdi/tpcdi_raw_data/tpcdi_volume/";
-- CREATE WIDGET TEXT wh_db DEFAULT "";
-- CREATE WIDGET TEXT catalog DEFAULT 'tpcdi';

-- COMMAND ----------

USE IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor);
CREATE OR REPLACE TABLE DimTrade (
  tradeid BIGINT NOT NULL COMMENT 'Trade identifier',
  sk_brokerid BIGINT COMMENT 'Surrogate key for BrokerID',
  sk_createdateid BIGINT COMMENT 'Surrogate key for date created',
  sk_createtimeid BIGINT COMMENT 'Surrogate key for time created',
  sk_closedateid BIGINT COMMENT 'Surrogate key for date closed',
  sk_closetimeid BIGINT COMMENT 'Surrogate key for time closed',
  status STRING COMMENT 'Trade status',
  type STRING COMMENT 'Trade type',
  cashflag BOOLEAN COMMENT 'Is this trade a cash or margin trade?',
  sk_securityid BIGINT COMMENT 'Surrogate key for SecurityID',
  sk_companyid BIGINT COMMENT 'Surrogate key for CompanyID',
  quantity INT COMMENT 'Quantity of securities traded.',
  bidprice DOUBLE COMMENT 'The requested unit price.',
  sk_customerid BIGINT COMMENT 'Surrogate key for CustomerID',
  sk_accountid BIGINT COMMENT 'Surrogate key for AccountID',
  executedby STRING COMMENT 'Name of person executing the trade.',
  tradeprice DOUBLE COMMENT 'Unit price at which the security was traded.',
  fee DOUBLE COMMENT 'Fee charged for placing this trade request',
  commission DOUBLE COMMENT 'Commission earned on this trade',
  tax DOUBLE COMMENT 'Amount of tax due on this trade',
  CONSTRAINT dimtrade_pk PRIMARY KEY(tradeid),
  CONSTRAINT dimtrade_security_fk FOREIGN KEY (sk_securityid) REFERENCES DimSecurity(sk_securityid),
  CONSTRAINT dimtrade_company_fk FOREIGN KEY (sk_companyid) REFERENCES DimCompany(sk_companyid),
  CONSTRAINT dimtrade_broker_fk FOREIGN KEY (sk_brokerid) REFERENCES DimBroker(sk_brokerid),
  CONSTRAINT dimtrade_account_fk FOREIGN KEY (sk_accountid) REFERENCES DimAccount(sk_accountid),
  CONSTRAINT dimtrade_customer_fk FOREIGN KEY (sk_customerid) REFERENCES DimCustomer(sk_customerid),
  CONSTRAINT dimtrade_createdate_fk FOREIGN KEY (sk_createdateid) REFERENCES DimDate(sk_dateid),
  CONSTRAINT dimtrade_closedate_fk FOREIGN KEY (sk_closedateid) REFERENCES DimDate(sk_dateid),
  CONSTRAINT dimtrade_createtime_fk FOREIGN KEY (sk_createtimeid) REFERENCES DimTime(sk_timeid),
  CONSTRAINT dimtrade_closetime_fk FOREIGN KEY (sk_closetimeid) REFERENCES DimTime(sk_timeid)
) 
PARTITIONED BY (sk_closedateid)
TBLPROPERTIES (
  'delta.autoOptimize.autoCompact' = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true'
);

-- COMMAND ----------

INSERT OVERWRITE IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimTrade')
with rawtrade as (
  SELECT
    tradeid,
    t_dts,
    min(case when cdc_flag = 'I' then t_dts end) over (partition by tradeid) create_ts,
    case when status IN ("CMPT", "CNCL") then t_dts end close_ts,
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
  FROM IDENTIFIER(:catalog || '.tpcdi_raw_data.rawtrade' || :scale_factor) a
  WHERE event_dt < '2015-07-06'
  QUALIFY row_number() over (partition by tradeid order by t_dts desc) = 1
) 
SELECT
  trade.tradeid,
  da.sk_brokerid,
  bigint(date_format(trade.create_ts, 'yyyyMMdd')) sk_createdateid,
  bigint(date_format(trade.create_ts, 'HHmmss')) sk_createtimeid,
  bigint(date_format(trade.close_ts, 'yyyyMMdd')) sk_closedateid,
  bigint(date_format(trade.close_ts, 'HHmmss')) sk_closetimeid,
  trade.status,
  trade.type,
  trade.cashflag,
  ds.sk_securityid,
  ds.sk_companyid,
  trade.quantity,
  trade.bidprice,
  da.sk_customerid,
  da.sk_accountid,
  trade.executedby,
  trade.tradeprice,
  trade.fee,
  trade.commission,
  trade.tax
FROM rawtrade trade
JOIN IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimSecurity') ds
  ON 
    ds.symbol = trade.t_s_symb
    AND date(create_ts) >= ds.effectivedate 
    AND date(create_ts) < ds.enddate
JOIN IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimAccount') da
  ON 
    trade.t_ca_id = da.accountid 
    AND date(create_ts) >= da.effectivedate 
    AND date(create_ts) < da.enddate