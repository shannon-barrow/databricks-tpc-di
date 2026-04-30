-- Databricks notebook source
-- CREATE WIDGET DROPDOWN scale_factor DEFAULT "10" CHOICES SELECT * FROM (VALUES ("10"), ("100"), ("1000"), ("5000"), ("10000"), ("20000"));
-- CREATE WIDGET TEXT tpcdi_directory DEFAULT "/Volumes/tpcdi/tpcdi_raw_data/tpcdi_volume/";
-- CREATE WIDGET TEXT wh_db DEFAULT "";
-- CREATE WIDGET TEXT catalog DEFAULT 'tpcdi';

-- COMMAND ----------

USE IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor);
-- Persist the pre-2015-07-06 cash transaction events as their own staging
-- table. The SDP pipeline's bronzecashtransaction backfill reads from this
-- so its `sum() over (partition by accountid order by ct_date)` running
-- balance starts from the correct historical baseline post-2015-07-06.
-- The temp Delta tpcdi_raw_data.cashtransaction{sf} gets dropped by
-- cleanup_stage0, so this is the persistent copy that survives.
CREATE OR REPLACE TABLE cashtransactionhistorical (
  cdc_flag STRING,
  cdc_dsn BIGINT,
  accountid BIGINT,
  ct_dts TIMESTAMP,
  ct_amt DOUBLE,
  ct_name STRING,
  event_dt DATE
)
PARTITIONED BY (event_dt)
TBLPROPERTIES (
  'delta.autoOptimize.autoCompact' = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true'
);
INSERT OVERWRITE cashtransactionhistorical
SELECT cdc_flag, cdc_dsn, accountid, ct_dts, ct_amt, ct_name,
       to_date(ct_dts) AS event_dt
FROM IDENTIFIER(:catalog || '.tpcdi_raw_data.cashtransaction' || :scale_factor)
WHERE stg_target = 'tables';

CREATE TABLE IF NOT EXISTS factcashbalances (
  sk_customerid BIGINT NOT NULL COMMENT 'Surrogate key for CustomerID',
  sk_accountid BIGINT NOT NULL COMMENT 'Surrogate key for AccountID',
  sk_dateid BIGINT NOT NULL COMMENT 'Surrogate key for the date',
  cash DECIMAL(15,2) COMMENT 'Cash balance for the account after applying',
  CONSTRAINT cashbalances_pk PRIMARY KEY(sk_customerid, sk_accountid, sk_dateid),
  CONSTRAINT cashbalances_customer_fk FOREIGN KEY (sk_customerid) REFERENCES DimCustomer(sk_customerid),
  CONSTRAINT cashbalances_account_fk FOREIGN KEY (sk_accountid) REFERENCES DimAccount(sk_accountid),
  CONSTRAINT cashbalances_date_fk FOREIGN KEY (sk_dateid) REFERENCES DimDate(sk_dateid)
)
PARTITIONED BY (sk_dateid)
TBLPROPERTIES (
  'delta.autoOptimize.autoCompact' = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true'
);
CREATE OR REPLACE TABLE currentaccountbalances (
  ct_date DATE NOT NULL COMMENT 'Date of the latest transactions',
  accountid BIGINT NOT NULL COMMENT 'AccountID',
  current_account_cash DECIMAL(15,2) COMMENT 'Current running cash balance for the account',
  latest_batch BOOLEAN COMMENT 'Accounts with transactions on the latest date processed'
)
PARTITIONED BY (latest_batch)
TBLPROPERTIES (
  'delta.autoOptimize.autoCompact' = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true'
);

-- COMMAND ----------

-- Source: spark-gen temp Delta cashtransaction{sf} (CashTransaction.txt shape, partitioned by stg_target). Filter stg_target='tables' (= ct_dts < 2015-07-06).
INSERT OVERWRITE IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.currentaccountbalances')
SELECT
  *,
  if(ct_date = '2015-07-05', True, False) latest_batch
FROM (
  SELECT
    to_date(max(ct_dts)) ct_date,
    accountid,
    cast(sum(ct_amt) as DECIMAL(15,2)) current_account_cash
  FROM IDENTIFIER(:catalog || '.tpcdi_raw_data.cashtransaction' || :scale_factor)
  WHERE stg_target = 'tables'
  GROUP BY ALL
)

-- COMMAND ----------

INSERT OVERWRITE IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.factcashbalances')
WITH dailycash as (
  SELECT
    * except(account_daily_total),
    sum(c.account_daily_total) OVER (partition by c.accountid order by c.datevalue) cash
  FROM (
    SELECT
      accountid,
      to_date(ct_dts) datevalue,
      cast(sum(ct_amt) as DECIMAL(15,2)) account_daily_total
    FROM IDENTIFIER(:catalog || '.tpcdi_raw_data.cashtransaction' || :scale_factor)
    WHERE stg_target = 'tables'
    GROUP BY ALL
  ) c
)
SELECT 
  a.sk_customerid, 
  a.sk_accountid, 
  bigint(date_format(c.datevalue, 'yyyyMMdd')) sk_dateid,
  c.cash
FROM dailycash c
JOIN IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.dimaccount') a 
  ON 
    c.accountid = a.accountid
    AND c.datevalue >= a.effectivedate 
    AND c.datevalue < a.enddate