-- Databricks notebook source
-- CREATE WIDGET DROPDOWN scale_factor DEFAULT "10" CHOICES SELECT * FROM (VALUES ("10"), ("100"), ("1000"), ("5000"), ("10000"), ("20000"));
-- CREATE WIDGET TEXT tpcdi_directory DEFAULT "/Volumes/tpcdi/tpcdi_raw_data/tpcdi_volume/";
-- CREATE WIDGET TEXT wh_db DEFAULT "";
-- CREATE WIDGET TEXT catalog DEFAULT 'tpcdi';

-- COMMAND ----------

USE IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor);
CREATE OR REPLACE TABLE DimAccount (
  sk_accountid BIGINT NOT NULL COMMENT 'Surrogate key for AccountID',
  accountid BIGINT COMMENT 'Customer account identifier',
  sk_brokerid BIGINT COMMENT 'Surrogate key of managing broker',
  sk_customerid BIGINT COMMENT 'Surrogate key of customer',
  accountdesc STRING COMMENT 'Name of customer account',
  taxstatus TINYINT COMMENT 'Tax status of this account',
  status STRING COMMENT 'Account status, active or closed',
  effectivedate DATE COMMENT 'Beginning of date range when this record was the current record',
  enddate DATE COMMENT 'Ending of date range when this record was the current record. A record that is not expired will use the date 9999-12-31.',
  iscurrent BOOLEAN COMMENT 'True if this is the current record',
  CONSTRAINT dimaccount_pk PRIMARY KEY(sk_accountid),
  CONSTRAINT dimaccount_customer_fk FOREIGN KEY (sk_customerid) REFERENCES DimCustomer(sk_customerid),
  CONSTRAINT dimaccount_broker_fk FOREIGN KEY (sk_brokerid) REFERENCES  DimBroker(sk_brokerid)
) 
PARTITIONED BY (iscurrent)
TBLPROPERTIES (
  'delta.autoOptimize.autoCompact' = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true'
);

-- COMMAND ----------

-- Source: spark-gen temp Delta tpcdi_raw_data.customermgmt{sf}. Account-touching ActionTypes are NEW / ADDACCT / UPDACCT / CLOSEACCT (NOT UPDCUST or INACT — those don't carry account fields). Status is already decoded; rows are dense, so the windowed last_value IGNORE NULLS pass present in the DIGen splitter version is unnecessary.
INSERT OVERWRITE IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimAccount')
WITH acct_updates AS (
  SELECT
    accountid,
    customerid,
    accountdesc,
    taxstatus,
    brokerid,
    CASE
      WHEN ActionType IN ('NEW', 'ADDACCT', 'UPDACCT', 'UPDCUST') THEN 'Active'
      WHEN ActionType IN ('CLOSEACCT', 'INACT') THEN 'Inactive'
    END AS status,
    date(update_ts) effectivedate,
    nvl(
      lead(date(update_ts)) OVER (PARTITION BY accountid ORDER BY update_ts),
      date('9999-12-31')
    ) enddate,
    row_number() OVER (PARTITION BY accountid, date(update_ts) ORDER BY update_ts DESC) rn
  FROM IDENTIFIER(:catalog || '.tpcdi_raw_data.customermgmt' || :scale_factor)
  WHERE stg_target = 'tables'
    AND ActionType IN ('NEW', 'ADDACCT', 'UPDACCT', 'CLOSEACCT')
),
acct_updates_dedup AS (
  SELECT * EXCEPT(rn) FROM acct_updates WHERE rn = 1
),
add_cust_updates AS (
  SELECT
    a.* except(effectivedate, enddate, customerid),
    c.sk_customerid,
    if(
      a.effectivedate < c.effectivedate,
      c.effectivedate,
      a.effectivedate
    ) effectivedate,
    if(a.enddate > c.enddate, c.enddate, a.enddate) enddate
  FROM acct_updates_dedup a
  FULL OUTER JOIN IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimCustomer') c
    ON 
      a.customerid = c.customerid
      AND c.enddate > a.effectivedate
      AND c.effectivedate < a.enddate
)
SELECT
  bigint(concat(date_format(a.effectivedate, 'yyyyMMdd'), a.accountid)) sk_accountid,
  a.accountid,
  b.sk_brokerid,
  a.sk_customerid,
  a.accountdesc,
  a.TaxStatus,
  a.status,
  a.effectivedate,
  a.enddate,
  if(a.enddate = date('9999-12-31'), true, false) iscurrent
FROM add_cust_updates a
JOIN IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimBroker') b 
  ON a.brokerid = b.brokerid