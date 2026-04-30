-- Databricks notebook source
CREATE OR REFRESH STREAMING TABLE dimcustomer (
  sk_customerid BIGINT COMMENT 'Surrogate key for CustomerID', 
  customerid BIGINT COMMENT 'Customer identifier', 
  taxid STRING COMMENT 'Customer’s tax identifier', 
  status STRING COMMENT 'Customer status type', 
  lastname STRING COMMENT 'Customers last name.', 
  firstname STRING COMMENT 'Customers first name.', 
  middleinitial STRING COMMENT 'Customers middle name initial', 
  gender STRING COMMENT 'Gender of the customer', 
  tier TINYINT COMMENT 'Customer tier', 
  dob DATE COMMENT 'Customer’s date of birth.', 
  addressline1 STRING COMMENT 'Address Line 1', 
  addressline2 STRING COMMENT 'Address Line 2', 
  postalcode STRING COMMENT 'Zip or Postal Code', 
  city STRING COMMENT 'City', 
  stateprov STRING COMMENT 'State or Province', 
  country STRING COMMENT 'Country', 
  phone1 STRING COMMENT 'Phone number 1', 
  phone2 STRING COMMENT 'Phone number 2', 
  phone3 STRING COMMENT 'Phone number 3', 
  email1 STRING COMMENT 'Email address 1', 
  email2 STRING COMMENT 'Email address 2', 
  nationaltaxratedesc STRING COMMENT 'National Tax rate description', 
  nationaltaxrate FLOAT COMMENT 'National Tax rate', 
  localtaxratedesc STRING COMMENT 'Local Tax rate description', 
  localtaxrate FLOAT COMMENT 'Local Tax rate', 
  iscurrent BOOLEAN GENERATED ALWAYS AS (nvl2(__END_AT, false, true)) COMMENT 'True if this is the current record', 
  effectivedate DATE COMMENT 'Beginning of date range when this record was the current record', 
  enddate DATE GENERATED ALWAYS AS (nvl(__END_AT, date('9999-12-31'))) COMMENT 'Ending of date range when this record was the current record. A record that is not expired will use the date 9999-12-31.', 
  __START_AT DATE COMMENT 'Beginning of date range when this record was the current record', 
  __END_AT DATE COMMENT 'Ending of date range when this record was the current record.'
)
PARTITIONED BY (iscurrent);

-- COMMAND ----------

CREATE FLOW dimcustomer_historical
COMMENT "Backfill historical DimCustomer table from staging table"
AS INSERT INTO ONCE dimcustomer BY NAME
SELECT
  sk_customerid, 
  customerid, 
  taxid, 
  status,
  lastname,
  firstname,
  middleinitial,
  gender,
  tier,
  dob,
  addressline1,
  addressline2,
  postalcode,
  city,
  stateprov,
  country,
  phone1,
  phone2,
  phone3,
  email1,
  email2,
  nationaltaxratedesc,
  nationaltaxrate,
  localtaxratedesc,
  localtaxrate,
  iscurrent,
  effectivedate,
  enddate,
  effectivedate __START_AT,
  nullif(enddate, date('9999-12-31')) __END_AT
FROM tpcdi_incremental_staging_${scale_factor}.dimcustomer;

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE dimaccount (
  sk_accountid BIGINT COMMENT 'Surrogate key for AccountID', 
  accountid BIGINT COMMENT 'Customer account identifier', 
  sk_brokerid BIGINT COMMENT 'Surrogate key of managing broker', 
  sk_customerid BIGINT COMMENT 'Surrogate key of customer', 
  customerid BIGINT COMMENT 'Customer identifier',
  accountdesc STRING COMMENT 'Name of customer account', 
  taxstatus TINYINT COMMENT 'Tax status of this account', 
  status STRING COMMENT 'Account status, active or closed', 
  iscurrent BOOLEAN GENERATED ALWAYS AS (nvl2(__END_AT, false, true)) COMMENT 'True if this is the current record', 
  effectivedate DATE COMMENT 'Beginning of date range when this record was the current record', 
  enddate DATE GENERATED ALWAYS AS (nvl(__END_AT, date('9999-12-31'))) COMMENT 'Ending of date range when this record was the current record. A record that is not expired will use the date 9999-12-31.', 
  __START_AT DATE COMMENT 'Beginning of date range when this record was the current record', 
  __END_AT DATE COMMENT 'Ending of date range when this record was the current record.'
)
PARTITIONED BY (iscurrent);

-- COMMAND ----------

CREATE FLOW dimaccount_historical
COMMENT "Backfill historical DimAccount table from staging table"
AS INSERT INTO ONCE dimaccount BY NAME
SELECT
  sk_accountid, 
  accountid, 
  sk_brokerid, 
  sk_customerid, 
  BIGINT(substring(cast(sk_customerid as string), 9)) customerid,
  accountdesc, 
  TaxStatus, 
  status, 
  iscurrent, 
  effectivedate, 
  enddate,
  effectivedate __START_AT,
  nullif(enddate, date('9999-12-31')) __END_AT
FROM tpcdi_incremental_staging_${scale_factor}.dimaccount;

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE dimtrade (
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
  tax DOUBLE COMMENT 'Amount of tax due on this trade'
)
PARTITIONED BY (sk_closedateid);

-- COMMAND ----------

CREATE FLOW dimtrade_historical
COMMENT "Backfill historical DimTrade table from staging table"
AS INSERT INTO ONCE dimtrade BY NAME
SELECT
  tradeid,
  sk_brokerid,
  sk_createdateid,
  sk_createtimeid,
  sk_closedateid,
  sk_closetimeid,
  status,
  type,
  cashflag,
  sk_securityid,
  sk_companyid,
  quantity,
  bidprice,
  sk_customerid,
  sk_accountid,
  executedby,
  tradeprice,
  fee,
  commission,
  tax
FROM tpcdi_incremental_staging_${scale_factor}.dimtrade;

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE factholdings (
  tradeid BIGINT COMMENT 'Key for Orignial Trade Indentifier',
  currenttradeid BIGINT COMMENT 'Key for the current trade',
  sk_customerid BIGINT COMMENT 'Surrogate key for Customer Identifier',
  sk_accountid BIGINT COMMENT 'Surrogate key for Account Identifier',
  sk_securityid BIGINT COMMENT 'Surrogate key for Security Identifier',
  sk_companyid BIGINT COMMENT 'Surrogate key for Company Identifier',
  sk_dateid BIGINT COMMENT 'Surrogate key for the date associated with the current trade',
  sk_timeid BIGINT COMMENT 'Surrogate key for the time associated with the current trade',
  currentprice DOUBLE COMMENT 'Unit price of this security for the current trade',
  currentholding INT COMMENT 'Quantity of a security held after the current trade.'
)
PARTITIONED BY (sk_dateid);

-- COMMAND ----------

CREATE FLOW factholdings_historical
COMMENT "Backfill historical factholdings table from staging table"
AS INSERT INTO ONCE factholdings BY NAME
SELECT
  tradeid,
  currenttradeid,
  sk_customerid,
  sk_accountid,
  sk_securityid,
  sk_companyid,
  sk_dateid,
  sk_timeid,
  currentprice,
  currentholding
FROM tpcdi_incremental_staging_${scale_factor}.factholdings;

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE factwatches (
  sk_customerid BIGINT COMMENT 'Surrogate Key of the customer associated with watch list',
  sk_securityid BIGINT COMMENT 'Surrogate Key of the security listed on watch list',
  customerid BIGINT COMMENT 'Customer associated with watch list',
  symbol STRING COMMENT 'Security listed on watch list',
  sk_dateid_dateplaced BIGINT COMMENT 'Date the watch list item was added',
  sk_dateid_dateremoved BIGINT COMMENT 'Date the watch list item was removed',
  removed BOOLEAN COMMENT 'True if this watch has been removed'
)
PARTITIONED BY (removed);

-- COMMAND ----------

CREATE FLOW factwatches_historical
COMMENT "Backfill historical factwatches table from staging table"
AS INSERT INTO ONCE factwatches BY NAME
SELECT
  sk_customerid, 
  sk_securityid, 
  customerid, 
  symbol, 
  sk_dateid_dateplaced, 
  sk_dateid_dateremoved, 
  removed
FROM tpcdi_incremental_staging_${scale_factor}.factwatches;