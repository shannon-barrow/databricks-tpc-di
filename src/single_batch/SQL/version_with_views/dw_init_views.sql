-- Databricks notebook source
-- CREATE WIDGET DROPDOWN scale_factor DEFAULT "10" CHOICES SELECT * FROM (VALUES ("10"), ("100"), ("1000"), ("5000"), ("10000"));
-- CREATE WIDGET TEXT tpcdi_directory DEFAULT "/Volumes/tpcdi/tpcdi_raw_data/tpcdi_volume/";
-- CREATE WIDGET TEXT wh_db DEFAULT '';
-- CREATE WIDGET TEXT catalog DEFAULT 'tpcdi';
-- CREATE WIDGET DROPDOWN pred_opt DEFAULT "DISABLE" CHOICES SELECT * FROM (VALUES ("ENABLE"), ("DISABLE")); -- Predictive Optimization

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Reset/Create Catalog and Database

-- COMMAND ----------

-- Enable Predictive Optimization for those workspaces that it is available
ALTER DATABASE ${catalog}.${wh_db}_${scale_factor} ${pred_opt} PREDICTIVE OPTIMIZATION;
SET timezone = Etc/UTC;
DROP DATABASE IF EXISTS ${catalog}.${wh_db}_${scale_factor} cascade;
CREATE DATABASE ${catalog}.${wh_db}_${scale_factor};
CREATE DATABASE IF NOT EXISTS ${catalog}.${wh_db}_${scale_factor}_stage;
DROP TABLE IF EXISTS ${catalog}.${wh_db}_${scale_factor}_stage.ProspectIncremental;
DROP TABLE IF EXISTS ${catalog}.${wh_db}_${scale_factor}_stage.finwire;

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Create Empty Tables

-- COMMAND ----------

CREATE OR REPLACE TABLE ${catalog}.${wh_db}_${scale_factor}_stage.FinWire (
  rectype STRING COMMENT 'Indicates the type of table into which this record will eventually be parsed: CMP FIN or SEC',
  recdate date COMMENT 'Date of the record',
  value STRING COMMENT 'Pre-parsed String Values of all FinWire files'
) 
PARTITIONED BY (rectype)
TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = 0, 'delta.autoOptimize.autoCompact'=False, 'delta.autoOptimize.optimizeWrite'=True);

-- COMMAND ----------

CREATE OR REPLACE TABLE ${catalog}.${wh_db}_${scale_factor}_stage.ProspectIncremental (
  agencyid STRING COMMENT 'Unique identifier from agency',
  lastname STRING COMMENT 'Last name',
  firstname STRING COMMENT 'First name',
  middleinitial STRING COMMENT 'Middle initial',
  gender STRING COMMENT '‘M’ or ‘F’ or ‘U’',
  addressline1 STRING COMMENT 'Postal address',
  addressline2 STRING COMMENT 'Postal address',
  postalcode STRING COMMENT 'Postal code',
  city STRING COMMENT 'City',
  state STRING COMMENT 'State or province',
  country STRING COMMENT 'Postal country',
  phone STRING COMMENT 'Telephone number',
  income STRING COMMENT 'Annual income',
  numbercars INT COMMENT 'Cars owned',
  numberchildren INT COMMENT 'Dependent children',
  maritalstatus STRING COMMENT '‘S’ or ‘M’ or ‘D’ or ‘W’ or ‘U’',
  age INT COMMENT 'Current age',
  creditrating INT COMMENT 'Numeric rating',
  ownorrentflag STRING COMMENT '‘O’ or ‘R’ or ‘U’',
  employer STRING COMMENT 'Name of employer',
  numbercreditcards INT COMMENT 'Credit cards',
  networth INT COMMENT 'Estimated total net worth',
  marketingnameplate STRING COMMENT 'Marketing nameplate',
  recordbatchid INT NOT NULL COMMENT 'Batch ID when this record last inserted',
  batchid INT NOT NULL COMMENT 'Batch ID when this record was initially inserted'
)
TBLPROPERTIES ('delta.autoOptimize.autoCompact'=False, 'delta.autoOptimize.optimizeWrite'=True);

-- COMMAND ----------

CREATE OR REPLACE TABLE ${catalog}.${wh_db}_${scale_factor}.TaxRate (
  tx_id STRING NOT NULL COMMENT 'Tax rate code',
  tx_name STRING NOT NULL COMMENT 'Tax rate description',
  tx_rate FLOAT NOT NULL COMMENT 'Tax rate',
  CONSTRAINT taxrate_pk PRIMARY KEY(tx_id)
)
TBLPROPERTIES ('delta.autoOptimize.autoCompact'=False, 'delta.autoOptimize.optimizeWrite'=True);

-- COMMAND ----------

CREATE OR REPLACE TABLE ${catalog}.${wh_db}_${scale_factor}.BatchDate (
  batchdate DATE NOT NULL COMMENT 'Batch date',
  batchid INT NOT NULL COMMENT 'Batch ID when this record was inserted',
  CONSTRAINT batchdate_pk PRIMARY KEY(batchdate)
)
TBLPROPERTIES ('delta.autoOptimize.autoCompact'=False, 'delta.autoOptimize.optimizeWrite'=True);

-- COMMAND ----------

CREATE OR REPLACE TABLE ${catalog}.${wh_db}_${scale_factor}.DimDate (
  sk_dateid BIGINT NOT NULL COMMENT 'Surrogate key for the date',
  datevalue DATE NOT NULL COMMENT 'The date stored appropriately for doing comparisons in the Data Warehouse',
  datedesc STRING NOT NULL COMMENT 'The date in full written form e.g. July 7 2004',
  calendaryearid INT NOT NULL COMMENT 'Year number as a number',
  calendaryeardesc STRING NOT NULL COMMENT 'Year number as text',
  calendarqtrid INT NOT NULL COMMENT 'Quarter as a number e.g. 20042',
  calendarqtrdesc STRING NOT NULL COMMENT 'Quarter as text e.g. 2004 Q2',
  calendarmonthid INT NOT NULL COMMENT 'Month as a number e.g. 20047',
  calendarmonthdesc STRING NOT NULL COMMENT 'Month as text e.g. 2004 July',
  calendarweekid INT NOT NULL COMMENT 'Week as a number e.g. 200428',
  calendarweekdesc STRING NOT NULL COMMENT 'Week as text e.g. 2004-W28',
  dayofweeknum INT NOT NULL COMMENT 'Day of week as a number e.g. 3',
  dayofweekdesc STRING NOT NULL COMMENT 'Day of week as text e.g. Wednesday',
  fiscalyearid INT NOT NULL COMMENT 'Fiscal year as a number e.g. 2005',
  fiscalyeardesc STRING NOT NULL COMMENT 'Fiscal year as text e.g. 2005',
  fiscalqtrid INT NOT NULL COMMENT 'Fiscal quarter as a number e.g. 20051',
  fiscalqtrdesc STRING NOT NULL COMMENT 'Fiscal quarter as text e.g. 2005 Q1',
  holidayflag BOOLEAN COMMENT 'Indicates holidays',
  CONSTRAINT dimdate_pk PRIMARY KEY(sk_dateid)
)
TBLPROPERTIES ('delta.autoOptimize.autoCompact'=False, 'delta.autoOptimize.optimizeWrite'=True);

-- COMMAND ----------

CREATE OR REPLACE TABLE ${catalog}.${wh_db}_${scale_factor}.DimTime (
  sk_timeid BIGINT NOT NULL COMMENT 'Surrogate key for the time',
  timevalue STRING NOT NULL COMMENT 'The time stored appropriately for doing',
  hourid INT NOT NULL COMMENT 'Hour number as a number e.g. 01',
  hourdesc STRING NOT NULL COMMENT 'Hour number as text e.g. 01',
  minuteid INT NOT NULL COMMENT 'Minute as a number e.g. 23',
  minutedesc STRING NOT NULL COMMENT 'Minute as text e.g. 01:23',
  secondid INT NOT NULL COMMENT 'Second as a number e.g. 45',
  seconddesc STRING NOT NULL COMMENT 'Second as text e.g. 01:23:45',
  markethoursflag BOOLEAN COMMENT 'Indicates a time during market hours',
  officehoursflag BOOLEAN COMMENT 'Indicates a time during office hours',
  CONSTRAINT dimtime_pk PRIMARY KEY(sk_timeid)
)
TBLPROPERTIES ('delta.autoOptimize.autoCompact'=False, 'delta.autoOptimize.optimizeWrite'=True);

-- COMMAND ----------

CREATE OR REPLACE TABLE ${catalog}.${wh_db}_${scale_factor}.StatusType (
  st_id STRING NOT NULL COMMENT 'Status code',
  st_name STRING NOT NULL COMMENT 'Status description',
  CONSTRAINT statustype_pk PRIMARY KEY(st_name)
)
TBLPROPERTIES ('delta.autoOptimize.autoCompact'=False, 'delta.autoOptimize.optimizeWrite'=True);

-- COMMAND ----------

CREATE OR REPLACE TABLE ${catalog}.${wh_db}_${scale_factor}.industry (
  in_id STRING NOT NULL COMMENT 'Industry code',
  in_name STRING NOT NULL COMMENT 'Industry description',
  in_sc_id STRING NOT NULL COMMENT 'Sector identifier',
  CONSTRAINT industry_pk PRIMARY KEY(in_name)
)
TBLPROPERTIES ('delta.autoOptimize.autoCompact'=False, 'delta.autoOptimize.optimizeWrite'=True);

-- COMMAND ----------

CREATE OR REPLACE TABLE ${catalog}.${wh_db}_${scale_factor}.TradeType (
  tt_id STRING NOT NULL COMMENT 'Trade type code',
  tt_name STRING NOT NULL COMMENT 'Trade type description',
  tt_is_sell INT NOT NULL COMMENT 'Flag indicating a sale',
  tt_is_mrkt INT NOT NULL COMMENT 'Flag indicating a market order',
  CONSTRAINT tradetype_pk PRIMARY KEY(tt_id)
)
TBLPROPERTIES ('delta.autoOptimize.autoCompact'=False, 'delta.autoOptimize.optimizeWrite'=True);

-- COMMAND ----------

CREATE OR REPLACE TABLE ${catalog}.${wh_db}_${scale_factor}.DimBroker (
  sk_brokerid BIGINT NOT NULL COMMENT 'Surrogate key for broker',
  brokerid BIGINT NOT NULL COMMENT 'Natural key for broker',
  managerid BIGINT COMMENT 'Natural key for manager’s HR record',
  firstname STRING NOT NULL COMMENT 'First name',
  lastname STRING NOT NULL COMMENT 'Last Name',
  middleinitial STRING COMMENT 'Middle initial',
  branch STRING COMMENT 'Facility in which employee has office',
  office STRING COMMENT 'Office number or description',
  phone STRING COMMENT 'Employee phone number',
  iscurrent BOOLEAN NOT NULL COMMENT 'True if this is the current record',
  batchid INT NOT NULL COMMENT 'Batch ID when this record was inserted',
  effectivedate DATE NOT NULL COMMENT 'Beginning of date range when this record was the current record',
  enddate DATE NOT NULL COMMENT 'Ending of date range when this record was the current record. A record that is not expired will use the date 9999-12-31.',
  CONSTRAINT dimbroker_pk PRIMARY KEY(sk_brokerid)
)
TBLPROPERTIES ('delta.autoOptimize.autoCompact'=False, 'delta.autoOptimize.optimizeWrite'=True);

-- COMMAND ----------

CREATE OR REPLACE TABLE ${catalog}.${wh_db}_${scale_factor}.DimCustomer (
  sk_customerid BIGINT NOT NULL COMMENT 'Surrogate key for CustomerID',
  customerid BIGINT NOT NULL COMMENT 'Customer identifier',
  taxid STRING NOT NULL COMMENT 'Customer’s tax identifier',
  status STRING NOT NULL COMMENT 'Customer status type',
  lastname STRING NOT NULL COMMENT 'Customers last name.',
  firstname STRING NOT NULL COMMENT 'Customers first name.',
  middleinitial STRING COMMENT 'Customers middle name initial',
  gender STRING COMMENT 'Gender of the customer',
  tier TINYINT COMMENT 'Customer tier',
  dob DATE NOT NULL COMMENT 'Customer’s date of birth.',
  addressline1 STRING NOT NULL COMMENT 'Address Line 1',
  addressline2 STRING COMMENT 'Address Line 2',
  postalcode STRING NOT NULL COMMENT 'Zip or Postal Code',
  city STRING NOT NULL COMMENT 'City',
  stateprov STRING NOT NULL COMMENT 'State or Province',
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
  agencyid STRING COMMENT 'Agency identifier',
  creditrating INT COMMENT 'Credit rating',
  networth INT COMMENT 'Net worth',
  marketingnameplate STRING COMMENT 'Marketing nameplate',
  iscurrent BOOLEAN NOT NULL COMMENT 'True if this is the current record',
  batchid INT NOT NULL COMMENT 'Batch ID when this record was inserted',
  effectivedate DATE NOT NULL COMMENT 'Beginning of date range when this record was the current record',
  enddate DATE NOT NULL COMMENT 'Ending of date range when this record was the current record. A record that is not expired will use the date 9999-12-31.',
  CONSTRAINT dimcustomer_pk PRIMARY KEY(sk_customerid)
) 
TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = 33, 'delta.autoOptimize.autoCompact'=False, 'delta.autoOptimize.optimizeWrite'=True);

-- COMMAND ----------

CREATE OR REPLACE TABLE ${catalog}.${wh_db}_${scale_factor}.DimCompany (
  sk_companyid BIGINT NOT NULL COMMENT 'Surrogate key for CompanyID',
  companyid BIGINT NOT NULL COMMENT 'Company identifier (CIK number)',
  status STRING NOT NULL COMMENT 'Company status',
  name STRING NOT NULL COMMENT 'Company name',
  industry STRING NOT NULL COMMENT 'Company’s industry',
  sprating STRING COMMENT 'Standard & Poor company’s rating',
  islowgrade BOOLEAN COMMENT 'True if this company is low grade',
  ceo STRING NOT NULL COMMENT 'CEO name',
  addressline1 STRING COMMENT 'Address Line 1',
  addressline2 STRING COMMENT 'Address Line 2',
  postalcode STRING NOT NULL COMMENT 'Zip or postal code',
  city STRING NOT NULL COMMENT 'City',
  stateprov STRING NOT NULL COMMENT 'State or Province',
  country STRING COMMENT 'Country',
  description STRING NOT NULL COMMENT 'Company description',
  foundingdate DATE COMMENT 'Date the company was founded',
  iscurrent BOOLEAN NOT NULL COMMENT 'True if this is the current record',
  batchid INT NOT NULL COMMENT 'Batch ID when this record was inserted',
  effectivedate DATE NOT NULL COMMENT 'Beginning of date range when this record was the current record',
  enddate DATE NOT NULL COMMENT 'Ending of date range when this record was the current record. A record that is not expired will use the date 9999-12-31.',
  CONSTRAINT dimcompany_pk PRIMARY KEY(sk_companyid),
  CONSTRAINT dimcompany_status_fk FOREIGN KEY (status) REFERENCES ${catalog}.${wh_db}_${scale_factor}.StatusType(st_name),
  CONSTRAINT dimcompany_industry_fk FOREIGN KEY (industry) REFERENCES ${catalog}.${wh_db}_${scale_factor}.Industry(in_name)
)
TBLPROPERTIES ('delta.autoOptimize.autoCompact'=False, 'delta.autoOptimize.optimizeWrite'=True);

-- COMMAND ----------

CREATE OR REPLACE TABLE ${catalog}.${wh_db}_${scale_factor}.DimAccount (
  sk_accountid BIGINT NOT NULL COMMENT 'Surrogate key for AccountID',
  accountid BIGINT NOT NULL COMMENT 'Customer account identifier',
  sk_brokerid BIGINT NOT NULL COMMENT 'Surrogate key of managing broker',
  sk_customerid BIGINT NOT NULL COMMENT 'Surrogate key of customer',
  accountdesc STRING COMMENT 'Name of customer account',
  taxstatus TINYINT COMMENT 'Tax status of this account',
  status STRING NOT NULL COMMENT 'Account status, active or closed',
  iscurrent BOOLEAN NOT NULL COMMENT 'True if this is the current record',
  batchid INT NOT NULL COMMENT 'Batch ID when this record was inserted',
  effectivedate DATE NOT NULL COMMENT 'Beginning of date range when this record was the current record',
  enddate DATE NOT NULL COMMENT 'Ending of date range when this record was the current record. A record that is not expired will use the date 9999-12-31.',
  CONSTRAINT dimaccount_pk PRIMARY KEY(sk_accountid),
  CONSTRAINT dimaccount_customer_fk FOREIGN KEY (sk_customerid) REFERENCES ${catalog}.${wh_db}_${scale_factor}.DimCustomer(sk_customerid),
  CONSTRAINT dimaccount_broker_fk FOREIGN KEY (sk_brokerid) REFERENCES ${catalog}.${wh_db}_${scale_factor}.DimBroker(sk_brokerid)
) 
TBLPROPERTIES ('delta.autoOptimize.autoCompact'=False, 'delta.autoOptimize.optimizeWrite'=True);

-- COMMAND ----------

CREATE OR REPLACE TABLE ${catalog}.${wh_db}_${scale_factor}.DimSecurity (
  sk_securityid BIGINT NOT NULL COMMENT 'Surrogate key for Symbol',
  symbol STRING NOT NULL COMMENT 'Identifies security on ticker',
  issue STRING NOT NULL COMMENT 'Issue type',
  status STRING NOT NULL COMMENT 'Status type',
  name STRING NOT NULL COMMENT 'Security name',
  exchangeid STRING NOT NULL COMMENT 'Exchange the security is traded on',
  sk_companyid BIGINT NOT NULL COMMENT 'Company issuing security',
  sharesoutstanding BIGINT NOT NULL COMMENT 'Shares outstanding',
  firsttrade DATE NOT NULL COMMENT 'Date of first trade',
  firsttradeonexchange DATE NOT NULL COMMENT 'Date of first trade on this exchange',
  dividend DOUBLE NOT NULL COMMENT 'Annual dividend per share',
  iscurrent BOOLEAN NOT NULL COMMENT 'True if this is the current record',
  batchid INT NOT NULL COMMENT 'Batch ID when this record was inserted',
  effectivedate DATE NOT NULL COMMENT 'Beginning of date range when this record was the current record',
  enddate DATE NOT NULL COMMENT 'Ending of date range when this record was the current record. A record that is not expired will use the date 9999-12-31.',
  CONSTRAINT dimsecurity_pk PRIMARY KEY(sk_securityid),
  CONSTRAINT dimsecurity_status_fk FOREIGN KEY (status) REFERENCES ${catalog}.${wh_db}_${scale_factor}.StatusType(st_name),
  CONSTRAINT dimsecurity_company_fk FOREIGN KEY (sk_companyid) REFERENCES ${catalog}.${wh_db}_${scale_factor}.DimCompany(sk_companyid)
) 
TBLPROPERTIES ('delta.autoOptimize.autoCompact'=False, 'delta.autoOptimize.optimizeWrite'=True);

-- COMMAND ----------

CREATE OR REPLACE TABLE ${catalog}.${wh_db}_${scale_factor}.Prospect (
  agencyid STRING NOT NULL COMMENT 'Unique identifier from agency',
  sk_recorddateid BIGINT NOT NULL COMMENT 'Last date this prospect appeared in input',
  sk_updatedateid BIGINT NOT NULL COMMENT 'Latest change date for this prospect',
  batchid INT NOT NULL COMMENT 'Batch ID when this record was last modified',
  iscustomer BOOLEAN NOT NULL COMMENT 'True if this person is also in DimCustomer,else False',
  lastname STRING NOT NULL COMMENT 'Last name',
  firstname STRING NOT NULL COMMENT 'First name',
  middleinitial STRING COMMENT 'Middle initial',
  gender STRING COMMENT 'M / F / U',
  addressline1 STRING COMMENT 'Postal address',
  addressline2 STRING COMMENT 'Postal address',
  postalcode STRING COMMENT 'Postal code',
  city STRING NOT NULL COMMENT 'City',
  state STRING NOT NULL COMMENT 'State or province',
  country STRING COMMENT 'Postal country',
  phone STRING COMMENT 'Telephone number',
  income STRING COMMENT 'Annual income',
  numbercars INT COMMENT 'Cars owned',
  numberchildren INT COMMENT 'Dependent children',
  maritalstatus STRING COMMENT 'S / M / D / W / U',
  age INT COMMENT 'Current age',
  creditrating INT COMMENT 'Numeric rating',
  ownorrentflag STRING COMMENT 'O / R / U',
  employer STRING COMMENT 'Name of employer',
  numbercreditcards INT COMMENT 'Credit cards',
  networth INT COMMENT 'Estimated total net worth',
  marketingnameplate STRING COMMENT 'For marketing purposes',
  CONSTRAINT prospect_pk PRIMARY KEY(agencyid)
) 
TBLPROPERTIES ('delta.autoOptimize.autoCompact'=False, 'delta.autoOptimize.optimizeWrite'=True);

-- COMMAND ----------

CREATE OR REPLACE TABLE ${catalog}.${wh_db}_${scale_factor}.Financial (
  sk_companyid BIGINT NOT NULL COMMENT 'Company SK.',
  fi_year INT NOT NULL COMMENT 'Year of the quarter end.',
  fi_qtr INT NOT NULL COMMENT 'Quarter number that the financial information is for: valid values 1, 2, 3, 4.',
  fi_qtr_start_date DATE NOT NULL COMMENT 'Start date of quarter.',
  fi_revenue DOUBLE NOT NULL COMMENT 'Reported revenue for the quarter.',
  fi_net_earn DOUBLE NOT NULL COMMENT 'Net earnings reported for the quarter.',
  fi_basic_eps DOUBLE NOT NULL COMMENT 'Basic earnings per share for the quarter.',
  fi_dilut_eps DOUBLE NOT NULL COMMENT 'Diluted earnings per share for the quarter.',
  fi_margin DOUBLE NOT NULL COMMENT 'Profit divided by revenues for the quarter.',
  fi_inventory DOUBLE NOT NULL COMMENT 'Value of inventory on hand at the end of quarter.',
  fi_assets DOUBLE NOT NULL COMMENT 'Value of total assets at the end of the quarter.',
  fi_liability DOUBLE NOT NULL COMMENT 'Value of total liabilities at the end of the quarter.',
  fi_out_basic BIGINT NOT NULL COMMENT 'Average number of shares outstanding (basic).',
  fi_out_dilut BIGINT NOT NULL COMMENT 'Average number of shares outstanding (diluted).',
  CONSTRAINT financial_pk PRIMARY KEY(sk_companyid, fi_year, fi_qtr),
  CONSTRAINT financial_company_fk FOREIGN KEY (sk_companyid) REFERENCES ${catalog}.${wh_db}_${scale_factor}.DimCompany(sk_companyid)
)
TBLPROPERTIES ('delta.autoOptimize.autoCompact'=False, 'delta.autoOptimize.optimizeWrite'=True);

-- COMMAND ----------

CREATE OR REPLACE TABLE ${catalog}.${wh_db}_${scale_factor}.DimTrade (
  tradeid INT NOT NULL COMMENT 'Trade identifier',
  sk_brokerid BIGINT NOT NULL COMMENT 'Surrogate key for BrokerID',
  sk_createdateid BIGINT NOT NULL COMMENT 'Surrogate key for date created',
  sk_createtimeid BIGINT NOT NULL COMMENT 'Surrogate key for time created',
  sk_closedateid BIGINT COMMENT 'Surrogate key for date closed',
  sk_closetimeid BIGINT COMMENT 'Surrogate key for time closed',
  status STRING NOT NULL COMMENT 'Trade status',
  type STRING NOT NULL COMMENT 'Trade type',
  cashflag BOOLEAN NOT NULL COMMENT 'Is this trade a cash or margin trade?',
  sk_securityid BIGINT NOT NULL COMMENT 'Surrogate key for SecurityID',
  sk_companyid BIGINT NOT NULL COMMENT 'Surrogate key for CompanyID',
  quantity INT NOT NULL COMMENT 'Quantity of securities traded.',
  bidprice DOUBLE NOT NULL COMMENT 'The requested unit price.',
  sk_customerid BIGINT NOT NULL COMMENT 'Surrogate key for CustomerID',
  sk_accountid BIGINT NOT NULL COMMENT 'Surrogate key for AccountID',
  executedby STRING NOT NULL COMMENT 'Name of person executing the trade.',
  tradeprice DOUBLE COMMENT 'Unit price at which the security was traded.',
  fee DOUBLE COMMENT 'Fee charged for placing this trade request',
  commission DOUBLE COMMENT 'Commission earned on this trade',
  tax DOUBLE COMMENT 'Amount of tax due on this trade',
  batchid INT NOT NULL COMMENT 'Batch ID when this record was inserted',
  CONSTRAINT dimtrade_pk PRIMARY KEY(tradeid),
  CONSTRAINT dimtrade_security_fk FOREIGN KEY (sk_securityid) REFERENCES ${catalog}.${wh_db}_${scale_factor}.DimSecurity(sk_securityid),
  CONSTRAINT dimtrade_company_fk FOREIGN KEY (sk_companyid) REFERENCES ${catalog}.${wh_db}_${scale_factor}.DimCompany(sk_companyid),
  CONSTRAINT dimtrade_broker_fk FOREIGN KEY (sk_brokerid) REFERENCES ${catalog}.${wh_db}_${scale_factor}.DimBroker(sk_brokerid),
  CONSTRAINT dimtrade_account_fk FOREIGN KEY (sk_accountid) REFERENCES ${catalog}.${wh_db}_${scale_factor}.DimAccount(sk_accountid),
  CONSTRAINT dimtrade_customer_fk FOREIGN KEY (sk_customerid) REFERENCES ${catalog}.${wh_db}_${scale_factor}.DimCustomer(sk_customerid),
  CONSTRAINT dimtrade_createdate_fk FOREIGN KEY (sk_createdateid) REFERENCES ${catalog}.${wh_db}_${scale_factor}.DimDate(sk_dateid),
  CONSTRAINT dimtrade_closedate_fk FOREIGN KEY (sk_closedateid) REFERENCES ${catalog}.${wh_db}_${scale_factor}.DimDate(sk_dateid),
  CONSTRAINT dimtrade_createtime_fk FOREIGN KEY (sk_createtimeid) REFERENCES ${catalog}.${wh_db}_${scale_factor}.DimTime(sk_timeid),
  CONSTRAINT dimtrade_closetime_fk FOREIGN KEY (sk_closetimeid) REFERENCES ${catalog}.${wh_db}_${scale_factor}.DimTime(sk_timeid)
) 
TBLPROPERTIES ('delta.autoOptimize.autoCompact'=False, 'delta.autoOptimize.optimizeWrite'=True);

-- COMMAND ----------

CREATE OR REPLACE TABLE ${catalog}.${wh_db}_${scale_factor}.FactHoldings (
  tradeid INT NOT NULL COMMENT 'Key for Orignial Trade Indentifier',
  currenttradeid INT NOT NULL COMMENT 'Key for the current trade',
  sk_customerid BIGINT NOT NULL COMMENT 'Surrogate key for Customer Identifier',
  sk_accountid BIGINT NOT NULL COMMENT 'Surrogate key for Account Identifier',
  sk_securityid BIGINT NOT NULL COMMENT 'Surrogate key for Security Identifier',
  sk_companyid BIGINT NOT NULL COMMENT 'Surrogate key for Company Identifier',
  sk_dateid BIGINT COMMENT 'Surrogate key for the date associated with the',
  sk_timeid BIGINT COMMENT 'Surrogate key for the time associated with the',
  currentprice DOUBLE COMMENT 'Unit price of this security for the current trade',
  currentholding INT NOT NULL COMMENT 'Quantity of a security held after the current trade.',
  batchid INT NOT NULL COMMENT 'Batch ID when this record was inserted',
  CONSTRAINT factholdings_pk PRIMARY KEY(currenttradeid),
  CONSTRAINT factholdings_security_fk FOREIGN KEY (sk_securityid) REFERENCES ${catalog}.${wh_db}_${scale_factor}.DimSecurity(sk_securityid),
  CONSTRAINT factholdings_company_fk FOREIGN KEY (sk_companyid) REFERENCES ${catalog}.${wh_db}_${scale_factor}.DimCompany(sk_companyid),
  CONSTRAINT factholdings_trade_fk FOREIGN KEY (tradeid) REFERENCES ${catalog}.${wh_db}_${scale_factor}.DimTrade(tradeid),
  CONSTRAINT factholdings_currenttrade_fk FOREIGN KEY (currenttradeid) REFERENCES ${catalog}.${wh_db}_${scale_factor}.DimTrade(tradeid),
  CONSTRAINT factholdings_account_fk FOREIGN KEY (sk_accountid) REFERENCES ${catalog}.${wh_db}_${scale_factor}.DimAccount(sk_accountid),
  CONSTRAINT factholdings_customer_fk FOREIGN KEY (sk_customerid) REFERENCES ${catalog}.${wh_db}_${scale_factor}.DimCustomer(sk_customerid),
  CONSTRAINT factholdings_date_fk FOREIGN KEY (sk_dateid) REFERENCES ${catalog}.${wh_db}_${scale_factor}.DimDate(sk_dateid),
  CONSTRAINT factholdings_time_fk FOREIGN KEY (sk_timeid) REFERENCES ${catalog}.${wh_db}_${scale_factor}.DimTime(sk_timeid)
)
TBLPROPERTIES ('delta.autoOptimize.autoCompact'=False, 'delta.autoOptimize.optimizeWrite'=True);

-- COMMAND ----------

CREATE OR REPLACE TABLE ${catalog}.${wh_db}_${scale_factor}.FactCashBalances (
  sk_customerid BIGINT NOT NULL COMMENT 'Surrogate key for CustomerID',
  sk_accountid BIGINT NOT NULL COMMENT 'Surrogate key for AccountID',
  sk_dateid BIGINT NOT NULL COMMENT 'Surrogate key for the date',
  cash DOUBLE NOT NULL COMMENT 'Cash balance for the account after applying',
  batchid INT NOT NULL COMMENT 'Batch ID when this record was inserted',
  CONSTRAINT cashbalances_pk PRIMARY KEY(sk_customerid, sk_accountid, sk_dateid),
  CONSTRAINT cashbalances_customer_fk FOREIGN KEY (sk_customerid) REFERENCES ${catalog}.${wh_db}_${scale_factor}.DimCustomer(sk_customerid),
  CONSTRAINT cashbalances_account_fk FOREIGN KEY (sk_accountid) REFERENCES ${catalog}.${wh_db}_${scale_factor}.DimAccount(sk_accountid),
  CONSTRAINT cashbalances_date_fk FOREIGN KEY (sk_dateid) REFERENCES ${catalog}.${wh_db}_${scale_factor}.DimDate(sk_dateid)
)
TBLPROPERTIES ('delta.autoOptimize.autoCompact'=False, 'delta.autoOptimize.optimizeWrite'=True);

-- COMMAND ----------

CREATE OR REPLACE TABLE ${catalog}.${wh_db}_${scale_factor}.FactMarketHistory (
  sk_securityid BIGINT NOT NULL COMMENT 'Surrogate key for SecurityID',
  sk_companyid BIGINT NOT NULL COMMENT 'Surrogate key for CompanyID',
  sk_dateid BIGINT NOT NULL COMMENT 'Surrogate key for the date',
  peratio DOUBLE COMMENT 'Price to earnings per share ratio',
  yield DOUBLE NOT NULL COMMENT 'Dividend to price ratio, as a percentage',
  fiftytwoweekhigh DOUBLE NOT NULL COMMENT 'Security highest price in last 52 weeks from this day',
  sk_fiftytwoweekhighdate BIGINT NOT NULL COMMENT 'Earliest date on which the 52 week high price was set',
  fiftytwoweeklow DOUBLE NOT NULL COMMENT 'Security lowest price in last 52 weeks from this day',
  sk_fiftytwoweeklowdate BIGINT NOT NULL COMMENT 'Earliest date on which the 52 week low price was set',
  closeprice DOUBLE NOT NULL COMMENT 'Security closing price on this day',
  dayhigh DOUBLE NOT NULL COMMENT 'Highest price for the security on this day',
  daylow DOUBLE NOT NULL COMMENT 'Lowest price for the security on this day',
  volume INT NOT NULL COMMENT 'Trading volume of the security on this day',
  batchid INT NOT NULL COMMENT 'Batch ID when this record was inserted',
  CONSTRAINT fmh_pk PRIMARY KEY(sk_securityid, sk_dateid),
  CONSTRAINT fmh_security_fk FOREIGN KEY (sk_securityid) REFERENCES ${catalog}.${wh_db}_${scale_factor}.DimSecurity(sk_securityid),
  CONSTRAINT fmh_company_fk FOREIGN KEY (sk_companyid) REFERENCES ${catalog}.${wh_db}_${scale_factor}.DimCompany(sk_companyid),
  CONSTRAINT fmh_date_fk FOREIGN KEY (sk_dateid) REFERENCES ${catalog}.${wh_db}_${scale_factor}.DimDate(sk_dateid)
)
TBLPROPERTIES ('delta.autoOptimize.autoCompact'=False, 'delta.autoOptimize.optimizeWrite'=True);

-- COMMAND ----------

CREATE OR REPLACE TABLE ${catalog}.${wh_db}_${scale_factor}.FactWatches (
  sk_customerid BIGINT NOT NULL COMMENT 'Customer associated with watch list',
  sk_securityid BIGINT NOT NULL COMMENT 'Security listed on watch list',
  sk_dateid_dateplaced BIGINT NOT NULL COMMENT 'Date the watch list item was added',
  sk_dateid_dateremoved BIGINT COMMENT 'Date the watch list item was removed',
  batchid INT NOT NULL COMMENT 'Batch ID when this record was inserted',
  CONSTRAINT factwatches_pk PRIMARY KEY(sk_customerid, sk_securityid),
  CONSTRAINT factwatches_customer_fk FOREIGN KEY (sk_customerid) REFERENCES ${catalog}.${wh_db}_${scale_factor}.DimCustomer(sk_customerid),
  CONSTRAINT factwatches_security_fk FOREIGN KEY (sk_securityid) REFERENCES ${catalog}.${wh_db}_${scale_factor}.DimSecurity(sk_securityid),
  CONSTRAINT factwatches_dateplaced_fk FOREIGN KEY (sk_dateid_dateplaced) REFERENCES ${catalog}.${wh_db}_${scale_factor}.DimDate(sk_dateid),
  CONSTRAINT factwatches_dateremoved_fk FOREIGN KEY (sk_dateid_dateremoved) REFERENCES ${catalog}.${wh_db}_${scale_factor}.DimDate(sk_dateid)
) 
TBLPROPERTIES ('delta.autoOptimize.autoCompact'=False, 'delta.autoOptimize.optimizeWrite'=True);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Create Views to simplify later stages
-- MAGIC Especially those called by external tools like dbt

-- COMMAND ----------

CREATE OR REPLACE VIEW ${catalog}.${wh_db}_${scale_factor}_stage.v_BatchDate AS 
SELECT
  *,
  int(substring(_metadata.file_path FROM (position('/Batch', _metadata.file_path) + 6) FOR 1)) batchid
from read_files(
  "${tpcdi_directory}sf=${scale_factor}/Batch*",
  format => "csv",
  inferSchema => False,
  header => False,
  sep => "|",
  fileNamePattern => "BatchDate.txt",
  schema => "batchdate date"
)

-- COMMAND ----------

CREATE OR REPLACE VIEW ${catalog}.${wh_db}_${scale_factor}_stage.v_CustomerIncremental AS
with c as (
  SELECT
    * except(cdc_flag, cdc_dsn),
    int(substring(_metadata.file_path FROM (position('/Batch', _metadata.file_path) + 6) FOR 1)) batchid
  FROM read_files(
    "${tpcdi_directory}sf=${scale_factor}/Batch[23]",
    format => "csv",
    inferSchema => False,
    header => False,
    sep => "|",
    fileNamePattern => "Customer.txt",
    schema => "cdc_flag STRING, cdc_dsn BIGINT, customerid BIGINT, taxid STRING, status STRING, lastname STRING, firstname STRING, middleinitial STRING, gender STRING, tier TINYINT, dob DATE, addressline1 STRING, addressline2 STRING, postalcode STRING, city STRING, stateprov STRING, country STRING, c_ctry_1 STRING, c_area_1 STRING, c_local_1 STRING, c_ext_1 STRING, c_ctry_2 STRING, c_area_2 STRING, c_local_2 STRING, c_ext_2 STRING, c_ctry_3 STRING, c_area_3 STRING, c_local_3 STRING, c_ext_3 STRING, email1 STRING, email2 STRING, lcl_tx_id STRING, nat_tx_id STRING"
  )
)
SELECT
  customerid,
  nullif(taxid, '') taxid,
  decode(status, 
    'ACTV',	'Active',
    'CMPT','Completed',
    'CNCL','Canceled',
    'PNDG','Pending',
    'SBMT','Submitted',
    'INAC','Inactive') status,
  nullif(lastname, '') lastname,
  nullif(firstname, '') firstname,
  nullif(middleinitial, '') middleinitial,
  nullif(gender, '') gender,
  tier,
  dob,
  nullif(addressline1, '') addressline1,
  nullif(addressline2, '') addressline2,
  nullif(postalcode, '') postalcode,
  nullif(city, '') city,
  nullif(stateprov, '') stateprov,
  country,
  nvl2(
    nullif(c_local_1, ''),
    concat(
      nvl2(nullif(c_ctry_1, ''), '+' || c_ctry_1 || ' ', ''),
      nvl2(nullif(c_area_1, ''), '(' || c_area_1 || ') ', ''),
      c_local_1,
      nvl(c_ext_1, '')),
    try_cast(null as string)) phone1,
  nvl2(
    nullif(c_local_2, ''),
    concat(
      nvl2(nullif(c_ctry_2, ''), '+' || c_ctry_2 || ' ', ''),
      nvl2(nullif(c_area_2, ''), '(' || c_area_2 || ') ', ''),
      c_local_2,
      nvl(c_ext_2, '')),
    try_cast(null as string)) phone2,
  nvl2(
    nullif(c_local_3, ''),
    concat(
      nvl2(nullif(c_ctry_3, ''), '+' || c_ctry_3 || ' ', ''),
      nvl2(nullif(c_area_3, ''), '(' || c_area_3 || ') ', ''),
      c_local_3,
      nvl(c_ext_3, '')),
    try_cast(null as string)) phone3,
  nullif(email1, '') email1,
  nullif(email2, '') email2,
  nullif(lcl_tx_id, '') lcl_tx_id,
  nullif(nat_tx_id, '') nat_tx_id,
  batchid
FROM c

-- COMMAND ----------

CREATE OR REPLACE VIEW ${catalog}.${wh_db}_${scale_factor}_stage.v_AccountIncremental AS
SELECT
  * except(cdc_flag, cdc_dsn),
  int(substring(_metadata.file_path FROM (position('/Batch', _metadata.file_path) + 6) FOR 1)) batchid
FROM read_files(
  "${tpcdi_directory}sf=${scale_factor}/Batch[23]",
  format => "csv",
  inferSchema => False,
  header => False,
  sep => "|",
  fileNamePattern => "Account.txt",
  schema => "cdc_flag STRING, cdc_dsn BIGINT, accountid BIGINT, brokerid BIGINT, customerid BIGINT, accountdesc STRING, taxstatus TINYINT, status STRING"
);

-- COMMAND ----------

CREATE OR REPLACE VIEW ${catalog}.${wh_db}_${scale_factor}_stage.v_TradeIncremental AS
SELECT
  *,
  int(substring(_metadata.file_path FROM (position('/Batch', _metadata.file_path) + 6) FOR 1)) batchid
FROM read_files(
  "${tpcdi_directory}sf=${scale_factor}/Batch[23]",
  format => "csv",
  inferSchema => False,
  header => False,
  sep => "|",
  fileNamePattern => "Trade.txt",
  schema => "cdc_flag STRING, cdc_dsn BIGINT, tradeid BIGINT, t_dts TIMESTAMP, status STRING, t_tt_id STRING, cashflag TINYINT, t_s_symb STRING, quantity INT, bidprice DOUBLE, t_ca_id BIGINT, executedby STRING, tradeprice DOUBLE, fee DOUBLE, commission DOUBLE, tax DOUBLE"
)

-- COMMAND ----------

CREATE OR REPLACE VIEW ${catalog}.${wh_db}_${scale_factor}_stage.v_Trade AS
SELECT
  *,
  1 batchid
FROM read_files(
  "${tpcdi_directory}sf=${scale_factor}/Batch1",
  format => "csv",
  inferSchema => False,
  header => False,
  sep => "|",
  fileNamePattern => "Trade.txt",
  schema => "t_id BIGINT, t_dts TIMESTAMP, t_st_id STRING, t_tt_id STRING, t_is_cash TINYINT, t_s_symb STRING, quantity INT, bidprice DOUBLE, t_ca_id BIGINT, executedby STRING, tradeprice DOUBLE, fee DOUBLE, commission DOUBLE, tax DOUBLE"
);

-- COMMAND ----------

CREATE OR REPLACE VIEW ${catalog}.${wh_db}_${scale_factor}_stage.v_TradeHistory AS
SELECT
  *
FROM read_files(
  "${tpcdi_directory}sf=${scale_factor}/Batch1",
  format => "csv",
  inferSchema => False,
  header => False,
  sep => "|",
  fileNamePattern => "TradeHistory.txt",
  schema => "tradeid BIGINT, th_dts TIMESTAMP, status STRING"
);

-- COMMAND ----------

CREATE OR REPLACE VIEW ${catalog}.${wh_db}_${scale_factor}_stage.v_HR AS
SELECT
  *
FROM read_files(
  "${tpcdi_directory}sf=${scale_factor}/Batch1",
  format => "csv",
  inferSchema => False, 
  header => False,
  sep => ",",
  fileNamePattern => "HR.csv", 
  schema => "employeeid BIGINT, managerid BIGINT, employeefirstname STRING, employeelastname STRING, employeemi STRING, employeejobcode STRING , employeebranch STRING, employeeoffice STRING, employeephone STRING"
)

-- COMMAND ----------

CREATE OR REPLACE VIEW ${catalog}.${wh_db}_${scale_factor}_stage.v_CashTransactionIncremental AS
with historical as (
  SELECT
    accountid,
    to_date(ct_dts) datevalue,
    sum(ct_amt) account_daily_total
  FROM read_files(
    "${tpcdi_directory}sf=${scale_factor}/Batch1",
    format => "csv",
    inferSchema => False,
    header => False,
    sep => "|",
    fileNamePattern => "CashTransaction.txt",
    schema => "accountid BIGINT, ct_dts TIMESTAMP, ct_amt DOUBLE, ct_name STRING"
  )
  GROUP BY ALL
),
incremental as (
  SELECT
    accountid,
    to_date(ct_dts) datevalue,
    sum(ct_amt) account_daily_total,
    int(substring(_metadata.file_path FROM (position('/Batch', _metadata.file_path) + 6) FOR 1)) batchid
  FROM read_files(
    "${tpcdi_directory}sf=${scale_factor}/Batch[23]",
    format => "csv",
    inferSchema => False,
    header => False,
    sep => "|",
    fileNamePattern => "CashTransaction.txt",
    schema => "cdc_flag STRING, cdc_dsn BIGINT, accountid BIGINT, ct_dts TIMESTAMP, ct_amt DOUBLE, ct_name STRING"
  )
  GROUP BY ALL
)
SELECT 
  accountid,
  datevalue,
  sum(account_daily_total) OVER (partition by accountid order by datevalue) cash,
  batchid
FROM (
  SELECT *, 1 batchid FROM historical
  UNION ALL
  SELECT * FROM incremental
);

-- COMMAND ----------

CREATE OR REPLACE VIEW ${catalog}.${wh_db}_${scale_factor}_stage.v_HoldingHistory AS
SELECT
  *,
  1 batchid
FROM read_files(
  "${tpcdi_directory}sf=${scale_factor}/Batch1",
  format => "csv",
  inferSchema => False,
  header => False,
  sep => "|",
  fileNamePattern => "HoldingHistory.txt",
  schema => "hh_h_t_id INT, hh_t_id INT, hh_before_qty INT, hh_after_qty INT"
);

-- COMMAND ----------

CREATE OR REPLACE VIEW ${catalog}.${wh_db}_${scale_factor}_stage.v_HoldingIncremental AS
SELECT
  * except(cdc_flag, cdc_dsn),
  int(substring(_metadata.file_path FROM (position('/Batch', _metadata.file_path) + 6) FOR 1)) batchid
FROM read_files(
  "${tpcdi_directory}sf=${scale_factor}/Batch[23]",
  format => "csv",
  inferSchema => False,
  header => False,
  sep => "|",
  fileNamePattern => "HoldingHistory.txt",
  schema => "cdc_flag STRING, cdc_dsn BIGINT, hh_h_t_id INT, hh_t_id INT, hh_before_qty INT, hh_after_qty INT"
);

-- COMMAND ----------

CREATE OR REPLACE VIEW ${catalog}.${wh_db}_${scale_factor}_stage.v_DailyMarketIncremental AS
WITH dailymarkethistorical as (
  SELECT
    *,
    1 batchid
  FROM read_files(
    "${tpcdi_directory}sf=${scale_factor}/Batch1",
    format => "csv",
    inferSchema => False,
    header => False,
    sep => "|",
    fileNamePattern => "DailyMarket.txt",
    schema => "dm_date DATE, dm_s_symb STRING, dm_close DOUBLE, dm_high DOUBLE, dm_low DOUBLE, dm_vol INT"
  )
),
dailymarketincremental as (
  SELECT
    * except(cdc_flag, cdc_dsn),
    int(substring(_metadata.file_path FROM (position('/Batch', _metadata.file_path) + 6) FOR 1)) batchid
  FROM read_files(
    "${tpcdi_directory}sf=${scale_factor}/Batch[23]",
    format => "csv",
    inferSchema => False,
    header => False,
    sep => "|",
    fileNamePattern => "DailyMarket.txt",
    schema => "cdc_flag STRING, cdc_dsn BIGINT, dm_date DATE, dm_s_symb STRING, dm_close DOUBLE, dm_high DOUBLE, dm_low DOUBLE, dm_vol INT"
  )
),
DailyMarket as (
  SELECT
    dm.*,
    min_by(struct(dm_low, dm_date), dm_low) OVER (
      PARTITION BY dm_s_symb
      ORDER BY dm_date ASC 
      --RANGE BETWEEN INTERVAL '1' YEAR PRECEDING 
      ROWS BETWEEN 364 PRECEDING 
      AND CURRENT ROW
    ) fiftytwoweeklow,
    max_by(struct(dm_high, dm_date), dm_high) OVER (
      PARTITION by dm_s_symb
      ORDER BY dm_date ASC 
      --RANGE BETWEEN INTERVAL '1' YEAR PRECEDING 
      ROWS BETWEEN 364 PRECEDING 
      AND CURRENT ROW
    ) fiftytwoweekhigh
  FROM
    (
      SELECT * FROM dailymarkethistorical
      UNION ALL
      SELECT * FROM dailymarketincremental
    ) dm
)
select
  dm.* except(fiftytwoweeklow, fiftytwoweekhigh),
  fiftytwoweekhigh.dm_high fiftytwoweekhigh,
  fiftytwoweekhigh.dm_date fiftytwoweekhighdate,
  --bigint(date_format(fiftytwoweekhigh.dm_date, 'yyyyMMdd')) sk_fiftytwoweekhighdate,
  fiftytwoweeklow.dm_low fiftytwoweeklow,
  fiftytwoweeklow.dm_date fiftytwoweeklowdate
  --bigint(date_format(fiftytwoweeklow.dm_date, 'yyyyMMdd')) sk_fiftytwoweeklowdate
from DailyMarket dm;

-- COMMAND ----------

CREATE OR REPLACE VIEW ${catalog}.${wh_db}_${scale_factor}_stage.v_WatchHistory AS
SELECT
  *,
  1 batchid
FROM read_files(
  "${tpcdi_directory}sf=${scale_factor}/Batch1",
  format => "csv",
  inferSchema => False,
  header => False,
  sep => "|",
  fileNamePattern => "WatchHistory.txt",
  schema => "w_c_id BIGINT, w_s_symb STRING, w_dts TIMESTAMP, w_action STRING"
);

-- COMMAND ----------

CREATE OR REPLACE VIEW ${catalog}.${wh_db}_${scale_factor}_stage.v_WatchIncremental AS
SELECT
  * except(cdc_flag, cdc_dsn),
  int(substring(_metadata.file_path FROM (position('/Batch', _metadata.file_path) + 6) FOR 1)) batchid
FROM
  read_files(
    "${tpcdi_directory}sf=${scale_factor}/Batch[23]",
    format => "csv",
    inferSchema => False,
    header => False,
    sep => "|",
    fileNamePattern => "WatchHistory.txt",
    schema => "cdc_flag STRING, cdc_dsn BIGINT, w_c_id BIGINT, w_s_symb STRING, w_dts TIMESTAMP, w_action STRING"
  );

-- COMMAND ----------

CREATE OR REPLACE VIEW ${catalog}.${wh_db}_${scale_factor}_stage.v_Prospect AS
with p as (
  SELECT
    *,
    if(
      isnotnull(
        if(networth > 1000000 or income > 200000,"HighValue+","") || 
        if(numberchildren > 3 or numbercreditcards > 5,"Expenses+","") ||
        if(age > 45, "Boomer+", "") ||
        if(income < 50000 or creditrating < 600 or networth < 100000, "MoneyAlert+","") ||
        if(numbercars > 3 or numbercreditcards > 7, "Spender+","") ||
        if(age < 25 and networth > 1000000, "Inherited+","")),
      left(
        if(networth > 1000000 or income > 200000,"HighValue+","") || 
        if(numberchildren > 3 or numbercreditcards > 5,"Expenses+","") ||
        if(age > 45, "Boomer+", "") ||
        if(income < 50000 or creditrating < 600 or networth < 100000, "MoneyAlert+","") ||
        if(numbercars > 3 or numbercreditcards > 7, "Spender+","") ||
        if(age < 25 and networth > 1000000, "Inherited+",""),
        length(
          if(networth > 1000000 or income > 200000,"HighValue+","") || 
          if(numberchildren > 3 or numbercreditcards > 5,"Expenses+","") ||
          if(age > 45, "Boomer+", "") ||
          if(income < 50000 or creditrating < 600 or networth < 100000, "MoneyAlert+","") ||
          if(numbercars > 3 or numbercreditcards > 7, "Spender+","") ||
          if(age < 25 and networth > 1000000, "Inherited+",""))
        -1),
      NULL) marketingnameplate,
    int(substring(_metadata.file_path FROM (position('/Batch', _metadata.file_path) + 6) FOR 1)) batchid
  FROM read_files(
    "${tpcdi_directory}sf=${scale_factor}/Batch*",
    format => "csv",
    inferSchema => False,
    header => False,
    sep => ",",
    fileNamePattern => "Prospect.csv",
    schema => "agencyid STRING, lastname STRING, firstname STRING, middleinitial STRING, gender STRING, addressline1 STRING, addressline2 STRING, postalcode STRING, city STRING, state STRING, country STRING, phone STRING, income STRING, numbercars INT, numberchildren INT, maritalstatus STRING, age INT, creditrating INT, ownorrentflag STRING, employer STRING, numbercreditcards INT, networth INT"
  )
)
SELECT * FROM (
  SELECT
    * except(batchid),
    max(batchid) recordbatchid,
    min(batchid) batchid
  FROM p
  GROUP BY ALL
)
WHERE recordbatchid = 3;

-- COMMAND ----------

CREATE OR REPLACE VIEW ${catalog}.${wh_db}_${scale_factor}_stage.v_FinWire AS
SELECT
  if(
    substring(value, 16, 3) = 'FIN', 
    nvl2(
      try_cast(trim(substring(value, 187, 60)) as bigint), 
      'FIN_COMPANYID', 
      'FIN_NAME'
    ), 
    substring(value, 16, 3)
  ) rectype,
  to_date(substring(value, 1, 8), 'yyyyMMdd') AS recdate,
  --to_date(try_to_timestamp(substring(value, 1, 8), 'yyyyMMdd')) AS recdate,
  substring(value, 19) value
FROM text.`${tpcdi_directory}sf=${scale_factor}/Batch1/FINWIRE[0-9][0-9][0-9][0-9]Q[1-4]`;
