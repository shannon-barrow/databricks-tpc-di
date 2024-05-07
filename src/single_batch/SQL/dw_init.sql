-- Databricks notebook source
CREATE CATALOG IF NOT EXISTS ${catalog};
GRANT ALL PRIVILEGES ON CATALOG ${catalog} TO `account users`;
USE CATALOG ${catalog};
DROP DATABASE IF EXISTS ${wh_db} cascade;
CREATE DATABASE ${wh_db};
CREATE DATABASE IF NOT EXISTS ${wh_db}_stage;
DROP TABLE IF EXISTS ${wh_db}_stage.dimcustomerstg;
DROP TABLE IF EXISTS ${wh_db}_stage.finwire;

-- COMMAND ----------

CREATE OR REPLACE TABLE ${catalog}.${wh_db}.TaxRate (
  tx_id STRING NOT NULL COMMENT 'Tax rate code',
  tx_name STRING NOT NULL COMMENT 'Tax rate description',
  tx_rate FLOAT NOT NULL COMMENT 'Tax rate',
  CONSTRAINT taxrate_pk PRIMARY KEY(tx_id)
);

-- COMMAND ----------

CREATE OR REPLACE TABLE ${catalog}.${wh_db}.BatchDate (
  batchdate DATE NOT NULL COMMENT 'Batch date',
  batchid INT NOT NULL COMMENT 'Batch ID when this record was inserted',
  CONSTRAINT batchdate_pk PRIMARY KEY(batchdate)
);

-- COMMAND ----------

CREATE OR REPLACE TABLE ${catalog}.${wh_db}.DimDate (
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
);

-- COMMAND ----------

CREATE OR REPLACE TABLE ${catalog}.${wh_db}.DimTime (
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
);

-- COMMAND ----------

CREATE OR REPLACE TABLE ${catalog}.${wh_db}.StatusType (
  st_id STRING NOT NULL COMMENT 'Status code',
  st_name STRING NOT NULL COMMENT 'Status description',
  CONSTRAINT statustype_pk PRIMARY KEY(st_name)
);

-- COMMAND ----------

CREATE OR REPLACE TABLE ${catalog}.${wh_db}.industry (
  in_id STRING NOT NULL COMMENT 'Industry code',
  in_name STRING NOT NULL COMMENT 'Industry description',
  in_sc_id STRING NOT NULL COMMENT 'Sector identifier',
  CONSTRAINT industry_pk PRIMARY KEY(in_name)
);

-- COMMAND ----------

CREATE OR REPLACE TABLE ${catalog}.${wh_db}.TradeType (
  tt_id STRING NOT NULL COMMENT 'Trade type code',
  tt_name STRING NOT NULL COMMENT 'Trade type description',
  tt_is_sell INT NOT NULL COMMENT 'Flag indicating a sale',
  tt_is_mrkt INT NOT NULL COMMENT 'Flag indicating a market order',
  CONSTRAINT tradetype_pk PRIMARY KEY(tt_id)
);

-- COMMAND ----------

CREATE OR REPLACE TABLE ${catalog}.${wh_db}_stage.FinWire (
  value STRING COMMENT 'Pre-parsed String Values of all FinWire files',
  rectype STRING COMMENT 'Indicates the type of table into which this record will eventually be parsed: CMP FIN or SEC'
) PARTITIONED BY (rectype);

-- COMMAND ----------

CREATE OR REPLACE TABLE ${catalog}.${wh_db}.DimBroker (
  sk_brokerid BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key for broker',
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
);

-- COMMAND ----------

CREATE OR REPLACE TABLE ${catalog}.${wh_db}.DimCustomer (
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

-- COMMAND ----------

CREATE OR REPLACE TABLE ${catalog}.${wh_db}_stage.DimCustomerStg (
  sk_customerid BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key for CustomerID',
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
  lcl_tx_id STRING COMMENT 'Customers local tax rate',
  nat_tx_id STRING COMMENT 'Customers national tax rate',
  batchid INT COMMENT 'Batch ID when this record was inserted',
  iscurrent BOOLEAN COMMENT 'True if this is the current record',
  effectivedate DATE COMMENT 'Beginning of date range when this record was the current record',
  enddate DATE COMMENT 'Ending of date range when this record was the current record. A record that is not expired will use the date 9999-12-31.'
) PARTITIONED BY (iscurrent);

-- COMMAND ----------

CREATE OR REPLACE TABLE ${catalog}.${wh_db}.DimCompany (
  sk_companyid BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key for CompanyID',
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
  CONSTRAINT dimcompany_status_fk FOREIGN KEY (status) REFERENCES ${catalog}.${wh_db}.StatusType(st_name),
  CONSTRAINT dimcompany_industry_fk FOREIGN KEY (industry) REFERENCES ${catalog}.${wh_db}.Industry(in_name)
);

-- COMMAND ----------

CREATE OR REPLACE TABLE ${catalog}.${wh_db}.DimCustomer (
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
);

-- COMMAND ----------

CREATE OR REPLACE TABLE ${catalog}.${wh_db}.DimAccount (
  sk_accountid BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL COMMENT 'Surrogate key for AccountID',
  accountid BIGINT NOT NULL COMMENT 'Customer account identifier',
  sk_brokerid BIGINT NOT NULL COMMENT 'Surrogate key of managing broker',
  sk_customerid BIGINT NOT NULL COMMENT 'Surrogate key of customer',
  accountdesc STRING COMMENT 'Name of customer account',
  taxstatus TINYINT COMMENT 'Tax status of this account',
  status STRING NOT NULL COMMENT 'Account status, active or closed',
  iscurrent BOOLEAN GENERATED ALWAYS AS (if(enddate = date('9999-12-31'), true, false)) NOT NULL COMMENT 'True if this is the current record',
  batchid INT NOT NULL COMMENT 'Batch ID when this record was inserted',
  effectivedate DATE NOT NULL COMMENT 'Beginning of date range when this record was the current record',
  enddate DATE NOT NULL COMMENT 'Ending of date range when this record was the current record. A record that is not expired will use the date 9999-12-31.',
  CONSTRAINT dimaccount_pk PRIMARY KEY(sk_accountid),
  CONSTRAINT dimaccount_customer_fk FOREIGN KEY (sk_customerid) REFERENCES ${catalog}.${wh_db}.DimCustomer(sk_customerid),
  CONSTRAINT dimaccount_broker_fk FOREIGN KEY (sk_brokerid) REFERENCES ${catalog}.${wh_db}.DimBroker(sk_brokerid)
);

-- COMMAND ----------

CREATE OR REPLACE TABLE ${catalog}.${wh_db}.DimSecurity (
  sk_securityid BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key for Symbol',
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
  CONSTRAINT dimsecurity_status_fk FOREIGN KEY (status) REFERENCES ${catalog}.${wh_db}.StatusType(st_name),
  CONSTRAINT dimsecurity_company_fk FOREIGN KEY (sk_companyid) REFERENCES ${catalog}.${wh_db}.DimCompany(sk_companyid)
);

-- COMMAND ----------

CREATE OR REPLACE TABLE ${catalog}.${wh_db}.Prospect (
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
);

-- COMMAND ----------

CREATE OR REPLACE TABLE ${catalog}.${wh_db}.Financial (
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
  CONSTRAINT financial_company_fk FOREIGN KEY (sk_companyid) REFERENCES ${catalog}.${wh_db}.DimCompany(sk_companyid)
);

-- COMMAND ----------

CREATE OR REPLACE TABLE ${catalog}.${wh_db}.DimTrade (
  tradeid INT NOT NULL COMMENT 'Trade identifier',
  sk_brokerid BIGINT NOT NULL COMMENT 'Surrogate key for BrokerID',
  sk_createdateid BIGINT NOT NULL COMMENT 'Surrogate key for date created',
  sk_createtimeid BIGINT NOT NULL COMMENT 'Surrogate key for time created',
  sk_closedateid BIGINT COMMENT 'Surrogate key for date closed',
  sk_closetimeid BIGINT COMMENT 'Surrogate key for time closed',
  status STRING NOT NULL COMMENT 'Trade status',
  type STRING NOT NULL COMMENT 'Trade type',
  cashflag BOOLEAN NOT NULL COMMENT 'Is this trade a cash (1) or margin (0) trade?',
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
  CONSTRAINT dimtrade_security_fk FOREIGN KEY (sk_securityid) REFERENCES ${catalog}.${wh_db}.DimSecurity(sk_securityid),
  CONSTRAINT dimtrade_company_fk FOREIGN KEY (sk_companyid) REFERENCES ${catalog}.${wh_db}.DimCompany(sk_companyid),
  CONSTRAINT dimtrade_broker_fk FOREIGN KEY (sk_brokerid) REFERENCES ${catalog}.${wh_db}.DimBroker(sk_brokerid),
  CONSTRAINT dimtrade_account_fk FOREIGN KEY (sk_accountid) REFERENCES ${catalog}.${wh_db}.DimAccount(sk_accountid),
  CONSTRAINT dimtrade_customer_fk FOREIGN KEY (sk_customerid) REFERENCES ${catalog}.${wh_db}.DimCustomer(sk_customerid),
  CONSTRAINT dimtrade_createdate_fk FOREIGN KEY (sk_createdateid) REFERENCES ${catalog}.${wh_db}.DimDate(sk_dateid),
  CONSTRAINT dimtrade_closedate_fk FOREIGN KEY (sk_closedateid) REFERENCES ${catalog}.${wh_db}.DimDate(sk_dateid),
  CONSTRAINT dimtrade_createtime_fk FOREIGN KEY (sk_createtimeid) REFERENCES ${catalog}.${wh_db}.DimTime(sk_timeid),
  CONSTRAINT dimtrade_closetime_fk FOREIGN KEY (sk_closetimeid) REFERENCES ${catalog}.${wh_db}.DimTime(sk_timeid)
);

-- COMMAND ----------

CREATE OR REPLACE TABLE ${catalog}.${wh_db}.FactHoldings (
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
  CONSTRAINT factholdings_security_fk FOREIGN KEY (sk_securityid) REFERENCES ${catalog}.${wh_db}.DimSecurity(sk_securityid),
  CONSTRAINT factholdings_company_fk FOREIGN KEY (sk_companyid) REFERENCES ${catalog}.${wh_db}.DimCompany(sk_companyid),
  CONSTRAINT factholdings_trade_fk FOREIGN KEY (tradeid) REFERENCES ${catalog}.${wh_db}.DimTrade(tradeid),
  CONSTRAINT factholdings_currenttrade_fk FOREIGN KEY (currenttradeid) REFERENCES ${catalog}.${wh_db}.DimTrade(tradeid),
  CONSTRAINT factholdings_account_fk FOREIGN KEY (sk_accountid) REFERENCES ${catalog}.${wh_db}.DimAccount(sk_accountid),
  CONSTRAINT factholdings_customer_fk FOREIGN KEY (sk_customerid) REFERENCES ${catalog}.${wh_db}.DimCustomer(sk_customerid),
  CONSTRAINT factholdings_date_fk FOREIGN KEY (sk_dateid) REFERENCES ${catalog}.${wh_db}.DimDate(sk_dateid),
  CONSTRAINT factholdings_time_fk FOREIGN KEY (sk_timeid) REFERENCES ${catalog}.${wh_db}.DimTime(sk_timeid)
)

-- COMMAND ----------

CREATE OR REPLACE TABLE ${catalog}.${wh_db}.FactCashBalances (
  sk_customerid BIGINT NOT NULL COMMENT 'Surrogate key for CustomerID',
  sk_accountid BIGINT NOT NULL COMMENT 'Surrogate key for AccountID',
  sk_dateid BIGINT NOT NULL COMMENT 'Surrogate key for the date',
  cash DOUBLE NOT NULL COMMENT 'Cash balance for the account after applying',
  batchid INT NOT NULL COMMENT 'Batch ID when this record was inserted',
  CONSTRAINT cashbalances_pk PRIMARY KEY(sk_customerid, sk_accountid, sk_dateid),
  CONSTRAINT cashbalances_customer_fk FOREIGN KEY (sk_customerid) REFERENCES ${catalog}.${wh_db}.DimCustomer(sk_customerid),
  CONSTRAINT cashbalances_account_fk FOREIGN KEY (sk_accountid) REFERENCES ${catalog}.${wh_db}.DimAccount(sk_accountid),
  CONSTRAINT cashbalances_date_fk FOREIGN KEY (sk_dateid) REFERENCES ${catalog}.${wh_db}.DimDate(sk_dateid)
)

-- COMMAND ----------

CREATE OR REPLACE TABLE ${catalog}.${wh_db}.FactMarketHistory (
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
  CONSTRAINT fmh_security_fk FOREIGN KEY (sk_securityid) REFERENCES ${catalog}.${wh_db}.DimSecurity(sk_securityid),
  CONSTRAINT fmh_company_fk FOREIGN KEY (sk_companyid) REFERENCES ${catalog}.${wh_db}.DimCompany(sk_companyid),
  CONSTRAINT fmh_date_fk FOREIGN KEY (sk_dateid) REFERENCES ${catalog}.${wh_db}.DimDate(sk_dateid)
)

-- COMMAND ----------

CREATE OR REPLACE TABLE ${catalog}.${wh_db}.FactWatches (
  sk_customerid BIGINT NOT NULL COMMENT 'Customer associated with watch list',
  sk_securityid BIGINT NOT NULL COMMENT 'Security listed on watch list',
  sk_dateid_dateplaced BIGINT NOT NULL COMMENT 'Date the watch list item was added',
  sk_dateid_dateremoved BIGINT COMMENT 'Date the watch list item was removed',
  batchid INT NOT NULL COMMENT 'Batch ID when this record was inserted',
  CONSTRAINT factwatches_pk PRIMARY KEY(sk_customerid, sk_securityid),
  CONSTRAINT factwatches_customer_fk FOREIGN KEY (sk_customerid) REFERENCES ${catalog}.${wh_db}.DimCustomer(sk_customerid),
  CONSTRAINT factwatches_security_fk FOREIGN KEY (sk_securityid) REFERENCES ${catalog}.${wh_db}.DimSecurity(sk_securityid),
  CONSTRAINT factwatches_dateplaced_fk FOREIGN KEY (sk_dateid_dateplaced) REFERENCES ${catalog}.${wh_db}.DimDate(sk_dateid),
  CONSTRAINT factwatches_dateremoved_fk FOREIGN KEY (sk_dateid_dateremoved) REFERENCES ${catalog}.${wh_db}.DimDate(sk_dateid)
)

-- COMMAND ----------

CREATE OR REPLACE VIEW ${catalog}.${wh_db}_stage.v_TradeIncremental AS
SELECT
  *,
  cast(
    substring(
      _metadata.file_path
      FROM (position('/Batch', _metadata.file_path) + 6) FOR 1
    ) as int
  ) batchid
FROM
  read_files(
    "${tpcdi_directory}sf=${scale_factor}/Batch[23]",
    format => "csv",
    inferSchema => False,
    header => False,
    sep => "|",
    fileNamePattern => "Trade.txt",
    schema => "cdc_flag STRING COMMENT 'Denotes insert or update', cdc_dsn BIGINT COMMENT 'Database Sequence Number', t_id BIGINT COMMENT 'Trade identifier.', t_dts TIMESTAMP COMMENT 'Date and time of trade.', t_st_id STRING COMMENT 'Status type identifier', t_tt_id STRING COMMENT 'Trade type identifier', t_is_cash TINYINT, t_s_symb STRING COMMENT 'Security symbol of the security', t_qty INT COMMENT 'Quantity of securities traded.', t_bid_price DOUBLE COMMENT 'The requested unit price.', t_ca_id BIGINT COMMENT 'Customer account identifier.', t_exec_name STRING COMMENT 'Name of the person executing the trade.', t_trade_price DOUBLE COMMENT 'Unit price at which the security was traded.', t_chrg DOUBLE COMMENT 'Fee charged for placing this trade request.', t_comm DOUBLE COMMENT 'Commission earned on this trade', t_tax DOUBLE COMMENT 'Amount of tax due on this trade'"
  )

-- COMMAND ----------

CREATE OR REPLACE VIEW ${catalog}.${wh_db}_stage.v_Trade AS
SELECT
  *,
  1 batchid
FROM 
  read_files(
    "${tpcdi_directory}sf=${scale_factor}/Batch1",
    format => "csv",
    inferSchema => False,
    header => False,
    sep => "|",
    fileNamePattern => "Trade.txt",
    schema => "t_id BIGINT COMMENT 'Trade identifier.', t_dts TIMESTAMP COMMENT 'Date and time of trade.', t_st_id STRING COMMENT 'Status type identifier', t_tt_id STRING COMMENT 'Trade type identifier', t_is_cash TINYINT , t_s_symb STRING COMMENT 'Security symbol of the security', t_qty INT COMMENT 'Quantity of securities traded.', t_bid_price DOUBLE COMMENT 'The requested unit price.', t_ca_id BIGINT COMMENT 'Customer account identifier.', t_exec_name STRING COMMENT 'Name of the person executing the trade.', t_trade_price DOUBLE COMMENT 'Unit price at which the security was traded.', t_chrg DOUBLE COMMENT 'Fee charged for placing this trade request.', t_comm DOUBLE COMMENT 'Commission earned on this trade', t_tax DOUBLE COMMENT 'Amount of tax due on this trade'"
);

-- COMMAND ----------

CREATE OR REPLACE VIEW ${catalog}.${wh_db}_stage.v_TradeHistory AS
SELECT
  *
FROM 
  read_files(
    "${tpcdi_directory}sf=${scale_factor}/Batch1",
    format => "csv",
    inferSchema => False,
    header => False,
    sep => "|",
    fileNamePattern => "TradeHistory.txt",
    schema => "th_t_id BIGINT COMMENT 'Trade identifier. Corresponds to T_ID in the Trade.txt file', th_dts TIMESTAMP COMMENT 'When the trade history was updated.', th_st_id STRING COMMENT 'Status type identifier.'"
);

-- COMMAND ----------

CREATE OR REPLACE VIEW ${catalog}.${wh_db}_stage.v_CustomerIncremental AS
SELECT
  * except(cdc_flag, cdc_dsn),
  cast(
    substring(
      _metadata.file_path
      FROM (position('/Batch', _metadata.file_path) + 6) FOR 1
    ) as int
  ) batchid
FROM
  read_files(
    "${tpcdi_directory}sf=${scale_factor}/Batch[23]",
    format => "csv",
    inferSchema => False,
    header => False,
    sep => "|",
    fileNamePattern => "Customer.txt",
    schema => "cdc_flag STRING COMMENT 'Denotes insert or update', cdc_dsn BIGINT COMMENT 'Database Sequence Number', customerid BIGINT COMMENT 'Customer identifier', taxid STRING COMMENT 'Customer’s tax identifier', status STRING COMMENT 'Customer status type identifier', lastname STRING COMMENT 'Primary Customers last name.', firstname STRING COMMENT 'Primary Customers first name.', middleinitial STRING COMMENT 'Primary Customers middle initial', gender STRING COMMENT 'Gender of the primary customer', tier TINYINT COMMENT 'Customer tier', dob DATE COMMENT 'Customer’s date of birth, as YYYY-MM-DD.', addressline1 STRING COMMENT 'Address Line 1', addressline2 STRING COMMENT 'Address Line 2', postalcode STRING COMMENT 'Zip or postal code', city STRING COMMENT 'City', stateprov STRING COMMENT 'State or province', country STRING COMMENT 'Country', c_ctry_1 STRING COMMENT 'Country code for Customers phone 1.', c_area_1 STRING COMMENT 'Area code for customer’s phone 1.', c_local_1 STRING COMMENT 'Local number for customer’s phone 1.', c_ext_1 STRING COMMENT 'Extension number for Customer’s phone 1.', c_ctry_2 STRING COMMENT 'Country code for Customers phone 2.', c_area_2 STRING COMMENT 'Area code for Customer’s phone 2.', c_local_2 STRING COMMENT 'Local number for Customer’s phone 2.', c_ext_2 STRING COMMENT 'Extension number for Customer’s phone 2.', c_ctry_3 STRING COMMENT 'Country code for Customers phone 3.', c_area_3 STRING COMMENT 'Area code for Customer’s phone 3.', c_local_3 STRING COMMENT 'Local number for Customer’s phone 3.', c_ext_3 STRING COMMENT 'Extension number for Customer’s phone 3.', email1 STRING COMMENT 'Customers e-mail address 1.', email2 STRING COMMENT 'Customers e-mail address 2.', lcl_tx_id STRING COMMENT 'Customers local tax rate', nat_tx_id STRING COMMENT 'Customers national tax rate'"
  );

-- COMMAND ----------

CREATE OR REPLACE VIEW ${catalog}.${wh_db}_stage.v_HR AS
SELECT
  *
FROM 
  read_files(
  "${tpcdi_directory}sf=${scale_factor}/Batch1",
  format => "csv",
  inferSchema => False, 
  header => False,
  sep => ",",
  fileNamePattern => "HR.csv", 
  schema => "employeeid BIGINT COMMENT 'ID of employee', managerid BIGINT COMMENT 'ID of employee’s manager', employeefirstname STRING COMMENT 'First name', employeelastname STRING COMMENT 'Last name', employeemi STRING COMMENT 'Middle initial', employeejobcode STRING COMMENT 'Numeric job code', employeebranch STRING COMMENT 'Facility in which employee has office', employeeoffice STRING COMMENT 'Office number or description', employeephone STRING COMMENT 'Employee phone number'"
)

-- COMMAND ----------

CREATE OR REPLACE VIEW ${catalog}.${wh_db}_stage.v_AccountIncremental AS
SELECT
  * except(cdc_flag, cdc_dsn),
  cast(
    substring(
      _metadata.file_path
      FROM (position('/Batch', _metadata.file_path) + 6) FOR 1
    ) as int
  ) batchid
FROM
  read_files(
    "${tpcdi_directory}sf=${scale_factor}/Batch[23]",
    format => "csv",
    inferSchema => False,
    header => False,
    sep => "|",
    fileNamePattern => "Account.txt",
    schema => "cdc_flag STRING COMMENT 'Denotes insert or update', cdc_dsn BIGINT COMMENT 'Database Sequence Number', accountid BIGINT COMMENT 'Customer account identifier', ca_b_id BIGINT COMMENT 'Identifier of the managing broker', ca_c_id BIGINT COMMENT 'Owning customer identifier', accountDesc STRING COMMENT 'Name of customer account', TaxStatus TINYINT COMMENT 'Tax status of this account', ca_st_id STRING COMMENT 'Customer status type identifier'"
  );

-- COMMAND ----------

CREATE OR REPLACE VIEW ${catalog}.${wh_db}_stage.v_CashTransactionHistory AS
SELECT
  *,
  1 batchid
FROM 
  read_files(
    "${tpcdi_directory}sf=${scale_factor}/Batch1",
    format => "csv",
    inferSchema => False,
    header => False,
    sep => "|",
    fileNamePattern => "CashTransaction.txt",
    schema => "ct_ca_id BIGINT COMMENT 'Customer account identifier', ct_dts TIMESTAMP COMMENT 'Timestamp of when the trade took place', ct_amt DOUBLE COMMENT 'Amount of the cash transaction.', ct_name STRING COMMENT 'Transaction name, or description: e.g. Cash from sale of DuPont stock.'"
  );

-- COMMAND ----------

CREATE OR REPLACE VIEW ${catalog}.${wh_db}_stage.v_CashTransactionIncremental AS
SELECT
  * except(cdc_flag, cdc_dsn),
  cast(
    substring(
      _metadata.file_path
      FROM (position('/Batch', _metadata.file_path) + 6) FOR 1
    ) as int
  ) batchid
FROM
  read_files(
    "${tpcdi_directory}sf=${scale_factor}/Batch[23]",
    format => "csv",
    inferSchema => False,
    header => False,
    sep => "|",
    fileNamePattern => "CashTransaction.txt",
    schema => "cdc_flag STRING COMMENT 'Denotes insert or update', cdc_dsn BIGINT COMMENT 'Database Sequence Number', ct_ca_id BIGINT COMMENT 'Customer account identifier', ct_dts TIMESTAMP COMMENT 'Timestamp of when the trade took place', ct_amt DOUBLE COMMENT 'Amount of the cash transaction.', ct_name STRING COMMENT 'Transaction name, or description: e.g. Cash from sale of DuPont stock.'"
  );

-- COMMAND ----------

CREATE OR REPLACE VIEW ${catalog}.${wh_db}_stage.v_HoldingHistory AS
SELECT
  *,
  1 batchid
FROM 
  read_files(
    "${tpcdi_directory}sf=${scale_factor}/Batch1",
    format => "csv",
    inferSchema => False,
    header => False,
    sep => "|",
    fileNamePattern => "HoldingHistory.txt",
    schema => "hh_h_t_id INT COMMENT 'Trade Identifier of the trade that originally created the holding row.', hh_t_id INT COMMENT 'Trade Identifier of the current trade', hh_before_qty INT COMMENT 'Quantity of this security held before the modifying trade.', hh_after_qty INT COMMENT 'Quantity of this security held after the modifying trade.'"
  );

-- COMMAND ----------

CREATE OR REPLACE VIEW ${catalog}.${wh_db}_stage.v_HoldingIncremental AS
SELECT
  * except(cdc_flag, cdc_dsn),
  cast(
    substring(
      _metadata.file_path
      FROM (position('/Batch', _metadata.file_path) + 6) FOR 1
    ) as int
  ) batchid
FROM
  read_files(
    "${tpcdi_directory}sf=${scale_factor}/Batch[23]",
    format => "csv",
    inferSchema => False,
    header => False,
    sep => "|",
    fileNamePattern => "HoldingHistory.txt",
    schema => "cdc_flag STRING COMMENT 'Denotes insert or update', cdc_dsn BIGINT COMMENT 'Database Sequence Number', hh_h_t_id INT COMMENT 'Trade Identifier of the trade that originally created the holding row.', hh_t_id INT COMMENT 'Trade Identifier of the current trade', hh_before_qty INT COMMENT 'Quantity of this security held before the modifying trade.', hh_after_qty INT COMMENT 'Quantity of this security held after the modifying trade.'"
  );

-- COMMAND ----------

CREATE OR REPLACE VIEW ${catalog}.${wh_db}_stage.v_dailymarkethistorical AS
SELECT
  *,
  1 batchid
FROM 
  read_files(
    "${tpcdi_directory}sf=${scale_factor}/Batch1",
    format => "csv",
    inferSchema => False,
    header => False,
    sep => "|",
    fileNamePattern => "DailyMarket.txt",
    schema => "dm_date DATE COMMENT 'Date of last completed trading day', dm_s_symb STRING COMMENT 'Security symbol of the security', dm_close DOUBLE COMMENT 'Closing price of the security on this day', dm_high DOUBLE COMMENT 'Highest price for the security on this day', dm_low DOUBLE COMMENT 'Lowest price for the security on this day', dm_vol INT COMMENT 'Volume of the security on this day'"
  );

-- COMMAND ----------

CREATE OR REPLACE VIEW ${catalog}.${wh_db}_stage.v_DailyMarketIncremental AS
SELECT
  * except(cdc_flag, cdc_dsn),
  cast(
    substring(
      _metadata.file_path
      FROM (position('/Batch', _metadata.file_path) + 6) FOR 1
    ) as int
  ) batchid
FROM
  read_files(
    "${tpcdi_directory}sf=${scale_factor}/Batch[23]",
    format => "csv",
    inferSchema => False,
    header => False,
    sep => "|",
    fileNamePattern => "DailyMarket.txt",
    schema => "cdc_flag STRING COMMENT 'Denotes insert or update', cdc_dsn BIGINT COMMENT 'Database Sequence Number', dm_date DATE COMMENT 'Date of last completed trading day.', dm_s_symb STRING COMMENT 'Security symbol of the security', dm_close DOUBLE COMMENT 'Closing price of the security on this day.', dm_high DOUBLE COMMENT 'Highest price for the security on this day.', dm_low DOUBLE COMMENT 'Lowest price for the security on this day.', dm_vol INT COMMENT 'Volume of the security on this day.'"
  );

-- COMMAND ----------

CREATE OR REPLACE VIEW ${catalog}.${wh_db}_stage.v_WatchHistory AS
SELECT
  *,
  1 batchid
FROM 
  read_files(
    "${tpcdi_directory}sf=${scale_factor}/Batch1",
    format => "csv",
    inferSchema => False,
    header => False,
    sep => "|",
    fileNamePattern => "WatchHistory.txt",
    schema => "w_c_id BIGINT COMMENT 'Customer identifier', w_s_symb STRING COMMENT 'Symbol of the security to watch', w_dts TIMESTAMP COMMENT 'Date and Time Stamp for the action', w_action STRING COMMENT 'Whether activating or canceling the watch'"
  );

-- COMMAND ----------

CREATE OR REPLACE VIEW ${catalog}.${wh_db}_stage.v_WatchIncremental AS
SELECT
  * except(cdc_flag, cdc_dsn),
  cast(
    substring(
      _metadata.file_path
      FROM (position('/Batch', _metadata.file_path) + 6) FOR 1
    ) as int
  ) batchid
FROM
  read_files(
    "${tpcdi_directory}sf=${scale_factor}/Batch[23]",
    format => "csv",
    inferSchema => False,
    header => False,
    sep => "|",
    fileNamePattern => "WatchHistory.txt",
    schema => "cdc_flag STRING COMMENT 'Denotes insert or update', cdc_dsn BIGINT COMMENT 'Database Sequence Number', w_c_id BIGINT COMMENT 'Customer identifier', w_s_symb STRING COMMENT 'Symbol of the security to watch', w_dts TIMESTAMP COMMENT 'Date and Time Stamp for the action', w_action STRING COMMENT 'Whether activating or canceling the watch'"
  );

-- COMMAND ----------

CREATE OR REPLACE VIEW ${catalog}.${wh_db}_stage.v_Prospect AS
SELECT
  *,
  cast(
    substring(
      _metadata.file_path
      FROM
        (position('/Batch', _metadata.file_path) + 6) FOR 1
    ) as int
  ) batchid
FROM
  read_files(
    "${tpcdi_directory}sf=${scale_factor}/Batch*",
    format => "csv",
    inferSchema => False,
    header => False,
    sep => ",",
    fileNamePattern => "Prospect.csv",
    schema => "agencyid STRING COMMENT 'Unique identifier from agency', lastname STRING COMMENT 'Last name', firstname STRING COMMENT 'First name', middleinitial STRING COMMENT 'Middle initial', gender STRING COMMENT '‘M’ or ‘F’ or ‘U’', addressline1 STRING COMMENT 'Postal address', addressline2 STRING COMMENT 'Postal address', postalcode STRING COMMENT 'Postal code', city STRING COMMENT 'City', state STRING COMMENT 'State or province', country STRING COMMENT 'Postal country', phone STRING COMMENT 'Telephone number', income STRING COMMENT 'Annual income', numbercars INT COMMENT 'Cars owned', numberchildren INT COMMENT 'Dependent children', maritalstatus STRING COMMENT '‘S’ or ‘M’ or ‘D’ or ‘W’ or ‘U’', age INT COMMENT 'Current age', creditrating INT COMMENT 'Numeric rating', ownorrentflag STRING COMMENT '‘O’ or ‘R’ or ‘U’', employer STRING COMMENT 'Name of employer', numbercreditcards INT COMMENT 'Credit cards', networth INT COMMENT 'Estimated total net worth'"
  );

-- COMMAND ----------

-- CREATE OR REPLACE VIEW ${catalog}.${wh_db}_${scale_factor}_stage.v_BatchDate AS
-- SELECT
--   DATE(val [0]) batchdate,
--   INT(batchid) batchid
-- FROM
--   (
--     SELECT
--       split(value, "[|]") val,
--       substring(_metadata.file_path FROM (position('/Batch', _metadata.file_path) + 6) FOR 1) batchid 
--     FROM
--       STREAM(text.`${tpcdi_directory}sf=${scale_factor}/Batch*/BatchDate.txt`)
--   );
-- CREATE OR REPLACE VIEW ${catalog}.${wh_db}_${scale_factor}_stage.v_DimDate AS
-- SELECT
--   BIGINT(val[0]) sk_dateid,
--   DATE(val[1]) datevalue,
--   val[2] datedesc,
--   INT(val[3]) calendaryearid,
--   val[4] calendaryeardesc,
--   INT(val[5]) calendarqtrid,
--   val[6] calendarqtrdesc,
--   INT(val[7]) calendarmonthid,
--   val[8] calendarmonthdesc,
--   INT(val[9]) calendarweekid,
--   val[10] calendarweekdesc,
--   INT(val[11]) dayofweeknum,
--   val[12] dayofweekdesc,
--   INT(val[13]) fiscalyearid,
--   val[14] fiscalyeardesc,
--   INT(val[15]) fiscalqtrid,
--   val[16] fiscalqtrdesc,
--   BOOLEAN(val[17]) holidayflag
-- FROM
--   (
--     SELECT
--       split(value, "[|]") val
--     FROM
--       text.`${tpcdi_directory}sf=${scale_factor}/Batch1/Date.txt`
--   );
-- CREATE OR REPLACE VIEW ${catalog}.${wh_db}_${scale_factor}_stage.v_DimTime AS
-- SELECT
--   BIGINT(val[0]) sk_timeid,
--   val[1] timevalue,
--   INT(val[2]) hourid,
--   val[3] hourdesc,
--   INT(val[4]) minuteid,
--   val[5] minutedesc,
--   INT(val[6]) secondid,
--   val[7] seconddesc,
--   BOOLEAN(val[8]) markethoursflag,
--   BOOLEAN(val[9]) officehoursflag
-- FROM
--   (
--     SELECT
--       split(value, "[|]") val
--     FROM
--       text.`${tpcdi_directory}sf=${scale_factor}/Batch1/Time.txt`
--   );
-- CREATE OR REPLACE VIEW ${catalog}.${wh_db}_${scale_factor}_stage.v_FinWire AS
-- SELECT
--   value,
--   substring(value, 16, 3) rectype
-- FROM 
--   text.`${tpcdi_directory}sf=${scale_factor}/Batch1/FINWIRE[0-9][0-9][0-9][0-9]Q[1-4]`;
-- CREATE OR REPLACE VIEW ${catalog}.${wh_db}_${scale_factor}_stage.v_StatusType AS
-- SELECT
--   val[0] st_id,
--   val[1] st_name
-- FROM
--   (
--     SELECT
--       split(value, "[|]") val
--     FROM
--       text.`${tpcdi_directory}sf=${scale_factor}/Batch1/StatusType.txt`
--   );
-- CREATE OR REPLACE VIEW ${catalog}.${wh_db}_${scale_factor}_stage.v_TaxRate AS
-- SELECT
--   val[0] tx_id,
--   val[1] tx_name,
--   FLOAT(val[2]) tx_rate
-- FROM
--   (
--     SELECT
--       split(value, "[|]") val
--     FROM
--       text.`${tpcdi_directory}sf=${scale_factor}/Batch1/TaxRate.txt`
--   );
-- CREATE OR REPLACE VIEW ${catalog}.${wh_db}_${scale_factor}_stage.v_TradeType AS
-- SELECT
--   val[0] tt_id,
--   val[1] tt_name,
--   INT(val[2]) tt_is_sell,
--   INT(val[3]) tt_is_mrkt
-- FROM
--   (
--     SELECT
--       split(value, "[|]") val
--     FROM
--       text.`${tpcdi_directory}sf=${scale_factor}/Batch1/TradeType.txt`
--   );
-- CREATE OR REPLACE VIEW ${catalog}.${wh_db}_${scale_factor}_stage.v_industry AS
-- SELECT
--   val[0] in_id,
--   val[1] in_name,
--   val[2] in_sc_id
-- FROM
--   (
--     SELECT
--       split(value, "[|]") val
--     FROM
--       text.`${tpcdi_directory}sf=${scale_factor}/Batch1/Industry.txt`
--   )
