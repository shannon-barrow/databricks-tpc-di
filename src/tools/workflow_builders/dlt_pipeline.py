"""Builder for the DLT pipeline definition.

Replaces `jinja_templates/dlt_pipeline.json`. Submitted to the
`/api/2.0/pipelines` endpoint (not Jobs API).
"""
from __future__ import annotations


# Bronze table descriptors used by the DLT bronze notebook. JSON-encoded as a
# single string and read back inside the notebook via json.loads. Identical to
# the value the Jinja template emitted; kept as one literal blob so future
# schema edits happen here in one place.
_BRONZE_TABLES_JSON = (
    '[{"table": "TaxRate", "filename": "TaxRate.txt", "raw_schema": "tx_id STRING NOT NULL COMMENT \'Tax rate code\', tx_name STRING NOT NULL COMMENT \'Tax rate description\', tx_rate FLOAT NOT NULL COMMENT \'Tax rate\'"}, '
    '{"table": "DimTime", "filename": "Time.txt", "raw_schema": "sk_timeid BIGINT NOT NULL COMMENT \'Surrogate key for the time\', timevalue STRING NOT NULL COMMENT \'The time stored appropriately for doing\', hourid INT NOT NULL COMMENT \'Hour number as a number e.g. 01\', hourdesc STRING NOT NULL COMMENT \'Hour number as text e.g. 01\', minuteid INT NOT NULL COMMENT \'Minute as a number e.g. 23\', minutedesc STRING NOT NULL COMMENT \'Minute as text e.g. 01:23\', secondid INT NOT NULL COMMENT \'Second as a number e.g. 45\', seconddesc STRING NOT NULL COMMENT \'Second as text e.g. 01:23:45\', markethoursflag BOOLEAN COMMENT \'Indicates a time during market hours\', officehoursflag BOOLEAN COMMENT \'Indicates a time during office hours\'"}, '
    '{"table": "DimDate", "filename": "Date.txt", "raw_schema": "sk_dateid BIGINT NOT NULL COMMENT \'Surrogate key for the date\', datevalue DATE NOT NULL COMMENT \'The date stored appropriately for doing comparisons in the Data Warehouse\', datedesc STRING NOT NULL COMMENT \'The date in full written form e.g. July 7 2004\', calendaryearid INT NOT NULL COMMENT \'Year number as a number\', calendaryeardesc STRING NOT NULL COMMENT \'Year number as text\', calendarqtrid INT NOT NULL COMMENT \'Quarter as a number e.g. 20042\', calendarqtrdesc STRING NOT NULL COMMENT \'Quarter as text e.g. 2004 Q2\', calendarmonthid INT NOT NULL COMMENT \'Month as a number e.g. 20047\', calendarmonthdesc STRING NOT NULL COMMENT \'Month as text e.g. 2004 July\', calendarweekid INT NOT NULL COMMENT \'Week as a number e.g. 200428\', calendarweekdesc STRING NOT NULL COMMENT \'Week as text e.g. 2004-W28\', dayofweeknum INT NOT NULL COMMENT \'Day of week as a number e.g. 3\', dayofweekdesc STRING NOT NULL COMMENT \'Day of week as text e.g. Wednesday\', fiscalyearid INT NOT NULL COMMENT \'Fiscal year as a number e.g. 2005\', fiscalyeardesc STRING NOT NULL COMMENT \'Fiscal year as text e.g. 2005\', fiscalqtrid INT NOT NULL COMMENT \'Fiscal quarter as a number e.g. 20051\', fiscalqtrdesc STRING NOT NULL COMMENT \'Fiscal quarter as text e.g. 2005 Q1\', holidayflag BOOLEAN COMMENT \'Indicates holidays\'"}, '
    '{"table": "TradeType", "filename": "TradeType.txt", "raw_schema": "tt_id STRING NOT NULL COMMENT \'Trade type code\', tt_name STRING NOT NULL COMMENT \'Trade type description\', tt_is_sell INT NOT NULL COMMENT \'Flag indicating a sale\', tt_is_mrkt INT NOT NULL COMMENT \'Flag indicating a market order\'"}, '
    '{"table": "BatchDate", "path": "Batch*", "filename": "BatchDate.txt", "raw_schema": "batchdate DATE NOT NULL COMMENT \'Batch date\'", "tgt_query": "*, cast(substring(_metadata.file_path FROM (position(\'/Batch\', _metadata.file_path) + 6) FOR 1) as int) batchid", "add_tgt_schema": ", batchid INT NOT NULL COMMENT \'Batch ID when this record was inserted\'"}, '
    '{"table": "StatusType", "raw_schema": "st_id STRING NOT NULL COMMENT \'Status code\', st_name STRING NOT NULL COMMENT \'Status description\'", "filename": "StatusType.txt"}, '
    '{"table": "Industry", "filename": "Industry.txt", "raw_schema": "in_id STRING NOT NULL COMMENT \'Industry code\', in_name STRING NOT NULL COMMENT \'Industry description\', in_sc_id STRING NOT NULL COMMENT \'Sector identifier\'"}, '
    '{"table": "finwire", "filename": "FINWIRE[0-9][0-9][0-9][0-9]Q[1-4]", "raw_schema": "value STRING COMMENT \'Pre-parsed String Values of all FinWire files\'", "part": "rectype", "add_tgt_schema": ", rectype STRING COMMENT \'Indicates the type of table into which this record will eventually be parsed: CMP FIN or SEC\'", "tgt_query": "*, substring(value, 16, 3) rectype"}]'
)

_DIM_BROKER_SCHEMA = "sk_brokerid BIGINT COMMENT 'Surrogate key for broker', brokerid BIGINT COMMENT 'Natural key for broker', managerid BIGINT COMMENT 'Natural key for manager\u2019s HR record', firstname STRING COMMENT 'First name', lastname STRING COMMENT 'Last Name', middleinitial STRING COMMENT 'Middle initial', branch STRING COMMENT 'Facility in which employee has office', office STRING COMMENT 'Office number or description', phone STRING COMMENT 'Employee phone number', iscurrent BOOLEAN COMMENT 'True if this is the current record', batchid INT COMMENT 'Batch ID when this record was inserted', effectivedate DATE COMMENT 'Beginning of date range when this record was the current record', enddate DATE COMMENT 'Ending of date range when this record was the current record. A record that is not expired will use the date 9999-12-31.'"

_DIM_COMPANY_SCHEMA = "sk_companyid BIGINT COMMENT 'Surrogate key for CompanyID', companyid BIGINT COMMENT 'Company identifier (CIK number)', status STRING COMMENT 'Company status', name STRING COMMENT 'Company name', industry STRING COMMENT 'Company\u2019s industry', sprating STRING COMMENT 'Standard & Poor company\u2019s rating', islowgrade BOOLEAN COMMENT 'True if this company is low grade', ceo STRING COMMENT 'CEO name', addressline1 STRING COMMENT 'Address Line 1', addressline2 STRING COMMENT 'Address Line 2', postalcode STRING COMMENT 'Zip or postal code', city STRING COMMENT 'City', stateprov STRING COMMENT 'State or Province', country STRING COMMENT 'Country', description STRING COMMENT 'Company description', foundingdate DATE COMMENT 'Date the company was founded', iscurrent BOOLEAN COMMENT 'True if this is the current record', batchid INT COMMENT 'Batch ID when this record was inserted', effectivedate DATE COMMENT 'Beginning of date range when this record was the current record', enddate DATE COMMENT 'Ending of date range when this record was the current record. A record that is not expired will use the date 9999-12-31.'"

_FINANCIAL_SCHEMA = "sk_companyid BIGINT COMMENT 'Company SK.', fi_year INT COMMENT 'Year of the quarter end.', fi_qtr INT COMMENT 'Quarter number that the financial information is for: valid values 1, 2, 3, 4.', fi_qtr_start_date DATE COMMENT 'Start date of quarter.', fi_revenue DOUBLE COMMENT 'Reported revenue for the quarter.', fi_net_earn DOUBLE COMMENT 'Net earnings reported for the quarter.', fi_basic_eps DOUBLE COMMENT 'Basic earnings per share for the quarter.', fi_dilut_eps DOUBLE COMMENT 'Diluted earnings per share for the quarter.', fi_margin DOUBLE COMMENT 'Profit divided by revenues for the quarter.', fi_inventory DOUBLE COMMENT 'Value of inventory on hand at the end of quarter.', fi_assets DOUBLE COMMENT 'Value of total assets at the end of the quarter.', fi_liability DOUBLE COMMENT 'Value of total liabilities at the end of the quarter.', fi_out_basic BIGINT COMMENT 'Average number of shares outstanding (basic).', fi_out_dilut BIGINT COMMENT 'Average number of shares outstanding (diluted).'"

_DIM_SECURITY_SCHEMA = "sk_securityid BIGINT COMMENT 'Surrogate key for Symbol', symbol STRING COMMENT 'Identifies security on ticker', issue STRING COMMENT 'Issue type', status STRING COMMENT 'Status type', name STRING COMMENT 'Security name', exchangeid STRING COMMENT 'Exchange the security is traded on', sk_companyid BIGINT COMMENT 'Company issuing security', sharesoutstanding BIGINT COMMENT 'Shares outstanding', firsttrade DATE COMMENT 'Date of first trade', firsttradeonexchange DATE COMMENT 'Date of first trade on this exchange', dividend DOUBLE COMMENT 'Annual dividend per share', iscurrent BOOLEAN COMMENT 'True if this is the current record', batchid INT COMMENT 'Batch ID when this record was inserted', effectivedate DATE COMMENT 'Beginning of date range when this record was the current record', enddate DATE COMMENT 'Ending of date range when this record was the current record. A record that is not expired will use the date 9999-12-31.'"

_CUSTOMER_INCREMENTAL_SCHEMA = "cdc_flag STRING COMMENT 'Denotes insert or update', cdc_dsn BIGINT COMMENT 'Database Sequence Number', customerid BIGINT COMMENT 'Customer identifier', taxid STRING COMMENT 'Customer\u2019s tax identifier', status STRING COMMENT 'Customer status type identifier', lastname STRING COMMENT 'Primary Customers last name.', firstname STRING COMMENT 'Primary Customers first name.', middleinitial STRING COMMENT 'Primary Customers middle initial', gender STRING COMMENT 'Gender of the primary customer', tier TINYINT COMMENT 'Customer tier', dob DATE COMMENT 'Customer\u2019s date of birth, as YYYY-MM-DD.', addressline1 STRING COMMENT 'Address Line 1', addressline2 STRING COMMENT 'Address Line 2', postalcode STRING COMMENT 'Zip or postal code', city STRING COMMENT 'City', stateprov STRING COMMENT 'State or province', country STRING COMMENT 'Country', c_ctry_1 STRING COMMENT 'Country code for Customers phone 1.', c_area_1 STRING COMMENT 'Area code for customer\u2019s phone 1.', c_local_1 STRING COMMENT 'Local number for customer\u2019s phone 1.', c_ext_1 STRING COMMENT 'Extension number for Customer\u2019s phone 1.', c_ctry_2 STRING COMMENT 'Country code for Customers phone 2.', c_area_2 STRING COMMENT 'Area code for Customer\u2019s phone 2.', c_local_2 STRING COMMENT 'Local number for Customer\u2019s phone 2.', c_ext_2 STRING COMMENT 'Extension number for Customer\u2019s phone 2.', c_ctry_3 STRING COMMENT 'Country code for Customers phone 3.', c_area_3 STRING COMMENT 'Area code for Customer\u2019s phone 3.', c_local_3 STRING COMMENT 'Local number for Customer\u2019s phone 3.', c_ext_3 STRING COMMENT 'Extension number for Customer\u2019s phone 3.', email1 STRING COMMENT 'Customers e-mail address 1.', email2 STRING COMMENT 'Customers e-mail address 2.', lcl_tx_id STRING COMMENT 'Customers local tax rate', nat_tx_id STRING COMMENT 'Customers national tax rate'"

_DIM_CUSTOMER_SCHEMA = "sk_customerid BIGINT COMMENT 'Surrogate key for CustomerID', customerid BIGINT COMMENT 'Customer identifier', taxid STRING COMMENT 'Customer\u2019s tax identifier', status STRING COMMENT 'Customer status type', lastname STRING COMMENT 'Customers last name.', firstname STRING COMMENT 'Customers first name.', middleinitial STRING COMMENT 'Customers middle name initial', gender STRING COMMENT 'Gender of the customer', tier TINYINT COMMENT 'Customer tier', dob DATE COMMENT 'Customer\u2019s date of birth.', addressline1 STRING COMMENT 'Address Line 1', addressline2 STRING COMMENT 'Address Line 2', postalcode STRING COMMENT 'Zip or Postal Code', city STRING COMMENT 'City', stateprov STRING COMMENT 'State or Province', country STRING COMMENT 'Country', phone1 STRING COMMENT 'Phone number 1', phone2 STRING COMMENT 'Phone number 2', phone3 STRING COMMENT 'Phone number 3', email1 STRING COMMENT 'Email address 1', email2 STRING COMMENT 'Email address 2', nationaltaxratedesc STRING COMMENT 'National Tax rate description', nationaltaxrate FLOAT COMMENT 'National Tax rate', localtaxratedesc STRING COMMENT 'Local Tax rate description', localtaxrate FLOAT COMMENT 'Local Tax rate', agencyid STRING COMMENT 'Agency identifier', creditrating INT COMMENT 'Credit rating', networth INT COMMENT 'Net worth', marketingnameplate STRING COMMENT 'Marketing nameplate', iscurrent BOOLEAN COMMENT 'True if this is the current record', batchid INT COMMENT 'Batch ID when this record was inserted', effectivedate DATE COMMENT 'Beginning of date range when this record was the current record', enddate DATE COMMENT 'Ending of date range when this record was the current record. A record that is not expired will use the date 9999-12-31.'"

_PROSPECT_RAW_SCHEMA = "agencyid STRING COMMENT 'Unique identifier from agency', lastname STRING COMMENT 'Last name', firstname STRING COMMENT 'First name', middleinitial STRING COMMENT 'Middle initial', gender STRING COMMENT '\u2018M\u2019 or \u2018F\u2019 or \u2018U\u2019', addressline1 STRING COMMENT 'Postal address', addressline2 STRING COMMENT 'Postal address', postalcode STRING COMMENT 'Postal code', city STRING COMMENT 'City', state STRING COMMENT 'State or province', country STRING COMMENT 'Postal country', phone STRING COMMENT 'Telephone number', income STRING COMMENT 'Annual income', numbercars INT COMMENT 'Cars owned', numberchildren INT COMMENT 'Dependent children', maritalstatus STRING COMMENT '\u2018S\u2019 or \u2018M\u2019 or \u2018D\u2019 or \u2018W\u2019 or \u2018U\u2019', age INT COMMENT 'Current age', creditrating INT COMMENT 'Numeric rating', ownorrentflag STRING COMMENT '\u2018O\u2019 or \u2018R\u2019 or \u2018U\u2019', employer STRING COMMENT 'Name of employer', numbercreditcards INT COMMENT 'Credit cards', networth INT COMMENT 'Estimated total net worth'"

_ACCOUNT_INCREMENTAL_SCHEMA = "cdc_flag STRING COMMENT 'Denotes insert or update', cdc_dsn BIGINT COMMENT 'Database Sequence Number', accountid BIGINT COMMENT 'Customer account identifier', ca_b_id BIGINT COMMENT 'Identifier of the managing broker', ca_c_id BIGINT COMMENT 'Owning customer identifier', accountDesc STRING COMMENT 'Name of customer account', TaxStatus TINYINT COMMENT 'Tax status of this account', ca_st_id STRING COMMENT 'Customer status type identifier'"

_DIM_ACCOUNT_SCHEMA = "sk_accountid BIGINT COMMENT 'Surrogate key for AccountID', accountid BIGINT COMMENT 'Customer account identifier', sk_brokerid BIGINT COMMENT 'Surrogate key of managing broker', sk_customerid BIGINT COMMENT 'Surrogate key of customer', accountdesc STRING COMMENT 'Name of customer account', taxstatus TINYINT COMMENT 'Tax status of this account', status STRING COMMENT 'Account status, active or closed', iscurrent BOOLEAN GENERATED ALWAYS AS (if(enddate = date('9999-12-31'), true, false)) COMMENT 'True if this is the current record', batchid INT COMMENT 'Batch ID when this record was inserted', effectivedate DATE COMMENT 'Beginning of date range when this record was the current record', enddate DATE COMMENT 'Ending of date range when this record was the current record. A record that is not expired will use the date 9999-12-31.'"

_FACT_MARKET_HISTORY_SCHEMA = "sk_securityid BIGINT COMMENT 'Surrogate key for SecurityID', sk_companyid BIGINT COMMENT 'Surrogate key for CompanyID', sk_dateid BIGINT COMMENT 'Surrogate key for the date', peratio DOUBLE COMMENT 'Price to earnings per share ratio', yield DOUBLE COMMENT 'Dividend to price ratio, as a percentage', fiftytwoweekhigh DOUBLE COMMENT 'Security highest price in last 52 weeks from this day', sk_fiftytwoweekhighdate BIGINT COMMENT 'Earliest date on which the 52 week high price was set', fiftytwoweeklow DOUBLE COMMENT 'Security lowest price in last 52 weeks from this day', sk_fiftytwoweeklowdate BIGINT COMMENT 'Earliest date on which the 52 week low price was set', closeprice DOUBLE COMMENT 'Security closing price on this day', dayhigh DOUBLE COMMENT 'Highest price for the security on this day', daylow DOUBLE COMMENT 'Lowest price for the security on this day', volume INT COMMENT 'Trading volume of the security on this day', batchid INT COMMENT 'Batch ID when this record was inserted'"

_DAILY_MARKET_HISTORICAL_SCHEMA = "dm_date DATE COMMENT 'Date of last completed trading day.', dm_s_symb STRING COMMENT 'Security symbol of the security', dm_close DOUBLE COMMENT 'Closing price of the security on this day.', dm_high DOUBLE COMMENT 'Highest price for the security on this day.', dm_low DOUBLE COMMENT 'Lowest price for the security on this day.', dm_vol INT COMMENT 'Volume of the security on this day.'"

_DAILY_MARKET_INCREMENTAL_SCHEMA = "cdc_flag STRING COMMENT 'Denotes insert or update', cdc_dsn BIGINT COMMENT 'Database Sequence Number', dm_date DATE COMMENT 'Date of last completed trading day.', dm_s_symb STRING COMMENT 'Security symbol of the security', dm_close DOUBLE COMMENT 'Closing price of the security on this day.', dm_high DOUBLE COMMENT 'Highest price for the security on this day.', dm_low DOUBLE COMMENT 'Lowest price for the security on this day.', dm_vol INT COMMENT 'Volume of the security on this day.'"

_DIM_TRADE_SCHEMA = "tradeid BIGINT COMMENT 'Trade identifier', sk_brokerid BIGINT COMMENT 'Surrogate key for BrokerID', sk_createdateid BIGINT COMMENT 'Surrogate key for date created', sk_createtimeid BIGINT COMMENT 'Surrogate key for time created', sk_closedateid BIGINT COMMENT 'Surrogate key for date closed', sk_closetimeid BIGINT COMMENT 'Surrogate key for time closed', status STRING COMMENT 'Trade status', type STRING COMMENT 'Trade type', cashflag BOOLEAN COMMENT 'Is this trade a cash (1) or margin (0) trade?', sk_securityid BIGINT COMMENT 'Surrogate key for SecurityID', sk_companyid BIGINT COMMENT 'Surrogate key for CompanyID', quantity INT COMMENT 'Quantity of securities traded.', bidprice DOUBLE COMMENT 'The requested unit price.', sk_customerid BIGINT COMMENT 'Surrogate key for CustomerID', sk_accountid BIGINT COMMENT 'Surrogate key for AccountID', executedby STRING COMMENT 'Name of person executing the trade.', tradeprice DOUBLE COMMENT 'Unit price at which the security was traded.', fee DOUBLE COMMENT 'Fee charged for placing this trade request', commission DOUBLE COMMENT 'Commission earned on this trade', tax DOUBLE COMMENT 'Amount of tax due on this trade', batchid INT COMMENT 'Batch ID when this record was inserted'"

_TRADE_INCREMENTAL_SCHEMA = "cdc_flag STRING COMMENT 'Denotes insert or update', cdc_dsn BIGINT COMMENT 'Database Sequence Number', t_id BIGINT COMMENT 'Trade identifier.', t_dts TIMESTAMP COMMENT 'Date and time of trade.', t_st_id STRING COMMENT 'Status type identifier', t_tt_id STRING COMMENT 'Trade type identifier', t_is_cash TINYINT COMMENT 'Is this trade a cash (\u20181\u2019) or margin (\u20180\u2019) trade?', t_s_symb STRING COMMENT 'Security symbol of the security', t_qty INT COMMENT 'Quantity of securities traded.', t_bid_price DOUBLE COMMENT 'The requested unit price.', t_ca_id BIGINT COMMENT 'Customer account identifier.', t_exec_name STRING COMMENT 'Name of the person executing the trade.', t_trade_price DOUBLE COMMENT 'Unit price at which the security was traded.', t_chrg DOUBLE COMMENT 'Fee charged for placing this trade request.', t_comm DOUBLE COMMENT 'Commission earned on this trade', t_tax DOUBLE COMMENT 'Amount of tax due on this trade'"

_TRADE_HISTORY_RAW_SCHEMA = "th_t_id BIGINT COMMENT 'Trade identifier. Corresponds to T_ID in the Trade.txt file', th_dts TIMESTAMP COMMENT 'When the trade history was updated.', th_st_id STRING COMMENT 'Status type identifier.'"

_TRADE_HISTORY_SCHEMA = "t_id BIGINT COMMENT 'Trade identifier.', t_dts TIMESTAMP COMMENT 'Date and time of trade.', t_st_id STRING COMMENT 'Status type identifier', t_tt_id STRING COMMENT 'Trade type identifier', t_is_cash TINYINT COMMENT 'Is this trade a cash (\u20181\u2019) or margin (\u20180\u2019) trade?', t_s_symb STRING COMMENT 'Security symbol of the security', t_qty INT COMMENT 'Quantity of securities traded.', t_bid_price DOUBLE COMMENT 'The requested unit price.', t_ca_id BIGINT COMMENT 'Customer account identifier.', t_exec_name STRING COMMENT 'Name of the person executing the trade.', t_trade_price DOUBLE COMMENT 'Unit price at which the security was traded.', t_chrg DOUBLE COMMENT 'Fee charged for placing this trade request.', t_comm DOUBLE COMMENT 'Commission earned on this trade', t_tax DOUBLE COMMENT 'Amount of tax due on this trade'"

_HOLDING_HISTORY_SCHEMA = "hh_h_t_id BIGINT COMMENT 'Trade Identifier of the trade that originally created the holding row.', hh_t_id BIGINT COMMENT 'Trade Identifier of the current trade', hh_before_qty INT COMMENT 'Quantity of this security held before the modifying trade.', hh_after_qty INT COMMENT 'Quantity of this security held after the modifying trade.'"

_HOLDING_INCREMENTAL_SCHEMA = "cdc_flag STRING COMMENT 'Denotes insert or update', cdc_dsn BIGINT COMMENT 'Database Sequence Number', hh_h_t_id BIGINT COMMENT 'Trade Identifier of the trade that originally created the holding row.', hh_t_id BIGINT COMMENT 'Trade Identifier of the current trade', hh_before_qty INT COMMENT 'Quantity of this security held before the modifying trade.', hh_after_qty INT COMMENT 'Quantity of this security held after the modifying trade.'"

_FACT_HOLDINGS_SCHEMA = "tradeid BIGINT COMMENT 'Key for Orignial Trade Indentifier', currenttradeid BIGINT COMMENT 'Key for the current trade', sk_customerid BIGINT COMMENT 'Surrogate key for Customer Identifier', sk_accountid BIGINT COMMENT 'Surrogate key for Account Identifier', sk_securityid BIGINT COMMENT 'Surrogate key for Security Identifier', sk_companyid BIGINT COMMENT 'Surrogate key for Company Identifier', sk_dateid BIGINT COMMENT 'Surrogate key for the date associated with the', sk_timeid BIGINT COMMENT 'Surrogate key for the time associated with the', currentprice DOUBLE COMMENT 'Unit price of this security for the current trade', currentholding INT COMMENT 'Quantity of a security held after the current trade.', batchid INT COMMENT 'Batch ID when this record was inserted'"

_CASH_HISTORY_SCHEMA = "ct_ca_id BIGINT COMMENT 'Customer account identifier', ct_dts TIMESTAMP COMMENT 'Timestamp of when the trade took place', ct_amt DOUBLE COMMENT 'Amount of the cash transaction.', ct_name STRING COMMENT 'Transaction name, or description: e.g. Cash from sale of DuPont stock.'"

_CASH_INCREMENTAL_SCHEMA = "cdc_flag STRING COMMENT 'Denotes insert or update', cdc_dsn BIGINT COMMENT 'Database Sequence Number', ct_ca_id BIGINT COMMENT 'Customer account identifier', ct_dts TIMESTAMP COMMENT 'Timestamp of when the trade took place', ct_amt DOUBLE COMMENT 'Amount of the cash transaction.', ct_name STRING COMMENT 'Transaction name, or description: e.g. Cash from sale of DuPont stock.'"

_FACT_CASH_BALANCES_SCHEMA = "sk_customerid BIGINT COMMENT 'Surrogate key for CustomerID', sk_accountid BIGINT COMMENT 'Surrogate key for AccountID', sk_dateid BIGINT COMMENT 'Surrogate key for the date', cash DOUBLE COMMENT 'Cash balance for the account after applying', batchid INT COMMENT 'Batch ID when this record was inserted'"

_WATCH_HISTORY_SCHEMA = "w_c_id BIGINT COMMENT 'Customer identifier', w_s_symb STRING COMMENT 'Symbol of the security to watch', w_dts TIMESTAMP COMMENT 'Date and Time Stamp for the action', w_action STRING COMMENT 'Whether activating or canceling the watch'"

_WATCH_INCREMENTAL_SCHEMA = "cdc_flag STRING COMMENT 'Denotes insert or update', cdc_dsn BIGINT COMMENT 'Database Sequence Number', w_c_id BIGINT COMMENT 'Customer identifier', w_s_symb STRING COMMENT 'Symbol of the security to watch', w_dts TIMESTAMP COMMENT 'Date and Time Stamp for the action', w_action STRING COMMENT 'Whether activating or canceling the watch'"

_FACT_WATCHES_SCHEMA = "sk_customerid BIGINT COMMENT 'Customer associated with watch list', sk_securityid BIGINT COMMENT 'Security listed on watch list', sk_dateid_dateplaced BIGINT COMMENT 'Date the watch list item was added', sk_dateid_dateremoved BIGINT COMMENT 'Date the watch list item was removed', batchid INT COMMENT 'Batch ID when this record was inserted'"

_DIM_ACCOUNT_STG_SCHEMA = "accountid BIGINT COMMENT 'Customer account identifier', customerid BIGINT COMMENT 'Customer identifier', accountdesc STRING COMMENT 'Name of customer account', taxstatus TINYINT COMMENT 'Tax status of this account', brokerid BIGINT COMMENT 'managing broker identifier', status STRING COMMENT 'Account status, active or closed', batchid INT COMMENT 'Batch ID when this record was inserted', effectivedate DATE GENERATED ALWAYS AS (date(__START_AT)) COMMENT 'Beginning of date range when this record was the current record', enddate DATE GENERATED ALWAYS AS (nvl(date(__END_AT), date('9999-12-31'))) COMMENT 'Ending of date range when this record was the current record. A record that is not expired will use the date 9999-12-31.', __START_AT TIMESTAMP COMMENT 'Beginning of date range when this record was the current record', __END_AT TIMESTAMP COMMENT 'Ending of date range when this record was the current record.'"

# Two variants of DimCustomerStg.schema differ only in iscurrent/effectivedate/enddate
# being either GENERATED columns (CORE) or plain columns (PRO/ADVANCED).
_DIM_CUSTOMER_STG_SCHEMA_CORE = "sk_customerid BIGINT COMMENT 'Surrogate key for CustomerID', customerid BIGINT COMMENT 'Customer identifier', taxid STRING COMMENT 'Customer\u2019s tax identifier', status STRING COMMENT 'Customer status type', lastname STRING COMMENT 'Customers last name.', firstname STRING COMMENT 'Customers first name.', middleinitial STRING COMMENT 'Customers middle name initial', gender STRING COMMENT 'Gender of the customer', tier TINYINT COMMENT 'Customer tier', dob DATE COMMENT 'Customer\u2019s date of birth.', addressline1 STRING COMMENT 'Address Line 1', addressline2 STRING COMMENT 'Address Line 2', postalcode STRING COMMENT 'Zip or Postal Code', city STRING COMMENT 'City', stateprov STRING COMMENT 'State or Province', country STRING COMMENT 'Country', phone1 STRING COMMENT 'Phone number 1', phone2 STRING COMMENT 'Phone number 2', phone3 STRING COMMENT 'Phone number 3', email1 STRING COMMENT 'Email address 1', email2 STRING COMMENT 'Email address 2', lcl_tx_id STRING COMMENT 'Customers local tax rate', nat_tx_id STRING COMMENT 'Customers national tax rate', batchid INT COMMENT 'Batch ID when this record was inserted', iscurrent BOOLEAN COMMENT 'True if this is the current record', effectivedate DATE COMMENT 'Beginning of date range when this record was the current record', enddate DATE COMMENT 'Ending of date range when this record was the current record. A record that is not expired will use the date 9999-12-31.'"
_DIM_CUSTOMER_STG_SCHEMA_NONCORE = "sk_customerid BIGINT COMMENT 'Surrogate key for CustomerID', customerid BIGINT COMMENT 'Customer identifier', taxid STRING COMMENT 'Customer\u2019s tax identifier', status STRING COMMENT 'Customer status type', lastname STRING COMMENT 'Customers last name.', firstname STRING COMMENT 'Customers first name.', middleinitial STRING COMMENT 'Customers middle name initial', gender STRING COMMENT 'Gender of the customer', tier TINYINT COMMENT 'Customer tier', dob DATE COMMENT 'Customer\u2019s date of birth.', addressline1 STRING COMMENT 'Address Line 1', addressline2 STRING COMMENT 'Address Line 2', postalcode STRING COMMENT 'Zip or Postal Code', city STRING COMMENT 'City', stateprov STRING COMMENT 'State or Province', country STRING COMMENT 'Country', phone1 STRING COMMENT 'Phone number 1', phone2 STRING COMMENT 'Phone number 2', phone3 STRING COMMENT 'Phone number 3', email1 STRING COMMENT 'Email address 1', email2 STRING COMMENT 'Email address 2', lcl_tx_id STRING COMMENT 'Customers local tax rate', nat_tx_id STRING COMMENT 'Customers national tax rate', batchid INT COMMENT 'Batch ID when this record was inserted', iscurrent BOOLEAN GENERATED ALWAYS AS (nvl2(__END_AT, false, true)) COMMENT 'True if this is the current record', effectivedate DATE GENERATED ALWAYS AS (date(__START_AT)) COMMENT 'Beginning of date range when this record was the current record', enddate DATE GENERATED ALWAYS AS (nvl(date(__END_AT), date('9999-12-31'))) COMMENT 'Ending of date range when this record was the current record. A record that is not expired will use the date 9999-12-31.', __START_AT TIMESTAMP COMMENT 'Beginning of date range when this record was the current record', __END_AT TIMESTAMP COMMENT 'Ending of date range when this record was the current record.'"

_ADVANCED_CONSTRAINTS = {
    "DimCustomer.constraints": "CONSTRAINT invalid_customer_tier EXPECT (tier IN (1,2,3) AND tier is NOT null)",
    "FactMarketHistory.constraints": "CONSTRAINT no_earnings_for_company EXPECT (PERatio IS NOT NULL)",
    "DimTrade.constraints": "CONSTRAINT invalid_trade_commission EXPECT (commission IS NULL OR commission <= tradeprice * quantity), CONSTRAINT invalid_trade_fee EXPECT (fee IS NULL OR fee <= tradeprice * quantity)",
}


def _libraries(repo_src_path: str, edition: str, scale_factor: int) -> list[dict]:
    libs: list[dict] = []
    if scale_factor < 1000:
        libs.append({"notebook": {"path": f"{repo_src_path}/single_batch/delta_live_tables/CustomerMgmtRaw"}})
    libs.append({"notebook": {"path": f"{repo_src_path}/single_batch/delta_live_tables/bronze"}})
    libs.append({"notebook": {"path": f"{repo_src_path}/single_batch/delta_live_tables/non-incremental"}})
    if edition == "CORE":
        libs.append({"notebook": {"path": f"{repo_src_path}/single_batch/delta_live_tables/incremental"}})
    else:
        libs.append({"notebook": {"path": f"{repo_src_path}/single_batch/delta_live_tables/incremental_apply_changes_into"}})
        libs.append({"notebook": {"path": f"{repo_src_path}/single_batch/delta_live_tables/Apply Changes ONCE Flow"}})
    return libs


def build(*, job_name: str, catalog: str, wh_target: str, edition: str,
          datagen_label: str, scale_factor: int, repo_src_path: str,
          tpcdi_directory: str, serverless: str,
          dlt_worker_node_type: str | None = None,
          dlt_driver_node_type: str | None = None,
          dlt_worker_node_count: int | None = None,
          **_unused) -> dict:
    cust_mgmt_schema = (
        "LIVE" if scale_factor < 1000
        else f"{catalog}.{wh_target}_DLT_{edition}_{datagen_label}_{scale_factor}_stage"
    )
    dim_customer_stg = _DIM_CUSTOMER_STG_SCHEMA_CORE if edition == "CORE" else _DIM_CUSTOMER_STG_SCHEMA_NONCORE

    configuration: dict = {
        "files_directory": tpcdi_directory,
        "scale_factor": str(scale_factor),
        "bronze_tables": _BRONZE_TABLES_JSON,
        "DimBroker.schema": _DIM_BROKER_SCHEMA,
        "DimCompany.schema": _DIM_COMPANY_SCHEMA,
        "Financial.schema": _FINANCIAL_SCHEMA,
        "DimSecurity.schema": _DIM_SECURITY_SCHEMA,
        "CustomerIncremental.schema": _CUSTOMER_INCREMENTAL_SCHEMA,
        "DimCustomer.schema": _DIM_CUSTOMER_SCHEMA,
        "ProspectRaw.schema": _PROSPECT_RAW_SCHEMA,
        "AccountIncremental.schema": _ACCOUNT_INCREMENTAL_SCHEMA,
        "DimAccount.schema": _DIM_ACCOUNT_SCHEMA,
        "FactMarketHistory.schema": _FACT_MARKET_HISTORY_SCHEMA,
        "DailyMarketHistorical.schema": _DAILY_MARKET_HISTORICAL_SCHEMA,
        "DailyMarketIncremental.schema": _DAILY_MARKET_INCREMENTAL_SCHEMA,
        "DimTrade.schema": _DIM_TRADE_SCHEMA,
        "TradeIncremental.schema": _TRADE_INCREMENTAL_SCHEMA,
        "TradeHistoryRaw.schema": _TRADE_HISTORY_RAW_SCHEMA,
        "TradeHistory.schema": _TRADE_HISTORY_SCHEMA,
        "HoldingHistory.schema": _HOLDING_HISTORY_SCHEMA,
        "HoldingIncremental.schema": _HOLDING_INCREMENTAL_SCHEMA,
        "FactHoldings.schema": _FACT_HOLDINGS_SCHEMA,
        "CashTransactionHistory.schema": _CASH_HISTORY_SCHEMA,
        "CashTransactionIncremental.schema": _CASH_INCREMENTAL_SCHEMA,
        "FactCashBalances.schema": _FACT_CASH_BALANCES_SCHEMA,
        "WatchHistory.schema": _WATCH_HISTORY_SCHEMA,
        "WatchIncremental.schema": _WATCH_INCREMENTAL_SCHEMA,
        "FactWatches.schema": _FACT_WATCHES_SCHEMA,
        "DimAccountStg.schema": _DIM_ACCOUNT_STG_SCHEMA,
        "cust_mgmt_schema": cust_mgmt_schema,
    }
    if edition == "ADVANCED":
        configuration.update(_ADVANCED_CONSTRAINTS)
    configuration["DimCustomerStg.schema"] = dim_customer_stg

    pipeline: dict = {
        "name": job_name,
        "pipeline_type": "WORKSPACE",
        "allow_duplicate_names": "true",
        "development": False,
        "continuous": False,
        "channel": "PREVIEW",
        "photon": True,
        "catalog": catalog,
        "target": f"{wh_target}_DLT_{edition}_{datagen_label}_{scale_factor}",
        "data_sampling": False,
        "libraries": _libraries(repo_src_path, edition, scale_factor),
    }
    if serverless == "YES":
        pipeline["serverless"] = True
    else:
        pipeline["serverless"] = False
        pipeline["edition"] = edition
        pipeline["clusters"] = [{
            "label": "default",
            "spark_conf": {"spark.sql.shuffle.partitions": "auto"},
            "node_type_id": dlt_worker_node_type,
            "driver_node_type_id": dlt_driver_node_type,
            "num_workers": dlt_worker_node_count,
        }]
    pipeline["configuration"] = configuration
    return pipeline
