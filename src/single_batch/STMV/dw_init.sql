-- Databricks notebook source
-- CREATE WIDGET DROPDOWN scale_factor DEFAULT "10" CHOICES SELECT * FROM (VALUES ("10"), ("100"), ("1000"), ("5000"), ("10000"));
-- CREATE WIDGET TEXT tpcdi_directory DEFAULT "/Volumes/tpcdi/tpcdi_raw_data/tpcdi_volume/";
-- CREATE WIDGET TEXT wh_db DEFAULT '';
-- CREATE WIDGET TEXT catalog DEFAULT 'tpcdi';
-- CREATE WIDGET DROPDOWN pred_opt DEFAULT "DISABLE" CHOICES SELECT * FROM (VALUES ("ENABLE"), ("DISABLE")); -- Predictive Optimization

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS ${catalog};
GRANT ALL PRIVILEGES ON CATALOG ${catalog} TO `account users`;
DROP DATABASE IF EXISTS ${catalog}.${wh_db} cascade;
CREATE DATABASE ${catalog}.${wh_db};
CREATE DATABASE IF NOT EXISTS ${catalog}.${wh_db}_stage;
DROP MATERIALIZED VIEW IF EXISTS ${catalog}.${wh_db}_stage.dimcustomerstg;
DROP TABLE IF EXISTS ${catalog}.${wh_db}_stage.finwire;

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
