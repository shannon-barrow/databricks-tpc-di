-- Databricks notebook source
-- CREATE WIDGET DROPDOWN scale_factor DEFAULT "10" CHOICES SELECT * FROM (VALUES ("10"), ("100"), ("1000"), ("5000"), ("10000"));
-- CREATE WIDGET TEXT tpcdi_directory DEFAULT "/Volumes/tpcdi/tpcdi_raw_data/tpcdi_volume/";
-- CREATE WIDGET TEXT wh_db DEFAULT '';
-- CREATE WIDGET TEXT catalog DEFAULT 'tpcdi';
-- CREATE WIDGET DROPDOWN batch_id DEFAULT '2' CHOICES SELECT * FROM (VALUES ("2"), ("3"));

-- COMMAND ----------

INSERT INTO ${catalog}.${wh_db}_${scale_factor}.FactCashBalances
SELECT
  a.sk_customerid, 
  a.sk_accountid, 
  bigint(date_format(datevalue, 'yyyyMMdd')) sk_dateid,
  cash,
  c.batchid
FROM ${catalog}.${wh_db}_${scale_factor}_stage.CashTransactionIncremental c 
JOIN ${catalog}.${wh_db}_${scale_factor}.DimAccount a 
  ON 
    c.accountid = a.accountid
    AND a.iscurrent
WHERE c.batchid = cast(${batch_id} as int)
