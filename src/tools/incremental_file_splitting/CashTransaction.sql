-- Databricks notebook source
CREATE WIDGET DROPDOWN scale_factor DEFAULT "10" CHOICES SELECT * FROM (VALUES ("10"), ("100"), ("1000"), ("5000"), ("10000"), ("20000"));
CREATE WIDGET TEXT tpcdi_directory DEFAULT "/Volumes/tpcdi/tpcdi_raw_data/tpcdi_volume/";
CREATE WIDGET TEXT target_dir DEFAULT 'split_data';
CREATE WIDGET TEXT catalog DEFAULT 'tpcdi';

-- COMMAND ----------

CREATE OR REPLACE TABLE ${catalog}.tpcdi_raw_data.rawcashtransaction${scale_factor} (
  cdc_flag STRING,
  cdc_dsn BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 0 INCREMENT BY 1),
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
)

-- COMMAND ----------

INSERT OVERWRITE ${catalog}.tpcdi_raw_data.rawcashtransaction${scale_factor} (
  cdc_flag ,
  accountid , 
  ct_dts , 
  ct_amt , 
  ct_name ,
  event_dt
)
select 
  'I' cdc_flag,
  *,
  date(ct_dts) event_dt
FROM read_files(
  "${tpcdi_directory}sf=${scale_factor}/Batch1",
  format => "csv",
  inferSchema => False,
  header => False,
  sep => "|",
  fileNamePattern => "CashTransaction.txt",
  schema => "accountid BIGINT, ct_dts TIMESTAMP, ct_amt DOUBLE, ct_name STRING"
)