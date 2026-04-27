-- Databricks notebook source
CREATE WIDGET DROPDOWN scale_factor DEFAULT "10" CHOICES SELECT * FROM (VALUES ("10"), ("100"), ("1000"), ("5000"), ("10000"), ("20000"));
CREATE WIDGET TEXT tpcdi_directory DEFAULT "/Volumes/tpcdi/tpcdi_raw_data/tpcdi_volume/";
CREATE WIDGET TEXT target_dir DEFAULT 'split_data';
CREATE WIDGET TEXT catalog DEFAULT 'tpcdi';

-- COMMAND ----------

CREATE OR REPLACE TABLE ${catalog}.tpcdi_raw_data.rawdailymarket${scale_factor} (
  cdc_flag STRING,
  cdc_dsn BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 0 INCREMENT BY 1),
  dm_date DATE, 
  dm_s_symb STRING, 
  dm_close DOUBLE, 
  dm_high DOUBLE, 
  dm_low DOUBLE, 
  dm_vol INT
)
PARTITIONED BY (dm_date);

-- COMMAND ----------

INSERT OVERWRITE ${catalog}.tpcdi_raw_data.rawdailymarket${scale_factor} (
  cdc_flag ,
  dm_date , 
  dm_s_symb , 
  dm_close , 
  dm_high , 
  dm_low , 
  dm_vol
)
select 
  'I' cdc_flag,
  dm.*
FROM read_files(
  "${tpcdi_directory}sf=${scale_factor}/Batch1",
  format => "csv",
  inferSchema => False,
  header => False,
  sep => "|",
  fileNamePattern => "DailyMarket.txt",
  schema => "dm_date DATE, dm_s_symb STRING, dm_close DOUBLE, dm_high DOUBLE, dm_low DOUBLE, dm_vol INT"
) dm