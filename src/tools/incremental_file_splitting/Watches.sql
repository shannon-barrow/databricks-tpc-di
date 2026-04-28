-- Databricks notebook source
CREATE WIDGET DROPDOWN scale_factor DEFAULT "10" CHOICES SELECT * FROM (VALUES ("10"), ("100"), ("1000"), ("5000"), ("10000"), ("20000"));
CREATE WIDGET TEXT tpcdi_directory DEFAULT "/Volumes/tpcdi/tpcdi_raw_data/tpcdi_volume/";
CREATE WIDGET TEXT target_dir DEFAULT 'split_data';
CREATE WIDGET TEXT catalog DEFAULT 'tpcdi';

-- COMMAND ----------

CREATE OR REPLACE TABLE ${catalog}.tpcdi_raw_data.rawwatches${scale_factor} (
  cdc_flag STRING,
  cdc_dsn BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 0 INCREMENT BY 1),
  w_c_id BIGINT, 
  w_s_symb STRING, 
  w_dts TIMESTAMP, 
  w_action STRING,
  event_dt DATE
)
PARTITIONED BY (event_dt)
TBLPROPERTIES (
  'delta.autoOptimize.autoCompact' = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true'
)

-- COMMAND ----------

INSERT OVERWRITE ${catalog}.tpcdi_raw_data.rawwatches${scale_factor} (
  cdc_flag ,
  w_c_id , 
  w_s_symb , 
  w_dts , 
  w_action ,
  event_dt
)
select 
  'I' cdc_flag,
  wh.*,
  date(wh.w_dts) event_dt
FROM read_files(
  "${tpcdi_directory}sf=${scale_factor}/Batch1",
  format => "csv",
  inferSchema => False,
  header => False,
  sep => "|",
  fileNamePattern => "WatchHistory.txt",
  schema => "w_c_id BIGINT, w_s_symb STRING, w_dts TIMESTAMP, w_action STRING"
) wh
join (select distinct customerid from ${catalog}.tpcdi_raw_data.rawcustomer${scale_factor}) c
  ON 
    c.customerid = wh.w_c_id
join (select distinct symbol from ${catalog}.tpcdi_raw_data.rawsecurity${scale_factor}) s
  ON 
    s.symbol = wh.w_s_symb