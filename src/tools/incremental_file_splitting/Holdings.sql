-- Databricks notebook source
CREATE WIDGET DROPDOWN scale_factor DEFAULT "10" CHOICES SELECT * FROM (VALUES ("10"), ("100"), ("1000"), ("5000"), ("10000"), ("20000"));
CREATE WIDGET TEXT tpcdi_directory DEFAULT "/Volumes/tpcdi/tpcdi_raw_data/tpcdi_volume/";
CREATE WIDGET TEXT target_dir DEFAULT 'split_data';
CREATE WIDGET TEXT catalog DEFAULT 'tpcdi';

-- COMMAND ----------

CREATE OR REPLACE TABLE ${catalog}.tpcdi_raw_data.rawholdings${scale_factor} (
  cdc_flag STRING,
  cdc_dsn BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 0 INCREMENT BY 1),
  hh_h_t_id BIGINT, 
  hh_t_id BIGINT, 
  hh_before_qty INT, 
  hh_after_qty INT,
  event_dt DATE
)
PARTITIONED BY (event_dt)
TBLPROPERTIES (
  'delta.autoOptimize.autoCompact' = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true'
)

-- COMMAND ----------

INSERT OVERWRITE ${catalog}.tpcdi_raw_data.rawholdings${scale_factor} (
  cdc_flag ,
  hh_h_t_id , 
  hh_t_id , 
  hh_before_qty , 
  hh_after_qty,
  event_dt
)
select 
  'I' cdc_flag,
  h.*,
  dt.event_dt
FROM read_files(
  "${tpcdi_directory}sf=${scale_factor}/Batch1",
  format => "csv",
  inferSchema => False,
  header => False,
  sep => "|",
  fileNamePattern => "HoldingHistory.txt",
  schema => "hh_h_t_id BIGINT, hh_t_id BIGINT, hh_before_qty INT, hh_after_qty INT"
) h
JOIN (
  select 
    tradeid,
    max(event_dt) event_dt
  from ${catalog}.tpcdi_raw_data.rawtrade${scale_factor}
  group by all
) dt 
  ON tradeid = hh_t_id