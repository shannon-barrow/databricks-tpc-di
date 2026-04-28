-- Databricks notebook source
CREATE WIDGET DROPDOWN scale_factor DEFAULT "10" CHOICES SELECT * FROM (VALUES ("10"), ("100"), ("1000"), ("5000"), ("10000"), ("20000"));
CREATE WIDGET TEXT tpcdi_directory DEFAULT "/Volumes/tpcdi/tpcdi_raw_data/tpcdi_volume/";
CREATE WIDGET TEXT target_dir DEFAULT 'split_data';
CREATE WIDGET TEXT catalog DEFAULT 'tpcdi';

-- COMMAND ----------

CREATE OR REPLACE TABLE ${catalog}.tpcdi_raw_data.rawtrade${scale_factor} (
  cdc_flag STRING,
  cdc_dsn BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 0 INCREMENT BY 1),
  tradeid BIGINT,
  t_dts TIMESTAMP,
  status STRING,
  t_tt_id STRING,
  cashflag TINYINT,
  t_s_symb STRING,
  quantity INT,
  bidprice DOUBLE,
  t_ca_id BIGINT,
  executedby STRING,
  tradeprice DOUBLE,
  fee DOUBLE,
  commission DOUBLE,
  tax DOUBLE,
  event_dt DATE
)
PARTITIONED BY (event_dt)
TBLPROPERTIES (
  'delta.autoOptimize.autoCompact' = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true'
)

-- COMMAND ----------

INSERT OVERWRITE ${catalog}.tpcdi_raw_data.rawtrade${scale_factor} (
  cdc_flag ,
  tradeid ,
  t_dts ,
  status ,
  t_tt_id ,
  cashflag ,
  t_s_symb ,
  quantity ,
  bidprice ,
  t_ca_id ,
  executedby ,
  tradeprice ,
  fee ,
  commission ,
  tax ,
  event_dt
)
select 
  if(
    row_number() over (
      partition by tradeid 
      order by th_dts
    ) = 1,
    'I',
    'U'
  ) cdc_flag,
  tradeid,
  th_dts t_dts,
  status,
  t_tt_id,
  t_is_cash cashflag,
  t_s_symb,
  quantity,
  bidprice,
  t_ca_id,
  executedby,
  case when status = 'CMPT' then tradeprice end tradeprice,
  case when status = 'CMPT' then fee end fee,
  case when status = 'CMPT' then commission end commission,
  case when status = 'CMPT' then tax end tax,
  date(th_dts) event_dt
FROM read_files(
  "${tpcdi_directory}sf=${scale_factor}/Batch1",
  format => "csv",
  inferSchema => False,
  header => False,
  sep => "|",
  fileNamePattern => "TradeHistory.txt",
  schema => "tradeid BIGINT, th_dts TIMESTAMP, status STRING"
) TradeHistory
JOIN read_files(
  "${tpcdi_directory}sf=${scale_factor}/Batch1",
  format => "csv",
  inferSchema => False,
  header => False,
  sep => "|",
  fileNamePattern => "Trade.txt",
  schema => "t_id BIGINT, t_dts TIMESTAMP, t_st_id STRING, t_tt_id STRING, t_is_cash TINYINT, t_s_symb STRING, quantity INT, bidprice DOUBLE, t_ca_id BIGINT, executedby STRING, tradeprice DOUBLE, fee DOUBLE, commission DOUBLE, tax DOUBLE"
) trade
ON
  trade.t_id = TradeHistory.tradeid