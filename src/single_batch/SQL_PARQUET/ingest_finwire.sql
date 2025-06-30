-- Databricks notebook source
-- CREATE WIDGET DROPDOWN scale_factor DEFAULT "10" CHOICES SELECT * FROM (VALUES ("10"), ("100"), ("1000"), ("5000"), ("10000"));
-- CREATE WIDGET TEXT tpcdi_directory DEFAULT "/Volumes/tpcdi/tpcdi_raw_data/tpcdi_volume/";
-- CREATE WIDGET TEXT wh_db DEFAULT '';
-- CREATE WIDGET TEXT catalog DEFAULT 'tpcdi';

-- COMMAND ----------

CREATE OR REPLACE TABLE ${catalog}.${wh_db}_${scale_factor}_stage.FinWire (
  rectype STRING COMMENT 'Indicates the type of table into which this record will eventually be parsed: CMP FIN or SEC',
  recdate DATE COMMENT 'Date of the record',
  value STRING COMMENT 'Pre-parsed String Values of all FinWire files'
) 
PARTITIONED BY (rectype)
TBLPROPERTIES (${tbl_props});

-- COMMAND ----------

INSERT OVERWRITE ${catalog}.${wh_db}_${scale_factor}_stage.finwire
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
  substring(value, 19) value
FROM parquet.`${tpcdi_directory}sf=${scale_factor}/Batch1/FINWIRE*`;
