-- Databricks notebook source
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
