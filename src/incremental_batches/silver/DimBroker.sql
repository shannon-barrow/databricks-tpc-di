-- Databricks notebook source
-- CREATE WIDGET DROPDOWN scale_factor DEFAULT "10" CHOICES SELECT * FROM (VALUES ("10"), ("100"), ("1000"), ("5000"), ("10000"));
-- CREATE WIDGET TEXT tpcdi_directory DEFAULT "/Volumes/tpcdi/tpcdi_raw_data/tpcdi_volume/";
-- CREATE WIDGET TEXT wh_db DEFAULT '';
-- CREATE WIDGET TEXT catalog DEFAULT 'tpcdi';

-- COMMAND ----------

-- CREATE OR REPLACE TABLE ${catalog}.${wh_db}_${scale_factor}.DimBroker (
--   ${tgt_schema}
--   ${constraints}
-- )
-- TBLPROPERTIES (${tbl_props});

-- COMMAND ----------

INSERT OVERWRITE ${catalog}.${wh_db}_${scale_factor}.DimBroker
SELECT
  employeeid sk_brokerid,
  employeeid brokerid,
  managerid,
  employeefirstname firstname,
  employeelastname lastname,
  employeemi middleinitial,
  employeebranch branch,
  employeeoffice office,
  employeephone phone,
  true iscurrent,
  1 batchid,
  (SELECT min(to_date(datevalue)) as effectivedate FROM ${catalog}.${wh_db}_${scale_factor}.DimDate) effectivedate,
  date('9999-12-31') enddate
FROM read_files(
  "${tpcdi_directory}sf=${scale_factor}/Batch1",
  format => "csv",
  inferSchema => False, 
  header => False,
  sep => ",",
  fileNamePattern => "HR.csv", 
  schema => "employeeid BIGINT, managerid BIGINT, employeefirstname STRING, employeelastname STRING, employeemi STRING, employeejobcode STRING , employeebranch STRING, employeeoffice STRING, employeephone STRING"
)
WHERE employeejobcode = 314;
