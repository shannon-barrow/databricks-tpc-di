-- Databricks notebook source
CREATE OR REPLACE TABLE ${catalog}.${wh_db}_${scale_factor}.DimBroker (
  ${tgt_schema}
  ${constraints}
)
cluster by (sk_brokerid)
TBLPROPERTIES (${tbl_props});

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
from parquet.`${tpcdi_directory}sf=${scale_factor}/Batch1/HR*.parquet`
WHERE employeejobcode = 314;
