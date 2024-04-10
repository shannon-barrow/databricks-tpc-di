-- Databricks notebook source
CREATE MATERIALIZED VIEW IF NOT EXISTS ${catalog}.${wh_db}.DimBroker AS 
SELECT
  cast(employeeid as BIGINT) sk_brokerid, -- BrokerID is already unique for this snapshot and no incremental updates
  cast(employeeid as BIGINT) brokerid,
  cast(managerid as BIGINT) managerid,
  employeefirstname firstname,
  employeelastname lastname,
  employeemi middleinitial,
  employeebranch branch,
  employeeoffice office,
  employeephone phone,
  true iscurrent,
  1 batchid,
  (SELECT min(to_date(datevalue)) as effectivedate FROM ${catalog}.${wh_db}.DimDate) effectivedate,
  date('9999-12-31') enddate
FROM ${catalog}.${wh_db}_stage.v_HR
WHERE employeejobcode = 314
