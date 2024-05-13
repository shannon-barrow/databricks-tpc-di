-- Databricks notebook source
CREATE MATERIALIZED VIEW IF NOT EXISTS ${catalog}.${wh_db}_${scale_factor}.DimBroker AS 
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
FROM ${catalog}.${wh_db}_${scale_factor}_stage.v_HR
WHERE employeejobcode = 314;
