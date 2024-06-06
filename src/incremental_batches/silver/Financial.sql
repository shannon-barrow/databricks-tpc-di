-- Databricks notebook source
INSERT INTO ${catalog}.${wh_db}_${scale_factor}.Financial
SELECT 
  sk_companyid,
  cast(substring(value, 1, 4) AS INT) AS fi_year,
  cast(substring(value, 5, 1) AS INT) AS fi_qtr,
  to_date(substring(value, 6, 8), 'yyyyMMdd') AS fi_qtr_start_date,
  cast(substring(value, 22, 17) AS DOUBLE) AS fi_revenue,
  cast(substring(value, 39, 17) AS DOUBLE) AS fi_net_earn,
  cast(substring(value, 56, 12) AS DOUBLE) AS fi_basic_eps,
  cast(substring(value, 68, 12) AS DOUBLE) AS fi_dilut_eps,
  cast(substring(value, 80, 12) AS DOUBLE) AS fi_margin,
  cast(substring(value, 92, 17) AS DOUBLE) AS fi_inventory,
  cast(substring(value, 109, 17) AS DOUBLE) AS fi_assets,
  cast(substring(value, 126, 17) AS DOUBLE) AS fi_liability,
  cast(substring(value, 143, 13) AS BIGINT) AS fi_out_basic,
  cast(substring(value, 156, 13) AS BIGINT) AS fi_out_dilut
FROM ${catalog}.${wh_db}_${scale_factor}_stage.FinWire
JOIN ${catalog}.${wh_db}_${scale_factor}.DimCompany dc
ON
  rectype = 'FIN_${conameorcik}'
  and trim(substring(value, 169, 60)) = dc.${conameorcik}
  AND recdate >= dc.effectivedate 
  AND recdate < dc.enddate
