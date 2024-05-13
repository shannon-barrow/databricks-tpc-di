-- Databricks notebook source
CREATE MATERIALIZED VIEW IF NOT EXISTS ${catalog}.${wh_db}_${scale_factor}.FactWatches AS
with Watches as (
  SELECT 
    wh.w_c_id customerid,
    wh.w_s_symb symbol,        
    date(min(w_dts)) dateplaced,
    date(max(if(w_action = 'CNCL', w_dts, null))) dateremoved,
    min(batchid) batchid
  FROM (
    SELECT * FROM ${catalog}.${wh_db}_${scale_factor}_stage.v_WatchHistory
    UNION ALL
    SELECT * FROM ${catalog}.${wh_db}_${scale_factor}_stage.v_WatchIncremental) wh
  GROUP BY 
    w_c_id,
    w_s_symb
)
select
  c.sk_customerid sk_customerid,
  s.sk_securityid sk_securityid,
  bigint(date_format(dateplaced, 'yyyyMMdd')) sk_dateid_dateplaced,
  bigint(date_format(dateremoved, 'yyyyMMdd')) sk_dateid_dateremoved,
  wh.batchid 
from Watches wh
JOIN ${catalog}.${wh_db}_${scale_factor}.DimSecurity s 
  ON 
    s.symbol = wh.symbol
    AND wh.dateplaced >= s.effectivedate 
    AND wh.dateplaced < s.enddate
JOIN ${catalog}.${wh_db}_${scale_factor}.DimCustomer c 
  ON
    wh.customerid = c.customerid
    AND wh.dateplaced >= c.effectivedate 
    AND wh.dateplaced < c.enddate
