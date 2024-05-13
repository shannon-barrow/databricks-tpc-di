-- Databricks notebook source
CREATE MATERIALIZED VIEW IF NOT EXISTS ${catalog}.${wh_db}_${scale_factor}.FactMarketHistory AS
with CompanyFinancialsStg as (
  SELECT
    sk_companyid,
    fi_qtr_start_date,
    sum(fi_basic_eps) OVER (PARTITION BY companyid ORDER BY fi_qtr_start_date ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING) sum_fi_basic_eps
  FROM ${catalog}.${wh_db}_${scale_factor}.Financial
  JOIN ${catalog}.${wh_db}_${scale_factor}.DimCompany
    USING (sk_companyid)
)
SELECT 
  s.sk_securityid,
  s.sk_companyid,
  bigint(date_format(dm_date, 'yyyyMMdd')) sk_dateid,
  try_divide(fmh.dm_close, sum_fi_basic_eps) AS peratio,
  (try_divide(s.dividend, fmh.dm_close)) / 100 yield,
  fiftytwoweekhigh,
  sk_fiftytwoweekhighdate,
  fiftytwoweeklow,
  sk_fiftytwoweeklowdate,
  dm_close closeprice,
  dm_high dayhigh,
  dm_low daylow,
  dm_vol volume,
  fmh.batchid
FROM ${catalog}.${wh_db}_${scale_factor}_stage.v_DailyMarketIncremental fmh
JOIN ${catalog}.${wh_db}_${scale_factor}.DimSecurity s 
  ON 
    s.symbol = fmh.dm_s_symb
    AND fmh.dm_date >= s.effectivedate 
    AND fmh.dm_date < s.enddate
LEFT JOIN CompanyFinancialsStg f 
  ON 
    f.sk_companyid = s.sk_companyid
    AND quarter(fmh.dm_date) = quarter(fi_qtr_start_date)
    AND year(fmh.dm_date) = year(fi_qtr_start_date)
