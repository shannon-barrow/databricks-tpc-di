-- Databricks notebook source
INSERT INTO ${catalog}.${wh_db}_${scale_factor}.FactMarketHistory
SELECT 
  s.sk_securityid,
  s.sk_companyid,
  bigint(date_format(dm_date, 'yyyyMMdd')) sk_dateid,
  fmh.dm_close / sum_fi_basic_eps AS peratio,
  (s.dividend / fmh.dm_close) / 100 yield,
  fiftytwoweekhigh,
  sk_fiftytwoweekhighdate,
  fiftytwoweeklow,
  sk_fiftytwoweeklowdate,
  dm_close closeprice,
  dm_high dayhigh,
  dm_low daylow,
  dm_vol volume,
  fmh.batchid
FROM ${catalog}.${wh_db}_${scale_factor}_stage.DailyMarketIncremental fmh
JOIN ${catalog}.${wh_db}_${scale_factor}.DimSecurity s 
  ON 
    s.symbol = fmh.dm_s_symb
    AND s.iscurrent
LEFT JOIN ${catalog}.${wh_db}_${scale_factor}_stage.CompanyFinancialsStg f 
  ON 
    f.sk_companyid = s.sk_companyid
    AND quarter(fmh.dm_date) = quarter(fi_qtr_start_date)
    AND year(fmh.dm_date) = year(fi_qtr_start_date)
WHERE fmh.batchid = cast(${batch_id} as int)
