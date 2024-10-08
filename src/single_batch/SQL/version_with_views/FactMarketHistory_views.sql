-- Databricks notebook source
-- CREATE WIDGET DROPDOWN scale_factor DEFAULT "10" CHOICES SELECT * FROM (VALUES ("10"), ("100"), ("1000"), ("5000"), ("10000"));
-- CREATE WIDGET TEXT tpcdi_directory DEFAULT "/Volumes/tpcdi/tpcdi_raw_data/tpcdi_volume/";
-- CREATE WIDGET TEXT wh_db DEFAULT '';
-- CREATE WIDGET TEXT catalog DEFAULT 'tpcdi';

-- COMMAND ----------

SET timezone = Etc/UTC;
INSERT OVERWRITE ${catalog}.${wh_db}_${scale_factor}.FactMarketHistory
with CompanyFinancialsStg as (
  SELECT
    f.sk_companyid,
    fi_qtr_start_date,
    sum(fi_basic_eps) OVER (PARTITION BY companyid ORDER BY fi_qtr_start_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) - fi_basic_eps sum_fi_basic_eps
  FROM ${catalog}.${wh_db}_${scale_factor}.Financial f
  JOIN ${catalog}.${wh_db}_${scale_factor}.DimCompany d
    on f.sk_companyid = d.sk_companyid
)
SELECT 
  s.sk_securityid,
  s.sk_companyid,
  bigint(replace(dm_date, '-','')) sk_dateid,
  --bigint(date_format(dm_date, 'yyyyMMdd')) sk_dateid,
  try_divide(fmh.dm_close, sum_fi_basic_eps) AS peratio,
  (try_divide(s.dividend, fmh.dm_close)) / 100 yield,
  fiftytwoweekhigh,
  bigint(replace(fiftytwoweekhighdate, '-','')) sk_fiftytwoweekhighdate,
  fiftytwoweeklow,
  bigint(replace(fiftytwoweeklowdate, '-','')) sk_fiftytwoweeklowdate,
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
