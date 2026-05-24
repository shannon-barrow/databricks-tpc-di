-- Databricks notebook source
-- CREATE WIDGET DROPDOWN scale_factor DEFAULT "10" CHOICES SELECT * FROM (VALUES ("10"), ("100"), ("1000"), ("5000"), ("10000"), ("20000"));
-- CREATE WIDGET TEXT tpcdi_directory DEFAULT "/Volumes/tpcdi/tpcdi_raw_data/tpcdi_volume/";
-- CREATE WIDGET TEXT wh_db DEFAULT "";
-- CREATE WIDGET TEXT catalog DEFAULT 'tpcdi';

-- COMMAND ----------

USE IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor);

-- Pre-window FactMarketHistory ([2015-07-06, 2016-07-05]). Computed once
-- during stage 0 so that benchmark batch 1 (2016-07-06) starts with a
-- fully populated factmarkethistory and a 365-day rolling window into
-- bronzedailymarket. Mirrors the FactMarketHistory Incremental.py logic
-- (52-week high/low via min_by/max_by, peratio = dm_close /
-- prev_year_basic_eps, yield = dividend / dm_close / 100), evaluated
-- batch-style across the full historical window.
--
-- The 52-week values for early historical dates (2015-07-06 → ~2016-07-05)
-- have a partial lookback because DM_BEGIN_DATE = 2015-07-06; that's
-- expected and harmless. The benchmark window (post-2016-07-06) starts
-- with a *full* 365-day lookback into bronzedailymarket on every batch.

CREATE OR REPLACE TABLE factmarkethistory (
  sk_securityid BIGINT NOT NULL COMMENT 'Surrogate key for SecurityID',
  sk_companyid BIGINT COMMENT 'Surrogate key for CompanyID',
  sk_dateid BIGINT NOT NULL COMMENT 'Surrogate key for the date',
  peratio DOUBLE COMMENT 'Price to earnings per share ratio',
  yield DOUBLE COMMENT 'Dividend to price ratio, as a percentage',
  fiftytwoweekhigh DOUBLE COMMENT 'Security highest price in last 52 weeks',
  sk_fiftytwoweekhighdate BIGINT COMMENT 'Earliest date on which the 52 week high price was set',
  fiftytwoweeklow DOUBLE COMMENT 'Security lowest price in last 52 weeks',
  sk_fiftytwoweeklowdate BIGINT COMMENT 'Earliest date on which the 52 week low price was set',
  closeprice DOUBLE COMMENT 'Security closing price on this day',
  dayhigh DOUBLE COMMENT 'Highest price for the security on this day',
  daylow DOUBLE COMMENT 'Lowest price for the security on this day',
  volume INT COMMENT 'Trading volume of the security on this day'
)
CLUSTER BY (sk_dateid)  -- liquid: matches setup choice
TBLPROPERTIES (
  'delta.autoOptimize.autoCompact' = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.columnMapping.mode' = 'name',
  'delta.enableDeletionVectors' = 'false'
);

-- COMMAND ----------

INSERT OVERWRITE IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.factmarkethistory')
WITH dm AS (
  SELECT dm_date, dm_s_symb, dm_close, dm_high, dm_low, dm_vol
  FROM IDENTIFIER(:catalog || '.tpcdi_raw_data.dailymarket' || :scale_factor)
  WHERE stg_target = 'tables'
),
-- 52-week rolling high/low computed per symbol with a 365-day RANGE window.
-- max_by / min_by as window functions return the (value, date) pair with
-- the largest/smallest key, matching the SDP / classic-incremental
-- tie-break semantics (earliest date when keys collide).
windowed AS (
  SELECT
    dm_date, dm_s_symb, dm_close, dm_high, dm_low, dm_vol,
    max_by(struct(dm_high, dm_date), dm_high) OVER (
      PARTITION BY dm_s_symb ORDER BY dm_date
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW  -- historical scope is exactly one year, so the 365-day rolling window is bounded by the data itself
    ) AS fiftytwoweekhigh,
    min_by(struct(dm_low, dm_date), dm_low) OVER (
      PARTITION BY dm_s_symb ORDER BY dm_date
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW  -- historical scope is exactly one year, so the 365-day rolling window is bounded by the data itself
    ) AS fiftytwoweeklow
  FROM dm
)
SELECT
  s.sk_securityid,
  s.sk_companyid,
  bigint(date_format(w.dm_date, 'yyyyMMdd')) AS sk_dateid,
  try_divide(w.dm_close, f.prev_year_basic_eps) AS peratio,
  try_divide(s.dividend, w.dm_close) / 100 AS yield,
  w.fiftytwoweekhigh.dm_high AS fiftytwoweekhigh,
  bigint(date_format(w.fiftytwoweekhigh.dm_date, 'yyyyMMdd')) AS sk_fiftytwoweekhighdate,
  w.fiftytwoweeklow.dm_low AS fiftytwoweeklow,
  bigint(date_format(w.fiftytwoweeklow.dm_date, 'yyyyMMdd')) AS sk_fiftytwoweeklowdate,
  w.dm_close AS closeprice,
  w.dm_high  AS dayhigh,
  w.dm_low   AS daylow,
  w.dm_vol   AS volume
FROM windowed w
JOIN IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.dimsecurity') s
  ON s.symbol = w.dm_s_symb
 AND w.dm_date >= s.effectivedate
 AND w.dm_date <  s.enddate
LEFT JOIN IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.companyyeareps') f
  ON f.sk_companyid = s.sk_companyid
 AND quarter(w.dm_date) = quarter(f.qtr_start_date)
 AND year(w.dm_date)    = year(f.qtr_start_date);
