{{
    config(
        materialized = 'table'
    )
}}
WITH companyfinancials AS (
  SELECT
    f.sk_companyid,
    fi_qtr_start_date,
    SUM(fi_basic_eps) OVER (PARTITION BY companyid ORDER BY fi_qtr_start_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) - fi_basic_eps AS sum_fi_basic_eps
  FROM {{ ref('Financial') }} f
  JOIN {{ ref('DimCompany') }} d
    ON f.sk_companyid = d.sk_companyid
),
dailymarket AS (
    SELECT
        $1:dm_date::DATE       AS dm_date,
        $1:dm_s_symb::STRING   AS dm_s_symb,
        $1:dm_close::DOUBLE    AS dm_close,
        $1:dm_high::DOUBLE     AS dm_high,
        $1:dm_low::DOUBLE      AS dm_low,
        $1:dm_vol::INT         AS dm_vol,
        1          as batchid
    FROM
        @{{ var('stage') }}/Batch1
        (
            FILE_FORMAT => 'parquet_format',
            PATTERN     => '.*DailyMarket_.*[.]parquet'
        )
    UNION ALL
    SELECT
        $1:dm_date::DATE        AS dm_date,
        $1:dm_s_symb::STRING    AS dm_s_symb,
        $1:dm_close::DOUBLE     AS dm_close,
        $1:dm_high::DOUBLE      AS dm_high,
        $1:dm_low::DOUBLE       AS dm_low,
        $1:dm_vol::INT          AS dm_vol,
        try_cast(SUBSTR(METADATA$FILENAME, position('/Batch' in METADATA$FILENAME) + 6, 1) as int) batchid
    FROM
        @{{ var('stage') }}
        (
            FILE_FORMAT => 'parquet_format',
            PATTERN     => '.*Batch[23]/DailyMarket[.]parquet'
        )
),
-- Snowflake has no MIN_BY/MAX_BY over a sliding window frame, so the 52-week
-- low/high values come from MIN/MAX over the window, and the date each was set
-- (earliest on ties) from a self-join back to the same 365-row window.
dailymarket_rn AS (
  SELECT
    dm.*,
    ROW_NUMBER() OVER (PARTITION BY dm_s_symb ORDER BY dm_date) AS rn
  FROM dailymarket dm
),
windowed AS (
  SELECT
    dm.*,
    MIN(dm_low) OVER (
      PARTITION BY dm_s_symb
      ORDER BY dm_date ASC
      ROWS BETWEEN 364 PRECEDING AND CURRENT ROW
    ) AS fiftytwoweeklow,
    MAX(dm_high) OVER (
      PARTITION BY dm_s_symb
      ORDER BY dm_date ASC
      ROWS BETWEEN 364 PRECEDING AND CURRENT ROW
    ) AS fiftytwoweekhigh
  FROM dailymarket_rn dm
),
lowdates AS (
  SELECT w.dm_s_symb, w.rn, MIN(p.dm_date) AS fiftytwoweeklowdate
  FROM windowed w
  JOIN dailymarket_rn p
    ON p.dm_s_symb = w.dm_s_symb
    AND p.dm_low = w.fiftytwoweeklow
    AND p.rn BETWEEN w.rn - 364 AND w.rn
  GROUP BY ALL
),
highdates AS (
  SELECT w.dm_s_symb, w.rn, MIN(p.dm_date) AS fiftytwoweekhighdate
  FROM windowed w
  JOIN dailymarket_rn p
    ON p.dm_s_symb = w.dm_s_symb
    AND p.dm_high = w.fiftytwoweekhigh
    AND p.rn BETWEEN w.rn - 364 AND w.rn
  GROUP BY ALL
),
markethistory AS (
  SELECT w.*, l.fiftytwoweeklowdate, h.fiftytwoweekhighdate
  FROM windowed w
  JOIN lowdates l USING (dm_s_symb, rn)
  JOIN highdates h USING (dm_s_symb, rn)
)
SELECT
  s.sk_securityid,
  s.sk_companyid,
  TO_NUMBER(TO_CHAR(dm_date, 'YYYYMMDD')) AS sk_dateid,
  DIV0(mh.dm_close, sum_fi_basic_eps) AS peratio,
  DIV0(DIV0(s.dividend, mh.dm_close), 100) AS yield,
  fiftytwoweekhigh,
  TO_NUMBER(TO_CHAR(fiftytwoweekhighdate, 'YYYYMMDD')) AS sk_fiftytwoweekhighdate,
  fiftytwoweeklow,
  TO_NUMBER(TO_CHAR(fiftytwoweeklowdate, 'YYYYMMDD')) AS sk_fiftytwoweeklowdate,
  dm_close AS closeprice,
  dm_high AS dayhigh,
  dm_low AS daylow,
  dm_vol AS volume,
  mh.batchid
FROM markethistory mh
JOIN {{ ref('DimSecurity') }} s 
  ON s.symbol = mh.dm_s_symb
  AND mh.dm_date >= s.effectivedate 
  AND mh.dm_date < s.enddate
LEFT JOIN companyfinancials f 
  ON f.sk_companyid = s.sk_companyid
  AND EXTRACT(QUARTER FROM mh.dm_date) = EXTRACT(QUARTER FROM fi_qtr_start_date)
  AND EXTRACT(YEAR FROM mh.dm_date) = EXTRACT(YEAR FROM fi_qtr_start_date)