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
        $1::date   as dm_date,
        $2::string as dm_s_symb,
        $3::double as dm_close,
        $4::double as dm_high,
        $5::double as dm_low,
        $6::int    as dm_vol,
        1          as batchid
    FROM
        @{{ var('stage') }}/Batch1
        (
            FILE_FORMAT => 'TXT_PIPE',
            PATTERN     => '.*DailyMarket[.]txt'
        )
    UNION ALL
    SELECT
        $3::date   as dm_date,
        $4::string as dm_s_symb,
        $5::double as dm_close,
        $6::double as dm_high,
        $7::double as dm_low,
        $8::int    as dm_vol,
        try_cast(SUBSTR(METADATA$FILENAME, position('/Batch' in METADATA$FILENAME) + 6, 1) as int) batchid
    FROM
        @{{ var('stage') }}
        (
            FILE_FORMAT => 'TXT_PIPE',
            PATTERN     => '.*Batch[23]/DailyMarket[.]txt'
        )
),
markethistory AS (
  SELECT
    dm.*,
    FIRST_VALUE(OBJECT_CONSTRUCT('dm_low', dm_low, 'dm_date', dm_date)) OVER (
      PARTITION BY dm_s_symb
      ORDER BY dm_low ASC, dm_date ASC
      ROWS BETWEEN 364 PRECEDING AND CURRENT ROW
    ) AS fiftytwoweeklow,
    FIRST_VALUE(OBJECT_CONSTRUCT('dm_high', dm_high, 'dm_date', dm_date)) OVER (
      PARTITION BY dm_s_symb
      ORDER BY dm_high DESC, dm_date ASC
      ROWS BETWEEN 364 PRECEDING AND CURRENT ROW
    ) AS fiftytwoweekhigh
  FROM dailymarket dm
)
SELECT
  s.sk_securityid,
  s.sk_companyid,
  TO_NUMBER(TO_CHAR(dm_date, 'YYYYMMDD')) AS sk_dateid,
  DIV0(mh.dm_close, sum_fi_basic_eps) AS peratio,
  DIV0(DIV0(s.dividend, mh.dm_close), 100) AS yield,
  fiftytwoweekhigh:dm_high::NUMBER AS fiftytwoweekhigh,
  TO_NUMBER(TO_CHAR(fiftytwoweekhigh:dm_date::DATE, 'YYYYMMDD')) AS sk_fiftytwoweekhighdate,
  fiftytwoweeklow:dm_low::NUMBER AS fiftytwoweeklow,
  TO_NUMBER(TO_CHAR(fiftytwoweeklow:dm_date::DATE, 'YYYYMMDD')) AS sk_fiftytwoweeklowdate,
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
