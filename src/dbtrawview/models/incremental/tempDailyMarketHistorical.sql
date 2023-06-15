{{
    config(
        materialized = 'table'
    )
}}
SELECT
  dmh.*,
  sk_dateid,
  min(dm_low) OVER (
    PARTITION BY dm_s_symb
    ORDER BY dm_date ASC ROWS BETWEEN 364 PRECEDING AND CURRENT ROW
  ) fiftytwoweeklow,
  max(dm_high) OVER (
    PARTITION by dm_s_symb
    ORDER BY dm_date ASC ROWS BETWEEN 364 PRECEDING AND CURRENT ROW
  ) fiftytwoweekhigh
FROM (
  SELECT * FROM {{ ref('DailyMarketHistorical') }}
  UNION ALL
  SELECT * except(cdc_flag, cdc_dsn) FROM {{ ref('DailyMarketIncremental') }}) dmh
JOIN {{ source('tpcdi', 'DimDate') }} d 
  ON d.datevalue = dm_date;
