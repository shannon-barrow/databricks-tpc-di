{{
  config(
    materialized = "table"
  )
}}
select
 IFF(
    SUBSTRING($1:value::STRING, 16, 3) = 'FIN',
    NVL2(
      TRY_CAST(TRIM(SUBSTRING($1:value::STRING, 187, 60)) AS INTEGER),
      'FIN_COMPANYID',
      'FIN_NAME'
    ),
    SUBSTRING($1:value::STRING, 16, 3)
  ) AS rectype,
  TRY_TO_DATE(SUBSTRING($1:value::STRING, 1, 8), 'YYYYMMDD') AS recdate,
  SUBSTRING($1:value::STRING, 19) AS value
  from
    @{{ var('stage') }}/Batch1/
    (
      FILE_FORMAT => 'parquet_format',
      PATTERN     => '.*FINWIRE_.*[.]parquet'
    )