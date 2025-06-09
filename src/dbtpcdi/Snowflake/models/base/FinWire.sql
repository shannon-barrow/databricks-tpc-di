
{{
  config(
    materialized = "table",
    partition_by = 'rectype'
  )
}}

SELECT
    IFF(
      SUBSTRING(value:c1, 16, 3) = 'FIN', 
      NVl2(
        TRY_CAST(TRIM(SUBSTRING(value:c1, 187, 60)) AS INTEGER) , 
        'FIN_COMPANYID', 
        'FIN_NAME'
      ), 
      SUBSTRING(value:c1, 16, 3)
    ) AS rectype,
    
    TRY_TO_DATE(SUBSTRING(value:c1, 1, 8), 'YYYYMMDD') AS recdate,
    
    SUBSTRING(value:c1, 19) AS value

FROM {{source('tpcdi', 'FinWireRaw') }}


