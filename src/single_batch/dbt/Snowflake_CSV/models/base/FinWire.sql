{{
  config(
    materialized = "table"
  )
}}
select
    IFF(
      SUBSTRING($1, 16, 3) = 'FIN', 
      NVL2(
        TRY_CAST(TRIM(SUBSTRING($1, 187, 60)) AS INTEGER), 
        'FIN_COMPANYID', 
        'FIN_NAME'
      ), 
      SUBSTRING($1, 16, 3)
    ) AS rectype,
    TRY_TO_DATE(SUBSTRING($1, 1, 8), 'YYYYMMDD') AS recdate,
    SUBSTRING($1, 19) AS value
from
    @{{ var('stage') }}/Batch1/
    (
      FILE_FORMAT => 'TXT_FIXED_WIDTH',
      PATTERN     => '.*FINWIRE[0-9][0-9][0-9][0-9]Q[1-4]'
    ) t