{{
    config(
        materialized = 'table'
    )
}}
select
  $1:tx_id::STRING   AS tx_id,
  $1:tx_name::STRING AS tx_name,
  $1:tx_rate::FLOAT  AS tx_rate
from
  @{{ var('stage') }}/Batch1
  (
    FILE_FORMAT => 'parquet_format',
    PATTERN     => '.*TaxRate[.]parquet'
  )