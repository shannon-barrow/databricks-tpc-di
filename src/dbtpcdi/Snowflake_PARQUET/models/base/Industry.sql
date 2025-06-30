{{
    config(
        materialized = 'table'
    )
}}
SELECT
  $1:in_id::STRING     AS in_id,
  $1:in_name::STRING   AS in_name,
  $1:in_sc_id::STRING  AS in_sc_id
FROM
  @{{ var('stage') }}/Batch1
  (
    FILE_FORMAT => 'parquet_format',
    PATTERN     => '.*Industry[.]parquet'
  )