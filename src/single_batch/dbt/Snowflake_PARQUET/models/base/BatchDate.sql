{{
  config(
    materialized = "table"
  )
}}
select
    $1:batchdate::DATE  AS batchdate,
    try_cast(SUBSTR(METADATA$FILENAME, position('/Batch' in METADATA$FILENAME) + 6, 1) as int) batchid
from
    @{{ var('stage') }}
    (
      FILE_FORMAT => 'parquet_format',
      PATTERN     => '.*BatchDate[.]parquet'
    ) t