{{
  config(
    materialized = "table"
  )
}}
select
    $1::date   as batchdate,
    try_cast(SUBSTR(METADATA$FILENAME, position('/Batch' in METADATA$FILENAME) + 6, 1) as int) batchid
from
    @{{ var('stage') }}
    (
      FILE_FORMAT => 'TXT_PIPE',
      PATTERN     => '.*BatchDate[.]txt'
    ) t