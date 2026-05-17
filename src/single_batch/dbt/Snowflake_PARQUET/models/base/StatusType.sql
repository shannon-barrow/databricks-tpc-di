{{
    config(
        materialized = 'table'
    )
}}
select
  $1:st_id::STRING   AS st_id,
  $1:st_name::STRING AS st_name
from
  @{{ var('stage') }}/Batch1
  (
    FILE_FORMAT => 'parquet_format',
    PATTERN     => '.*StatusType[.]parquet'
  )