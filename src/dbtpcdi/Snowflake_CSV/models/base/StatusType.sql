{{
    config(
        materialized = 'table'
    )
}}
select
  $1::string as st_id,
  $2::string as st_name
from
  @{{ var('stage') }}/Batch1
  (
    FILE_FORMAT => 'TXT_PIPE',
    PATTERN     => '.*StatusType[.]txt'
  )