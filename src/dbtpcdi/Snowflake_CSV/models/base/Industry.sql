{{
    config(
        materialized = 'table'
    )
}}
select
  $1::string as in_id,
  $2::string as in_name,
  $3::string as in_sc_id
from
  @{{ var('stage') }}/Batch1
  (
    FILE_FORMAT => 'TXT_PIPE',
    PATTERN     => '.*Industry[.]txt'
  )