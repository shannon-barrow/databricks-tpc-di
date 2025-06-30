{{
    config(
        materialized = 'table'
    )
}}
select
  $1::string as tx_id,
  $2::string as tx_name,
  $3::float  as tx_rate
from
  @{{ var('stage') }}/Batch1
  (
    FILE_FORMAT => 'TXT_PIPE',
    PATTERN     => '.*TaxRate[.]txt'
  )