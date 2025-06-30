{{
    config(
        materialized = 'table'
    )
}}
select
  $1::string as tt_id,
  $2::string as tt_name,
  $3::int    as tt_is_sell,
  $4::int    as tt_is_mrkt
from
  @{{ var('stage') }}/Batch1
  (
    FILE_FORMAT => 'TXT_PIPE',
    PATTERN     => '.*TradeType[.]txt'
  )