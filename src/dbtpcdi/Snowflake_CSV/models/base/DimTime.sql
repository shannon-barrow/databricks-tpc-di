{{
  config(
    materialized = "table"
  )
}}
select
  $1::bigint   as sk_timeid,
  $2::string   as timevalue,
  $3::int      as hourid,
  $4::string   as hourdesc,
  $5::int      as minuteid,
  $6::string   as minutedesc,
  $7::int      as secondid,
  $8::string   as seconddesc,
  $9::boolean  as markethoursflag,
  $10::boolean as officehoursflag
from
  @{{ var('stage') }}/Batch1
  (
    FILE_FORMAT => 'TXT_PIPE',
    PATTERN     => '.*Time[.]txt'
  ) t