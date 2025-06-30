{{
  config(
    materialized = "table"
  )
}}
select
  $1::bigint   as sk_dateid,
  $2::date     as datevalue,
  $3::string   as datedesc,
  $4::int      as calendaryearid,
  $5::string   as calendaryeardesc,
  $6::int      as calendarqtrid,
  $7::string   as calendarqtrdesc,
  $8::int      as calendarmonthid,
  $9::string   as calendarmonthdesc,
  $10::int     as calendarweekid,
  $11::string  as calendarweekdesc,
  $12::int     as dayofweeknum,
  $13::string  as dayofweekdesc,
  $14::int     as fiscalyearid,
  $15::string  as fiscalyeardesc,
  $16::int     as fiscalqtrid,
  $17::string  as fiscalqtrdesc,
  $18::boolean as holidayflag
from
  @{{ var('stage') }}/Batch1
  (
    FILE_FORMAT => 'TXT_PIPE',
    PATTERN     => '.*[/]Date[.]txt'
  ) t