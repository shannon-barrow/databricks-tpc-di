{{
  config(
    materialized = "table"
  )
}}
select
  $1:sk_dateid::BIGINT           AS sk_dateid,
  $1:datevalue::DATE             AS datevalue,
  $1:datedesc::STRING            AS datedesc,
  $1:calendaryearid::INT         AS calendaryearid,
  $1:calendaryeardesc::STRING    AS calendaryeardesc,
  $1:calendarqtrid::INT          AS calendarqtrid,
  $1:calendarqtrdesc::STRING     AS calendarqtrdesc,
  $1:calendarmonthid::INT        AS calendarmonthid,
  $1:calendarmonthdesc::STRING   AS calendarmonthdesc,
  $1:calendarweekid::INT         AS calendarweekid,
  $1:calendarweekdesc::STRING    AS calendarweekdesc,
  $1:dayofweeknum::INT           AS dayofweeknum,
  $1:dayofweekdesc::STRING       AS dayofweekdesc,
  $1:fiscalyearid::INT           AS fiscalyearid,
  $1:fiscalyeardesc::STRING      AS fiscalyeardesc,
  $1:fiscalqtrid::INT            AS fiscalqtrid,
  $1:fiscalqtrdesc::STRING       AS fiscalqtrdesc,
  $1:holidayflag::BOOLEAN        AS holidayflag
from
    @EXTERNAL_STAGE/tpcdi/250mb_splitparquet/sf=10000/Batch1
  (
    FILE_FORMAT => 'parquet_format',
    PATTERN     => '.*[/]Date[.]parquet'
  )