{{
  config(
    materialized = "table"
  )
}}
SELECT
  $1:sk_timeid::BIGINT           AS sk_timeid,
  $1:timevalue::STRING           AS timevalue,
  $1:hourid::INT                 AS hourid,
  $1:hourdesc::STRING            AS hourdesc,
  $1:minuteid::INT               AS minuteid,
  $1:minutedesc::STRING          AS minutedesc,
  $1:secondid::INT               AS secondid,
  $1:seconddesc::STRING          AS seconddesc,
  $1:markethoursflag::BOOLEAN    AS markethoursflag,
  $1:officehoursflag::BOOLEAN    AS officehoursflag
FROM
  @{{ var('stage') }}/Batch1
  (
    FILE_FORMAT => 'parquet_format',
    PATTERN     => '.*Time[.]parquet'
  )