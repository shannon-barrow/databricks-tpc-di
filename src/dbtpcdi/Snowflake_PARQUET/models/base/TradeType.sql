{{
    config(
        materialized = 'table'
    )
}}
SELECT
  $1:tt_id::STRING      AS tt_id,
  $1:tt_name::STRING    AS tt_name,
  $1:tt_is_sell::INT    AS tt_is_sell,
  $1:tt_is_mrkt::INT    AS tt_is_mrkt
FROM
  @{{ var('stage') }}/Batch1
  (
    FILE_FORMAT => 'parquet_format',
    PATTERN     => '.*TradeType[.]parquet'
  )