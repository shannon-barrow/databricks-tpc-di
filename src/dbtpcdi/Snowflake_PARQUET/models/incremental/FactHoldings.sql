{{
    config(
        materialized = 'table'
    )
}}
WITH Holdings as (
    SELECT
        $1:hh_h_t_id::BIGINT  AS hh_h_t_id,
        $1:hh_t_id::BIGINT    AS hh_t_id,
        $1:hh_before_qty::INT AS hh_before_qty,
        $1:hh_after_qty::INT  AS hh_after_qty,
        1 batchid
    FROM
        @{{ var('stage') }}/Batch1
        (
            FILE_FORMAT => 'parquet_format',
            PATTERN     => '.*HoldingHistory_.*[.]parquet'
        )
    UNION ALL
    SELECT
        $1:hh_h_t_id::BIGINT      AS hh_h_t_id,
        $1:hh_t_id::BIGINT        AS hh_t_id,
        $1:hh_before_qty::INT     AS hh_before_qty,
        $1:hh_after_qty::INT      AS hh_after_qty,
        try_cast(SUBSTR(METADATA$FILENAME, position('/Batch' in METADATA$FILENAME) + 6, 1) as int) batchid
    FROM
        @{{ var('stage') }}
        (
            FILE_FORMAT => 'parquet_format',
            PATTERN     => '.*Batch[23]/HoldingHistory[.]parquet'
        )
)
SELECT 
  hh_h_t_id tradeid,
  hh_t_id currenttradeid,
  sk_customerid,
  sk_accountid,
  sk_securityid,
  sk_companyid,
  sk_closedateid sk_dateid,
  sk_closetimeid sk_timeid,
  tradeprice currentprice,
  hh_after_qty currentholding,
  hh.batchid
FROM  Holdings hh
JOIN {{ ref('DimTrade') }} dt
  ON tradeid = hh_t_id
