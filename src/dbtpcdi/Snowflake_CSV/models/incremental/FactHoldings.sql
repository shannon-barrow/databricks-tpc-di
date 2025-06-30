{{
    config(
        materialized = 'table'
    )
}}
WITH Holdings as (
    SELECT
        $1::BIGINT as hh_h_t_id,
        $2::BIGINT as hh_t_id,
        $3::INT as hh_before_qty,
        $4::INT as hh_after_qty,
        1 batchid
    FROM
        @{{ var('stage') }}/Batch1
        (
            FILE_FORMAT => 'TXT_PIPE',
            PATTERN     => '.*HoldingHistory[.]txt'
        ) t
    UNION ALL
    SELECT
        $3::BIGINT    as hh_h_t_id,
        $4::BIGINT    as hh_t_id,
        $5::INT    as hh_before_qty,
        $6::INT    as hh_after_qty,
        try_cast(SUBSTR(METADATA$FILENAME, position('/Batch' in METADATA$FILENAME) + 6, 1) as int) batchid
    FROM
        @{{ var('stage') }}
        (
            FILE_FORMAT => 'TXT_PIPE',
            PATTERN     => '.*Batch[23]/HoldingHistory[.]txt'
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
