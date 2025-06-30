{{
    config(
        materialized = 'table'
    )
}}
WITH WatchHistory AS (
    SELECT
        $1:w_c_id::BIGINT       AS customerid,
        $1:w_s_symb::STRING     AS symbol,
        CONVERT_TIMEZONE('UTC', $1:w_dts::TIMESTAMP)     AS w_dts,
        $1:w_action::STRING     AS w_action,
        1             AS batchid
    FROM
        @{{ var('stage') }}/Batch1
        (
            FILE_FORMAT => 'parquet_format',
            PATTERN     => '.*WatchHistory_.*[.]parquet'
        )
    UNION ALL
    SELECT
        $1:w_c_id::BIGINT        AS customerid,
        $1:w_s_symb::STRING      AS symbol,
        CONVERT_TIMEZONE('UTC', $1:w_dts::TIMESTAMP)      AS w_dts,
        $1:w_action::STRING      AS w_action,
        try_cast(SUBSTR(METADATA$FILENAME, position('/Batch' in METADATA$FILENAME) + 6, 1) as int) batchid
    FROM
        @{{ var('stage') }}
        (
            FILE_FORMAT => 'parquet_format',
            PATTERN     => '.*Batch[23]/WatchHistory[.]parquet'
        )
),
Watches AS (
  SELECT 
    wh.customerid,
    wh.symbol,        
    DATE(MIN(w_dts)) AS dateplaced,
    DATE(MAX(IFF(w_action = 'CNCL', w_dts, NULL))) AS dateremoved,
    MIN(batchid) AS batchid
  FROM WatchHistory wh
  GROUP BY wh.customerid, wh.symbol
)

SELECT
  c.sk_customerid,
  s.sk_securityid,
  CAST(TO_CHAR(wh.dateplaced, 'YYYYMMDD') AS NUMBER(38,0)) AS sk_dateid_dateplaced,
  CAST(TO_CHAR(wh.dateremoved, 'YYYYMMDD') AS NUMBER(38,0)) AS sk_dateid_dateremoved,
  wh.batchid
FROM Watches wh
JOIN {{ ref('DimSecurity') }} s
  ON s.symbol = wh.symbol
  AND wh.dateplaced >= s.effectivedate 
  AND wh.dateplaced < s.enddate
JOIN {{ ref('DimCustomer') }} c
  ON wh.customerid = c.customerid
  AND wh.dateplaced >= c.effectivedate 
  AND wh.dateplaced < c.enddate

