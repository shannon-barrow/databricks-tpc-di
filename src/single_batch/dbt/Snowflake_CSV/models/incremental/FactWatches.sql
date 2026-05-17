{{
    config(
        materialized = 'table'
    )
}}
WITH WatchHistory AS (
    SELECT
        $1::bigint    AS customerid,
        $2::string    AS symbol,
        $3::timestamp AS w_dts,
        $4::string    AS w_action,
        1             AS batchid
    FROM
        @{{ var('stage') }}/Batch1
        (
            FILE_FORMAT => 'TXT_PIPE',
            PATTERN     => '.*WatchHistory[.]txt'
        )
    UNION ALL
    SELECT
        $3::bigint    AS customerid,
        $4::string    AS symbol,
        $5::timestamp AS w_dts,
        $6::string    AS w_action,
        try_cast(SUBSTR(METADATA$FILENAME, position('/Batch' in METADATA$FILENAME) + 6, 1) as int) batchid
    FROM
        @{{ var('stage') }}
        (
            FILE_FORMAT => 'TXT_PIPE',
            PATTERN     => '.*Batch[23]/WatchHistory[.]txt'
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

