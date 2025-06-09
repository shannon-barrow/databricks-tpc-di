{{
    config(
        materialized = 'table'

    )
}}


WITH WatchHistory AS (
  SELECT
    w_c_id AS customerid,
    w_s_symb AS symbol,
    w_dts,
    w_action,
    1 AS batchid
  FROM {{ source("tpcdi", 'WatchHistory') }}
  UNION ALL
  SELECT
    w_c_id AS customerid,
    w_s_symb AS symbol,
    w_dts,
    w_action,
    batchid
  FROM {{ ref('WatchIncremental') }}
),

Watches AS (
  SELECT 
    wh.customerid,
    wh.symbol,        
    DATE(MIN(w_dts)) AS dateplaced,
    DATE(MAX(IFF(w_action = 'CNCL', w_dts, NULL))) AS dateremoved,
    MIN(batchid) AS batchid
  FROM WatchHistory wh
  GROUP BY 1,2
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

