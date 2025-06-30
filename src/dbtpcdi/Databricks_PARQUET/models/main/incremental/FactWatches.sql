{{
    config(
        materialized = 'table'
    )
}}
with watchhistory AS (
    SELECT *, 1 batchid
    FROM parquet.`{{var("rawfilelocation")}}/sf={{var("scalefactor")}}/Batch1/WatchHistory*.parquet`
    UNION ALL
    SELECT
        * except(cdc_flag, cdc_dsn),
        int(substring(_metadata.file_path FROM (position('/Batch', _metadata.file_path) + 6) FOR 1)) batchid
    FROM parquet.`{{var("rawfilelocation")}}/sf={{var("scalefactor")}}/Batch{2,3}/WatchHistory.parquet`
),
watches as (
  SELECT 
    wh.w_c_id customerid,
    wh.w_s_symb symbol,        
    date(min(w_dts)) dateplaced,
    date(max(if(w_action = 'CNCL', w_dts, null))) dateremoved,
    min(batchid) batchid
  FROM watchhistory wh
  GROUP BY 
    w_c_id,
    w_s_symb
)
select
  c.sk_customerid sk_customerid,
  s.sk_securityid sk_securityid,
  bigint(date_format(dateplaced, 'yyyyMMdd')) sk_dateid_dateplaced,
  bigint(date_format(dateremoved, 'yyyyMMdd')) sk_dateid_dateremoved,
  wh.batchid 
from watches wh
JOIN  {{ref("DimSecurity")}} s 
  ON 
    s.symbol = wh.symbol
    AND wh.dateplaced >= s.effectivedate 
    AND wh.dateplaced < s.enddate
JOIN {{ref("DimCustomer")}} c 
  ON
    wh.customerid = c.customerid
    AND wh.dateplaced >= c.effectivedate 
    AND wh.dateplaced < c.enddate;