{{
    config(
        materialized = 'table'
    )
}}
WITH Holdings as (
    SELECT
        *,
        1 batchid
    FROM parquet.`{{var("rawfilelocation")}}/sf={{var("scalefactor")}}/Batch1/HoldingHistory*.parquet`
    UNION ALL
    SELECT
        * except(cdc_flag, cdc_dsn),
        int(substring(_metadata.file_path FROM (position('/Batch', _metadata.file_path) + 6) FOR 1)) batchid
    FROM parquet.`{{var("rawfilelocation")}}/sf={{var("scalefactor")}}/Batch{2,3}/HoldingHistory.parquet`
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
  h.batchid
FROM Holdings h
  JOIN {{ ref('DimTrade') }} dt
    ON tradeid = hh_t_id;