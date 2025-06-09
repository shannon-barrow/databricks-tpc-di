{{
    config(
        materialized = 'table'
    )
}}

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
FROM (
  SELECT 
    * ,
    1 batchid
  FROM {{source('tpcdi', 'HoldingHistory') }}
  UNION ALL
  SELECT * except(cdc_flag, cdc_dsn)
  FROM {{ ref('HoldingIncremental') }}) hh
 LEFT JOIN {{ ref('DimTrade') }} dt
  ON tradeid = hh_t_id
