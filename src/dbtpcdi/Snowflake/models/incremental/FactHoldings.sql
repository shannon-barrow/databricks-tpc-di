{{
    config(
        materialized = 'table'

    )
}}


WITH Holdings as (
  SELECT 
    * exclude (value), 
    1 batchid
  FROM  {{source("tpcdi", 'HoldingHistory') }}
  UNION ALL
  SELECT
    * EXCLUDE(cdc_flag, cdc_dsn)
    
  FROM {{ ref('HoldingIncremental') }}
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
