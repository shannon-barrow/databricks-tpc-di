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
  FROM {{ source('tpcdi', 'HoldingHistory') }}
  UNION ALL
  SELECT * except(cdc_flag, cdc_dsn)
  FROM {{ ref('HoldingIncremental') }}) hh
-- Converts to LEFT JOIN if this is run as DQ EDITION. It is possible, because of the issues upstream with DimSecurity/DimAccount on "some" scale factors, that DimTrade may be missing some rows.
--${dq_left_flg}
 LEFT JOIN {{ ref('DimTrade') }} dt
  ON tradeid = hh_t_id
