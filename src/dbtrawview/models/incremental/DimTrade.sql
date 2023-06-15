{{
    config(
        materialized = 'table'
    )
}}
SELECT
  trade.tradeid,
  sk_brokerid,
  trade.sk_createdateid,
  trade.sk_createtimeid,
  trade.sk_closedateid,
  trade.sk_closetimeid,
  st_name status,
  tt_name type,
  trade.cashflag,
  sk_securityid,
  sk_companyid,
  trade.quantity,
  trade.bidprice,
  sk_customerid,
  sk_accountid,
  trade.executedby,
  trade.tradeprice,
  trade.fee,
  trade.commission,
  trade.tax,
  trade.batchid
FROM (
  SELECT * EXCEPT(t_dts)
  FROM (
    SELECT
      tradeid,
      min(date(t_dts)) OVER (PARTITION BY tradeid) createdate,
      t_dts,
      coalesce(sk_createdateid, last_value(sk_createdateid) IGNORE NULLS OVER (
        PARTITION BY tradeid ORDER BY t_dts)) sk_createdateid,
      coalesce(sk_createtimeid, last_value(sk_createtimeid) IGNORE NULLS OVER (
        PARTITION BY tradeid ORDER BY t_dts)) sk_createtimeid,
      coalesce(sk_closedateid, last_value(sk_closedateid) IGNORE NULLS OVER (
        PARTITION BY tradeid ORDER BY t_dts)) sk_closedateid,
      coalesce(sk_closetimeid, last_value(sk_closetimeid) IGNORE NULLS OVER (
        PARTITION BY tradeid ORDER BY t_dts)) sk_closetimeid,
      cashflag,
      t_st_id,
      t_tt_id,
      t_s_symb,
      quantity,
      bidprice,
      t_ca_id,
      executedby,
      tradeprice,
      fee,
      commission,
      tax,
      batchid
    FROM (
      SELECT
        tradeid,
        t_dts,
        if(create_flg, sk_dateid, cast(NULL AS BIGINT)) sk_createdateid,
        if(create_flg, sk_timeid, cast(NULL AS BIGINT)) sk_createtimeid,
        if(!create_flg, sk_dateid, cast(NULL AS BIGINT)) sk_closedateid,
        if(!create_flg, sk_timeid, cast(NULL AS BIGINT)) sk_closetimeid,
        CASE 
          WHEN t_is_cash = 1 then TRUE
          WHEN t_is_cash = 0 then FALSE
          ELSE cast(null as BOOLEAN) END AS cashflag,
        t_st_id,
        t_tt_id,
        t_s_symb,
        quantity,
        bidprice,
        t_ca_id,
        executedby,
        tradeprice,
        fee,
        commission,
        tax,
        t.batchid
      FROM (
        SELECT
          t_id tradeid,
          th_dts t_dts,
          t_st_id,
          t_tt_id,
          t_is_cash,
          t_s_symb,
          t_qty AS quantity,
          t_bid_price AS bidprice,
          t_ca_id,
          t_exec_name AS executedby,
          t_trade_price AS tradeprice,
          t_chrg AS fee,
          t_comm AS commission,
          t_tax AS tax,
          1 batchid,
          CASE 
            WHEN (th_st_id == "SBMT" AND t_tt_id IN ("TMB", "TMS")) OR th_st_id = "PNDG" THEN TRUE 
            WHEN th_st_id IN ("CMPT", "CNCL") THEN FALSE 
            ELSE cast(null as boolean) END AS create_flg
        FROM {{ source('tpcdi', 'TradeHistory') }} t
        JOIN {{ source('tpcdi', 'TradeHistoryRaw') }} th
          ON th_t_id = t_id
        UNION ALL
        SELECT
          t_id tradeid,
          t_dts,
          t_st_id,
          t_tt_id,
          t_is_cash,
          t_s_symb,
          t_qty AS quantity,
          t_bid_price AS bidprice,
          t_ca_id,
          t_exec_name AS executedby,
          t_trade_price AS tradeprice,
          t_chrg AS fee,
          t_comm AS commission,
          t_tax AS tax,
          t.batchid,
          CASE 
            WHEN cdc_flag = 'I' THEN TRUE 
            WHEN t_st_id IN ("CMPT", "CNCL") THEN FALSE 
            ELSE cast(null as boolean) END AS create_flg
        FROM {{ ref('TradeIncremental') }} t
      ) t
      JOIN {{ source('tpcdi', 'DimDate') }} dd
        ON date(t.t_dts) = dd.datevalue
      JOIN {{ source('tpcdi', 'DimTime') }} dt
        ON date_format(t.t_dts, 'HH:mm:ss') = dt.timevalue
    )
  )
  QUALIFY ROW_NUMBER() OVER (PARTITION BY tradeid ORDER BY t_dts desc) = 1
) trade
JOIN {{ source('tpcdi', 'StatusType') }} status
  ON status.st_id = trade.t_st_id
JOIN {{ source('tpcdi', 'TradeType') }} tt
  ON tt.tt_id == trade.t_tt_id
-- Converts to LEFT JOIN if this is run as DQ EDITION. On some higher Scale Factors, a small number of Security symbols or Account IDs are missing from DimSecurity/DimAccount, causing audit check failures. 
--${dq_left_flg} 
LEFT JOIN {{ ref('DimSecurity') }} ds
  ON 
    ds.symbol = trade.t_s_symb
    AND createdate >= ds.effectivedate 
    AND createdate < ds.enddate
--${dq_left_flg} 
LEFT JOIN {{ ref('DimAccount') }} da
  ON 
    trade.t_ca_id = da.accountid 
    AND createdate >= da.effectivedate 
    AND createdate < da.enddate
