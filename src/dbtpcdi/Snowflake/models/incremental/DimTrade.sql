{{
    config(
        materialized = 'table'

    )
}}

-- WITH finaltrades AS (
--   SELECT
--     tradeid,
--     MIN(cdc_flag) AS cdc_flag,
--     MIN(t_dts) AS create_ts,
--     ARRAY_AGG(
--       OBJECT_CONSTRUCT(
--         't_dts', t_dts,
--         'status', status,
--         't_tt_id', t_tt_id,
--         'cashflag', cashflag,
--         't_s_symb', t_s_symb,
--         'quantity', quantity,
--         'bidprice', bidprice,
--         't_ca_id', t_ca_id,
--         'executedby', executedby,
--         'tradeprice', tradeprice,
--         'fee', fee,
--         'commission', commission,
--         'tax', tax
--       ) 
--     ) WITHIN GROUP (ORDER BY t_dts DESC)[0] AS current_record,
--     MIN(t.batchid) AS batchid
--   FROM {{ ref('TradeIncremental') }} t
--   GROUP BY tradeid
-- ),

-- TradeIncrementalHistory AS (
--   SELECT
--     tradeid,
--     current_record['t_dts']::TIMESTAMP AS ts,
--     current_record['status']::STRING AS status
--   FROM finaltrades
--   WHERE cdc_flag = 'U'
--   UNION ALL
--   SELECT th_t_id,th_dts,th_st_id FROM {{ source("tpcdi", "TradeHistoryRaw") }}
-- ),

-- Current_Trades AS (
--   SELECT
--     tradeid,
--     MIN(ts) AS create_ts,
--     ARRAY_AGG(
--       OBJECT_CONSTRUCT('ts', ts, 'status', status) 
--     ) WITHIN GROUP (ORDER BY ts DESC)[0] AS current_status
--   FROM TradeIncrementalHistory
--   GROUP BY tradeid
-- ),

-- Trades_Final AS (
--     SELECT
--       tradeid,
--       DATE(create_ts) AS create_ts,
--       CASE
--         WHEN current_status['status'] IN ('CMPT', 'CNCL') THEN current_status['ts']
--         END AS close_ts,
--       ct.current_status['status'] as status ,
--       t.t_is_cash AS cashflag,
--       t.t_st_id,
--       t.t_tt_id,
--       t.t_s_symb,
--       t.quantity,
--       t.bidprice,
--       t.t_ca_id,
--       t.executedby,
--       t.tradeprice,
--       t.fee,
--       t.commission,
--       t.tax,
--       1 AS batchid
--     FROM {{ source("tpcdi", "TradeHistory") }} t
--     JOIN Current_Trades ct
--       ON t.t_id = ct.tradeid
--     UNION ALL
--     SELECT
--       tradeid,
--       DATE(create_ts) AS create_ts,
--       CASE
--         WHEN current_record['status'] IN ('CMPT', 'CNCL') THEN current_record['t_dts'] 
--         END AS close_ts,
--       current_record:status,
--       current_record:cashflag,
--       current_record:status,
--       current_record:t_tt_id,
--       current_record:t_s_symb,
--       current_record:quantity,
--       current_record:bidprice,
--       current_record:t_ca_id,
--       current_record:executedby,
--       current_record:tradeprice,
--       current_record:fee,
--       current_record:commission,
--       current_record:tax,
--       batchid
--     FROM finaltrades
--     WHERE cdc_flag = 'I'
-- )

-- SELECT
--   trade.tradeid,
--   sk_brokerid,
--   TO_CHAR(create_ts::TIMESTAMP, 'YYYYMMDD')::INT AS sk_createdateid,
--   TO_CHAR(create_ts::TIMESTAMP, 'HH24MISS')::INT AS sk_createtimeid,
--   TO_CHAR(close_ts::TIMESTAMP, 'YYYYMMDD')::INT AS sk_closedateid,
--   TO_CHAR(close_ts::TIMESTAMP, 'HH24MISS')::INT AS sk_closetimeid,

--   CASE trade.status
--         WHEN 'ACTV' THEN 'Active'
--         WHEN 'CMPT' THEN 'Completed'
--         WHEN 'CNCL' THEN 'Canceled'
--         WHEN 'PNDG' THEN 'Pending'
--         WHEN 'SBMT' THEN 'Submitted'
--         WHEN 'INAC' THEN 'Inactive'
--     END AS status,
--     CASE t_tt_id
--         WHEN 'TMB' THEN 'Market Buy'
--         WHEN 'TMS' THEN 'Market Sell'
--         WHEN 'TSL' THEN 'Stop Loss'
--         WHEN 'TLS' THEN 'Limit Sell'
--         WHEN 'TLB' THEN 'Limit Buy'
--     END AS type,
--   cashflag = 1 AS cashflag,
--   sk_securityid,
--   sk_companyid,
--   trade.quantity,
--   trade.bidprice,
--   sk_customerid,
--   sk_accountid,
--   trade.executedby,
--   trade.tradeprice,
--   trade.fee,
--   trade.commission,
--   trade.tax,
--   trade.batchid
-- FROM Trades_Final trade
-- JOIN {{ ref('DimSecurity') }} ds
--   ON ds.symbol = trade.t_s_symb
--     AND create_ts >= ds.effectivedate 
--     AND create_ts < ds.enddate
-- JOIN {{ ref('DimAccount') }} da
--   ON trade.t_ca_id = da.accountid 
--     AND create_ts >= da.effectivedate 
--     AND create_ts < da.enddate

WITH trade_with_latest AS (
  SELECT
    tradeid,
    t_dts,
    status,
    t_tt_id,
    cashflag,
    t_s_symb,
    quantity,
    bidprice,
    t_ca_id,
    executedby,
    tradeprice,
    fee,
    commission,
    tax,
    cdc_flag,
    batchid,
    ROW_NUMBER() OVER (PARTITION BY tradeid ORDER BY t_dts DESC) AS rn,
    MIN(t_dts) OVER (PARTITION BY tradeid) AS create_ts_raw,
    MIN(cdc_flag) OVER (PARTITION BY tradeid) AS min_cdc_flag,
    MIN(batchid) OVER (PARTITION BY tradeid) AS min_batchid
  FROM {{ref("TradeIncremental")}} t
  QUALIFY rn=1
),

latest_trades AS (
  SELECT
    tradeid,
    t_dts              AS latest_t_dts,
    status             AS latest_status,
    t_tt_id,
    cashflag,
    t_s_symb,
    quantity,
    bidprice,
    t_ca_id,
    executedby,
    tradeprice,
    fee,
    commission,
    tax,
    create_ts_raw,
    min_cdc_flag       AS cdc_flag,
    min_batchid        AS batchid
  FROM trade_with_latest
),

trade_status_history AS (
  SELECT
    tradeid,
    latest_t_dts    AS ts,
    latest_status   AS status
  FROM latest_trades
  WHERE cdc_flag = 'U'

  UNION ALL

  SELECT 
    th_t_id         AS tradeid,
    th_dts          AS ts,
    th_st_id        AS status

  FROM {{source("tpcdi","TradeHistoryRaw")}} 
),

current_trade_status AS (
  SELECT
    tradeid,
    MIN(ts) OVER (PARTITION BY tradeid)  AS create_ts,
    status as current_status,
    ts               AS last_status_ts,
    ROW_NUMBER() OVER (PARTITION BY tradeid ORDER BY ts DESC)                           AS status_rn
  FROM trade_status_history
  QUALIFY status_rn = 1
),

trades_final AS (
  SELECT
    t.t_id                          AS tradeid,
    DATE(cts.create_ts)             AS create_ts,
    YEAR(cts.create_ts) * 10000
      + MONTH(cts.create_ts) * 100
      + DAY(cts.create_ts)          AS sk_createdateid,
    HOUR(cts.create_ts) * 10000
      + MINUTE(cts.create_ts) * 100
      + SECOND(cts.create_ts)       AS sk_createtimeid,
    CASE 
      WHEN cts.current_status IN ('CMPT','CNCL')
      THEN DATE(cts.last_status_ts)
    END                             AS close_ts,
    CASE 
      WHEN cts.current_status IN ('CMPT','CNCL')
      THEN YEAR(cts.last_status_ts) * 10000
           + MONTH(cts.last_status_ts) * 100
           + DAY(cts.last_status_ts)
    END                             AS sk_closedateid,
    CASE 
      WHEN cts.current_status IN ('CMPT','CNCL')
      THEN HOUR(cts.last_status_ts) * 10000
           + MINUTE(cts.last_status_ts) * 100
           + SECOND(cts.last_status_ts)
    END                             AS sk_closetimeid,
    cts.current_status               AS status,
    t.t_is_cash                      AS cashflag,
    t.t_st_id,
    t.t_tt_id,
    t.t_s_symb,
    t.quantity,
    t.bidprice,
    t.t_ca_id,
    t.executedby,
    t.tradeprice,
    t.fee,
    t.commission,
    t.tax,
    1  AS batchid
  FROM {{source("tpcdi","TradeHistory")}}   t
  JOIN current_trade_status cts
    ON t.t_id = cts.tradeid

  UNION ALL

  SELECT
    tradeid,
    DATE(create_ts_raw)             AS create_ts,
    YEAR(create_ts_raw) * 10000
      + MONTH(create_ts_raw) * 100
      + DAY(create_ts_raw)          AS sk_createdateid,
    HOUR(create_ts_raw) * 10000
      + MINUTE(create_ts_raw) * 100
      + SECOND(create_ts_raw)       AS sk_createtimeid,
    CASE 
      WHEN latest_status IN ('CMPT','CNCL')
      THEN DATE(latest_t_dts)
    END                             AS close_ts,
    CASE 
      WHEN latest_status IN ('CMPT','CNCL')
      THEN YEAR(latest_t_dts) * 10000
           + MONTH(latest_t_dts) * 100
           + DAY(latest_t_dts)
    END                             AS sk_closedateid,
    CASE 
      WHEN latest_status IN ('CMPT','CNCL')
      THEN HOUR(latest_t_dts) * 10000
           + MINUTE(latest_t_dts) * 100
           + SECOND(latest_t_dts)
    END                             AS sk_closetimeid,
    latest_status                    AS status,
    cashflag,
    latest_status                    AS t_st_id,
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
  FROM latest_trades
  WHERE cdc_flag = 'I'
)

SELECT
  trade.tradeid,
  da.sk_brokerid,
  trade.sk_createdateid,
  trade.sk_createtimeid,
  trade.sk_closedateid,
  trade.sk_closetimeid,
  CASE trade.status
    WHEN 'ACTV' THEN 'Active'
    WHEN 'CMPT' THEN 'Completed'
    WHEN 'CNCL' THEN 'Canceled'
    WHEN 'PNDG' THEN 'Pending'
    WHEN 'SBMT' THEN 'Submitted'
    WHEN 'INAC' THEN 'Inactive'
    ELSE trade.status
  END                                AS status,
  CASE trade.t_tt_id
    WHEN 'TMB' THEN 'Market Buy'
    WHEN 'TMS' THEN 'Market Sell'
    WHEN 'TSL' THEN 'Stop Loss'
    WHEN 'TLS' THEN 'Limit Sell'
    WHEN 'TLB' THEN 'Limit Buy'
    ELSE trade.t_tt_id
  END                                AS type,
  (trade.cashflag = 1)               AS cashflag,
  ds.sk_securityid,
  ds.sk_companyid,
  trade.quantity,
  trade.bidprice,
  da.sk_customerid,
  da.sk_accountid,
  trade.executedby,
  trade.tradeprice,
  trade.fee,
  trade.commission,
  trade.tax,
  trade.batchid
FROM trades_final AS trade
JOIN {{ref("DimAccount")}}  AS da
  ON trade.t_ca_id   = da.accountid 
  AND trade.create_ts >= da.effectivedate 
  AND trade.create_ts <  da.enddate
JOIN {{ref("DimSecurity")}}   AS ds
  ON ds.symbol       = trade.t_s_symb
  AND trade.create_ts >= ds.effectivedate 
  AND trade.create_ts <  ds.enddate
