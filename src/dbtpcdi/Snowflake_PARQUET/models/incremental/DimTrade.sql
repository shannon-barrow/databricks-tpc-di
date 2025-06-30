{{
    config(
        materialized = 'table'
    )
}}
WITH tradeincremental as (
    select
        $1:cdc_flag::STRING       AS cdc_flag,
        $1:cdc_dsn::BIGINT        AS cdc_dsn,
        $1:tradeid::BIGINT        AS tradeid,
        CONVERT_TIMEZONE('UTC', $1:t_dts::TIMESTAMP)       AS t_dts,
        $1:status::STRING         AS status,
        $1:t_tt_id::STRING        AS t_tt_id,
        $1:cashflag::TINYINT      AS cashflag,
        $1:t_s_symb::STRING       AS t_s_symb,
        $1:quantity::INT          AS quantity,
        $1:bidprice::DOUBLE       AS bidprice,
        $1:t_ca_id::BIGINT        AS t_ca_id,
        $1:executedby::STRING     AS executedby,
        $1:tradeprice::DOUBLE     AS tradeprice,
        $1:fee::DOUBLE            AS fee,
        $1:commission::DOUBLE     AS commission,
        $1:tax::DOUBLE            AS tax,
        try_cast(SUBSTR(METADATA$FILENAME, position('/Batch' in METADATA$FILENAME) + 6, 1) as int) batchid
    from
        @{{ var('stage') }}
        (
            FILE_FORMAT => 'parquet_format',
            PATTERN     => '.*Batch[23]/Trade[.]parquet'
        )
),
tradehistoryraw as (
    select
        $1:tradeid::BIGINT      AS tradeid,
        CONVERT_TIMEZONE('UTC', $1:th_dts::TIMESTAMP)    AS th_dts,
        $1:status::STRING       AS status
    from
        @{{ var('stage') }}/Batch1
        (
            FILE_FORMAT => 'parquet_format',
            PATTERN     => '.*TradeHistory_.*[.]parquet'
        )
),
tradehistory as (
    select
        $1:t_id::BIGINT         AS t_id,
        CONVERT_TIMEZONE('UTC', $1:t_dts::TIMESTAMP)     AS t_dts,
        $1:t_st_id::STRING      AS t_st_id,
        $1:t_tt_id::STRING      AS t_tt_id,
        $1:t_is_cash::TINYINT   AS t_is_cash,
        $1:t_s_symb::STRING     AS t_s_symb,
        $1:quantity::INT        AS quantity,
        $1:bidprice::DOUBLE     AS bidprice,
        $1:t_ca_id::BIGINT      AS t_ca_id,
        $1:executedby::STRING   AS executedby,
        $1:tradeprice::DOUBLE   AS tradeprice,
        $1:fee::DOUBLE          AS fee,
        $1:commission::DOUBLE   AS commission,
        $1:tax::DOUBLE          AS tax
    from
        @{{ var('stage') }}/Batch1
        (
            FILE_FORMAT => 'parquet_format',
            PATTERN     => '.*Trade_.*[.]parquet'
        ) t
),
trade_with_latest AS (
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
  FROM tradeincremental t
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
    tradeid,
    th_dts AS ts,
    status
  FROM tradehistoryraw
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
  FROM tradehistory  t
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