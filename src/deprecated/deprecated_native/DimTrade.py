# Databricks notebook source
dbutils.widgets.text("env",'a01','Name of the environment')
dbutils.widgets.text("files_directory", "/tmp/tpc-di", "Directory where Files are located")
dbutils.widgets.text("scale_factor", "10", "Scale factor")
dbutils.widgets.text("batch_id", "2", "Batch ID (1,2,3)")

env = dbutils.widgets.get("env")
files_directory = dbutils.widgets.get("files_directory")
batch_id = dbutils.widgets.get("batch_id")
scale_factor = dbutils.widgets.get("scale_factor")
wh_db = f"{env}_tpcdi_warehouse"
spark.conf.set("spark.sql.shuffle.partitions", sc.defaultParallelism)

# COMMAND ----------

tradeIncrementalSchema = """
  cdc_flag STRING COMMENT 'Denotes insert, update',
  cdc_dsn BIGINT COMMENT 'Database Sequence Number',
  t_id BIGINT COMMENT 'Trade identifier.',
  t_dts TIMESTAMP COMMENT 'Date and time of trade.',
  t_st_id STRING COMMENT 'Status type identifier',
  t_tt_id STRING COMMENT 'Trade type identifier',
  t_is_cash TINYINT COMMENT 'Is this trade a cash (‘1’) or margin (‘0’) trade?',
  t_s_symb STRING COMMENT 'Security symbol of the security',
  t_qty INT COMMENT 'Quantity of securities traded.',
  t_bid_price DOUBLE COMMENT 'The requested unit price.',
  t_ca_id BIGINT COMMENT 'Customer account identifier.',
  t_exec_name STRING COMMENT 'Name of the person executing the trade.',
  t_trade_price DOUBLE COMMENT 'Unit price at which the security was traded.',
  t_chrg DOUBLE COMMENT 'Fee charged for placing this trade request.',
  t_comm DOUBLE COMMENT 'Commission earned on this trade',
  t_tax DOUBLE COMMENT 'Amount of tax due on this trade'
"""

# COMMAND ----------

spark.read.csv(f'{files_directory}/{scale_factor}/Batch{batch_id}/Trade.txt', schema=tradeIncrementalSchema, sep='|', header=False, inferSchema=False).createOrReplaceTempView('v_trade')

# COMMAND ----------

spark.sql(f"""
  CREATE OR REPLACE TEMPORARY VIEW TradeIncrementalView AS
  SELECT
    t_id tradeid,
    t_dts,
    CASE 
      WHEN cdc_flag = 'I' THEN sk_dateid 
      ELSE cast(NULL AS BIGINT) 
      END AS sk_createdateid,
    CASE 
      WHEN cdc_flag = 'I' THEN sk_timeid 
      ELSE cast(NULL AS BIGINT) 
      END AS sk_createtimeid,
    CASE 
      WHEN 
        t_st_id IN ("CMPT", "CNCL") THEN sk_dateid 
      ELSE cast(NULL AS BIGINT) 
      END AS sk_closedateid,
    CASE 
      WHEN 
        t_st_id IN ("CMPT", "CNCL") THEN sk_timeid 
      ELSE cast(NULL AS BIGINT) 
      END AS sk_closetimeid,
    CASE 
      WHEN t_is_cash = 1 then TRUE
      WHEN t_is_cash = 0 then FALSE
      ELSE cast(null as BOOLEAN) 
      END AS cashflag,
    t_qty AS quantity,
    t_bid_price AS bidprice,
    t_exec_name AS executedby,
    t_trade_price AS tradeprice,
    t_chrg AS fee,
    t_comm AS commission,
    t_tax AS tax,
    t_st_id,
    t_tt_id,
    t_s_symb,
    t_ca_id
  FROM v_trade trade
  JOIN {wh_db}.DimDate dd
    ON to_date(trade.t_dts) = dd.datevalue
  JOIN {wh_db}.DimTime dt
    ON date_format(trade.t_dts, 'HH:mm:ss') = dt.timevalue
""")

# COMMAND ----------

spark.sql(f"""
  CREATE OR REPLACE TEMPORARY VIEW TradeView AS
  SELECT * 
  FROM (
    SELECT
      tradeid,
      t_dts,
      min(t_dts) OVER (PARTITION BY tradeid) first_trade_ts,
      coalesce(
        sk_createdateid,
        last_value(sk_createdateid) IGNORE NULLS OVER (
          PARTITION BY tradeid
          ORDER BY t_dts
        )
      ) sk_createdateid,
      coalesce(
        sk_createtimeid,
        last_value(sk_createtimeid) IGNORE NULLS OVER (
          PARTITION BY tradeid
          ORDER BY t_dts
        )
      ) sk_createtimeid,
      coalesce(
        sk_closedateid,
        last_value(sk_closedateid) IGNORE NULLS OVER (
          PARTITION BY tradeid
          ORDER BY t_dts
        )
      ) sk_closedateid,
      coalesce(
        sk_closetimeid,
        last_value(sk_closetimeid) IGNORE NULLS OVER (
          PARTITION BY tradeid
          ORDER BY t_dts
        )
      ) sk_closetimeid,
      cashflag,
      quantity,
      bidprice,
      executedby,
      tradeprice,
      fee,
      commission,
      tax,
      t_st_id,
      t_tt_id,
      t_s_symb,
      t_ca_id
    FROM TradeIncrementalView)
  QUALIFY ROW_NUMBER() OVER (PARTITION BY tradeid ORDER BY t_dts desc) = 1
""")

# COMMAND ----------

spark.sql(f"""
  CREATE OR REPLACE TEMPORARY VIEW DimTradeView AS
  SELECT
    tradeid,
    sk_brokerid,
    sk_createdateid,
    sk_createtimeid,
    sk_closedateid,
    sk_closetimeid,
    st_name AS status,
    tt_name AS type,
    cashflag,
    sk_securityid,
    sk_companyid,
    quantity,
    bidprice,
    sk_customerid,
    sk_accountid,
    executedby,
    tradeprice,
    fee,
    commission,
    tax,
    {batch_id} AS batchid
  FROM TradeView trade
  JOIN {wh_db}.StatusType status
    ON status.st_id = trade.t_st_id
  JOIN {wh_db}.TradeType tt
    ON tt.tt_id == trade.t_tt_id
  JOIN {wh_db}.DimSecurity ds
    ON 
      ds.symbol = trade.t_s_symb
      AND to_date(trade.first_trade_ts) >= ds.effectivedate 
      AND to_date(trade.first_trade_ts) < ds.enddate
  JOIN {wh_db}.DimAccount da
    ON 
      trade.t_ca_id = da.accountid 
      AND to_date(trade.first_trade_ts) >= da.effectivedate 
      AND to_date(trade.first_trade_ts) < da.enddate
""")

# COMMAND ----------

spark.sql(f"""
  MERGE INTO {wh_db}.DimTrade t
  USING DimTradeView s
    ON t.tradeid = s.tradeid
  WHEN MATCHED THEN UPDATE SET
      sk_createdateid = coalesce(t.sk_createdateid, s.sk_createdateid),
      sk_createtimeid = coalesce(t.sk_createtimeid, s.sk_createtimeid),
      sk_closedateid = coalesce(t.sk_closedateid, s.sk_closedateid),
      sk_closetimeid = coalesce(t.sk_closetimeid, s.sk_closetimeid),
      status = s.status,
      type = s.type,
      cashflag = s.cashflag,
      quantity = s.quantity,
      bidprice = s.bidprice,
      executedby = s.executedby,
      tradeprice = s.tradeprice,
      fee = s.fee,
      commission = s.commission,
      tax = s.tax
  WHEN NOT MATCHED THEN INSERT (
    tradeid,
    sk_brokerid,
    sk_createdateid,
    sk_createtimeid,
    sk_closedateid,
    sk_closetimeid,
    status,
    type,
    cashflag,
    sk_securityid,
    sk_companyid,
    quantity,
    bidprice,
    sk_customerid,
    sk_accountid,
    executedby,
    tradeprice,
    fee,
    commission,
    tax,
    batchid
  )
  VALUES (
    s.tradeid,
    s.sk_brokerid,
    s.sk_createdateid,
    s.sk_createtimeid,
    s.sk_closedateid,
    s.sk_closetimeid,
    s.status,
    s.type,
    s.cashflag,
    s.sk_securityid,
    s.sk_companyid,
    s.quantity,
    s.bidprice,
    s.sk_customerid,
    s.sk_accountid,
    s.executedby,
    s.tradeprice,
    s.fee,
    s.commission,
    s.tax,
    s.batchid
  )
""")
