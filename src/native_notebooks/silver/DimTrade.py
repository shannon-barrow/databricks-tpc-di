# Databricks notebook source
# MAGIC %md
# MAGIC # DimTrade

# COMMAND ----------

# MAGIC %md
# MAGIC # Setup

# COMMAND ----------

import json
import string

with open("../../tools/traditional_config.json", "r") as json_conf:
  table_conf = json.load(json_conf)['views']
  
user_name = spark.sql("select lower(regexp_replace(split(current_user(), '@')[0], '(\\\W+)', ' '))").collect()[0][0]
default_catalog = 'tpcdi' if spark.conf.get('spark.databricks.unityCatalog.enabled') == 'true' else 'hive_metastore'
default_wh = f"{string.capwords(user_name).replace(' ','_')}_TPCDI"

dbutils.widgets.text("catalog", default_catalog, 'Target Catalog')
dbutils.widgets.text("wh_db", default_wh,'Target Database')
dbutils.widgets.text("tpcdi_directory", "/tmp/tpcdi/", "Directory where Raw Files are located")
dbutils.widgets.text("scale_factor", "10", "Scale factor")
dbutils.widgets.text("batch_id", "1", "Batch ID (1,2,3)")

batch_id = dbutils.widgets.get("batch_id")
catalog = dbutils.widgets.get("catalog")
wh_db = f"{dbutils.widgets.get('wh_db')}"
staging_db = f"{dbutils.widgets.get('wh_db')}_stage"
scale_factor = dbutils.widgets.get("scale_factor")
tpcdi_directory = dbutils.widgets.get("tpcdi_directory")
files_directory = f"{tpcdi_directory}sf={scale_factor}"
hist_views = ["TradeHistory", "TradeHistoryRaw"]
tgt_cols = "tradeid, sk_brokerid, sk_createdateid, sk_createtimeid, sk_closedateid, sk_closetimeid, status, type, cashflag, sk_securityid, sk_companyid, quantity, bidprice, sk_customerid, sk_accountid, executedby, tradeprice, fee, commission, tax, batchid"
spark.sql(f"USE CATALOG {catalog}")

# COMMAND ----------

# MAGIC %md
# MAGIC # This Notebook is for historical and incremental loads
# MAGIC * When it is historical load (batchid = 1) reads from temp view of 2 large raw TEXT files (TradeHistory and Trade) 
# MAGIC * Incremental (batches 2 and 3) are being loaded via autoloader into staging DB TradeIncremental table

# COMMAND ----------

if batch_id == '1':
  for view in hist_views:
    spark.read.csv(
      f"{files_directory}/{table_conf[view]['path']}/{table_conf[view]['filename']}", 
      schema=table_conf[view]['raw_schema'], 
      sep=table_conf[view]['sep'], 
      header=table_conf[view]['header'], 
      inferSchema=False).createOrReplaceTempView(f"{view}")
  
  trade_query = f"""
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
    FROM TradeHistory t
    JOIN TradeHistoryRaw th
      ON th_t_id = t_id"""
else:
  trade_query = f"""
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
    FROM {staging_db}.TradeIncremental t
    WHERE batchid = cast({batch_id} as int)"""

# COMMAND ----------

scd1_query = f"""
  SELECT 
    * EXCEPT(t_dts, createdate),
    nvl2(sk_createdateid, createdate, cast(null as timestamp)) createdate
  FROM (
    SELECT
      tradeid,
      min(date(t_dts)) OVER (PARTITION BY tradeid) createdate,
      t_dts,
      coalesce(sk_createdateid, first_value(sk_createdateid) IGNORE NULLS OVER (
        PARTITION BY tradeid ORDER BY t_dts)) sk_createdateid,
      coalesce(sk_createtimeid, first_value(sk_createtimeid) IGNORE NULLS OVER (
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
      FROM ({trade_query}) t
      JOIN {wh_db}.DimDate dd
        ON date(t.t_dts) = dd.datevalue
      JOIN {wh_db}.DimTime dt
        ON date_format(t.t_dts, 'HH:mm:ss') = dt.timevalue
    )
  )
  QUALIFY ROW_NUMBER() OVER (PARTITION BY tradeid ORDER BY t_dts desc) = 1
"""

# COMMAND ----------

stg_query = f"""
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
    FROM ({scd1_query}) trade
    JOIN {wh_db}.StatusType status
      ON status.st_id = trade.t_st_id
    JOIN {wh_db}.TradeType tt
      ON tt.tt_id == trade.t_tt_id
    -- Keep following two queries as LEFT JOINS until the Data Generator is fixed! 
    -- Downstream table needs all trades to flow into it otherwise fails audit checks and some trades are missing DIM table versions of the symbol or account
    LEFT JOIN {wh_db}.DimSecurity ds
      ON 
        ds.symbol = trade.t_s_symb
        AND createdate >= ds.effectivedate 
        AND createdate < ds.enddate
    LEFT JOIN {wh_db}.DimAccount da
      ON 
        trade.t_ca_id = da.accountid 
        AND createdate >= da.effectivedate 
        AND createdate < da.enddate
"""

# COMMAND ----------

merge_query = f"""
  MERGE INTO {wh_db}.DimTrade t
  USING ({stg_query}) s
    ON t.tradeid = s.tradeid
  WHEN MATCHED THEN UPDATE SET
      sk_closedateid = s.sk_closedateid,
      sk_closetimeid = s.sk_closetimeid,
      status = s.status,
      type = s.type,
      cashflag = s.cashflag,
      quantity = s.quantity,
      bidprice = s.bidprice,
      executedby = s.executedby,
      tradeprice = s.tradeprice,
      fee = s.fee,
      commission = s.commission,
      tax = s.tax,
      batchid = s.batchid
  WHEN NOT MATCHED THEN INSERT ({tgt_cols})
  VALUES ({tgt_cols})
"""

# COMMAND ----------

if batch_id == '1': spark.sql(f"INSERT INTO {wh_db}.DimTrade {stg_query}")
else: spark.sql(merge_query)

# COMMAND ----------

spark.sql(f"ANALYZE TABLE {wh_db}.DimTrade COMPUTE STATISTICS FOR ALL COLUMNS")
