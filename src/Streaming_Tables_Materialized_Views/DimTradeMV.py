# Databricks notebook source
from dbsqlclient import ServerlessClient
import json
import re
import string

# COMMAND ----------

user_name = spark.sql("select lower(regexp_replace(split(current_user(), '@')[0], '(\\\W+)', ' '))").collect()[0][0]
default_catalog = 'tpcdi' if spark.conf.get('spark.databricks.unityCatalog.enabled') == 'true' else 'hive_metastore'
dbutils.widgets.text("scale_factor", "10", "Scale factor")
dbutils.widgets.text("catalog", default_catalog, 'Target Catalog')
scale_factor = dbutils.widgets.get("scale_factor")
default_wh = f"{string.capwords(user_name).replace(' ','_')}_TPCDI_STMV_{scale_factor}"
dbutils.widgets.text("wh_db", default_wh,'Target Database')
dbutils.widgets.text("warehouse_id", '', "Warehouse ID")

catalog = dbutils.widgets.get("catalog")
dbutils.widgets.dropdown("table_or_mv", "MATERIALIZED VIEW", ['TABLE', 'MATERIALIZED VIEW'], "Table Type")
table_or_mv = dbutils.widgets.get("table_or_mv")
wh_db = dbutils.widgets.get('wh_db')
staging_db = f"{wh_db}_stage"
warehouse_id = dbutils.widgets.get("warehouse_id")
serverless_client = ServerlessClient(warehouse_id=warehouse_id)

# COMMAND ----------

query = f"""CREATE OR REPLACE {table_or_mv} {catalog}.{wh_db}.DimTrade AS SELECT
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
        FROM {catalog}.{staging_db}.TradeHistory t
        JOIN {catalog}.{staging_db}.TradeHistoryRaw th
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
        FROM {catalog}.{staging_db}.TradeIncremental t
      ) t
      JOIN {catalog}.{wh_db}.DimDate dd
        ON date(t.t_dts) = dd.datevalue
      JOIN {catalog}.{wh_db}.DimTime dt
        ON date_format(t.t_dts, 'HH:mm:ss') = dt.timevalue
    )
  )
  QUALIFY ROW_NUMBER() OVER (PARTITION BY tradeid ORDER BY t_dts desc) = 1
) trade
JOIN {catalog}.{wh_db}.StatusType status
  ON status.st_id = trade.t_st_id
JOIN {catalog}.{wh_db}.TradeType tt
  ON tt.tt_id == trade.t_tt_id
JOIN {catalog}.{wh_db}.DimSecurity ds
  ON 
    ds.symbol = trade.t_s_symb
    AND createdate >= ds.effectivedate 
    AND createdate < ds.enddate
JOIN {catalog}.{wh_db}.DimAccount da
  ON 
    trade.t_ca_id = da.accountid 
    AND createdate >= da.effectivedate 
    AND createdate < da.enddate"""

# COMMAND ----------

display(serverless_client.sql(sql_statement = query))
