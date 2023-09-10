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

query = f"""CREATE {table_or_mv} IF NOT EXISTS {catalog}.{wh_db}.DimSecurity AS SELECT 
  monotonically_increasing_id() sk_securityid,
  Symbol,
  issue,
  status,
  Name,
  exchangeid,
  sk_companyid,
  sharesoutstanding,
  firsttrade,
  firsttradeonexchange,
  Dividend,
  if(enddate = date('9999-12-31'), True, False) iscurrent,
  1 batchid,
  effectivedate,
  enddate
 FROM (
  SELECT 
    fws.Symbol,
    fws.issue,
    fws.status,
    fws.Name,
    fws.exchangeid,
    dc.sk_companyid,
    fws.sharesoutstanding,
    fws.firsttrade,
    fws.firsttradeonexchange,
    fws.Dividend,
    if(fws.effectivedate < dc.effectivedate, dc.effectivedate, fws.effectivedate) effectivedate,
    if(fws.enddate > dc.enddate, dc.enddate, fws.enddate) enddate
  FROM (
    SELECT 
      fws.* except(Status, conameorcik),
      nvl(string(cast(conameorcik as bigint)), conameorcik) conameorcik,
      s.ST_NAME as status,
      coalesce(
        lead(effectivedate) OVER (
          PARTITION BY symbol
          ORDER BY effectivedate),
        date('9999-12-31')
      ) enddate
    FROM (
      SELECT
        date(to_timestamp(substring(value, 1, 15), 'yyyyMMdd-HHmmss')) AS effectivedate,
        trim(substring(value, 19, 15)) AS Symbol,
        trim(substring(value, 34, 6)) AS issue,
        trim(substring(value, 40, 4)) AS Status,
        trim(substring(value, 44, 70)) AS Name,
        trim(substring(value, 114, 6)) AS exchangeid,
        cast(substring(value, 120, 13) as BIGINT) AS sharesoutstanding,
        to_date(substring(value, 133, 8), 'yyyyMMdd') AS firsttrade,
        to_date(substring(value, 141, 8), 'yyyyMMdd') AS firsttradeonexchange,
        cast(substring(value, 149, 12) AS DOUBLE) AS Dividend,
        trim(substring(value, 161, 60)) AS conameorcik
      FROM {catalog}.{staging_db}.FinWire
      WHERE rectype = 'SEC'
      ) fws
    JOIN {catalog}.{wh_db}.StatusType s 
      ON s.ST_ID = fws.status
    ) fws
  JOIN (
    SELECT 
      sk_companyid,
      name conameorcik,
      EffectiveDate,
      EndDate
    FROM {catalog}.{wh_db}.DimCompany
    UNION ALL
    SELECT 
      sk_companyid,
      cast(companyid as string) conameorcik,
      EffectiveDate,
      EndDate
    FROM {catalog}.{wh_db}.DimCompany
  ) dc 
  ON
    fws.conameorcik = dc.conameorcik 
    AND fws.EffectiveDate < dc.EndDate
    AND fws.EndDate > dc.EffectiveDate
) fws
WHERE effectivedate < enddate"""

# COMMAND ----------

display(serverless_client.sql(sql_statement = query))
