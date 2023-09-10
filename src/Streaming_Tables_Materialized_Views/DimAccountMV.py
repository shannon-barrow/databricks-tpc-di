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

query = f"""CREATE {table_or_mv} IF NOT EXISTS {catalog}.{wh_db}.DimAccount AS SELECT
  bigint(concat(a.accountid,date_format(a.effectivedate, 'DDDyyyy'))) sk_accountid,
  a.accountid,
  b.sk_brokerid,
  a.sk_customerid,
  a.accountdesc,
  a.TaxStatus,
  a.status,
  if(a.enddate = date('9999-12-31'), true, false) iscurrent,
  a.batchid,
  a.effectivedate,
  a.enddate
FROM (
  SELECT
    a.* except(effectivedate, enddate, customerid),
    c.sk_customerid,
    if(a.effectivedate < c.effectivedate, c.effectivedate, a.effectivedate) effectivedate,
    if(a.enddate > c.enddate, c.enddate, a.enddate) enddate
  FROM (
    SELECT *
    FROM (
      SELECT
        accountid,
        customerid,
        coalesce(accountdesc, last_value(accountdesc) IGNORE NULLS OVER (
          PARTITION BY accountid ORDER BY update_ts)) accountdesc,
        coalesce(taxstatus, last_value(taxstatus) IGNORE NULLS OVER (
          PARTITION BY accountid ORDER BY update_ts)) taxstatus,
        coalesce(brokerid, last_value(brokerid) IGNORE NULLS OVER (
          PARTITION BY accountid ORDER BY update_ts)) brokerid,
        coalesce(status, last_value(status) IGNORE NULLS OVER (
          PARTITION BY accountid ORDER BY update_ts)) status,
        date(update_ts) effectivedate,
        nvl(lead(date(update_ts)) OVER (PARTITION BY accountid ORDER BY update_ts), date('9999-12-31')) enddate,
        batchid
      FROM (
        SELECT
          accountid,
          customerid,
          accountdesc,
          taxstatus,
          brokerid,
          status,
          update_ts,
          1 batchid
        FROM {catalog}.{staging_db}.CustomerMgmt c
        WHERE ActionType NOT IN ('UPDCUST', 'INACT')
        UNION ALL
        SELECT
          accountid,
          a.ca_c_id customerid,
          accountDesc,
          TaxStatus,
          a.ca_b_id brokerid,
          st_name as status,
          TIMESTAMP(bd.batchdate) update_ts,
          a.batchid
        FROM {catalog}.{staging_db}.AccountIncremental a
        JOIN {catalog}.{wh_db}.BatchDate bd
          ON a.batchid = bd.batchid
        JOIN {catalog}.{wh_db}.StatusType st 
          ON a.CA_ST_ID = st.st_id
      ) a
    ) a
    WHERE a.effectivedate < a.enddate
  ) a
  FULL OUTER JOIN {catalog}.{staging_db}.DimCustomerStg c 
    ON 
      a.customerid = c.customerid
      AND c.enddate > a.effectivedate
      AND c.effectivedate < a.enddate
) a
JOIN {catalog}.{wh_db}.DimBroker b 
  ON a.brokerid = b.brokerid"""

# COMMAND ----------

display(serverless_client.sql(sql_statement = query))
