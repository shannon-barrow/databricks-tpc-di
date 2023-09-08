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

query = f"""CREATE OR REPLACE {table_or_mv} {catalog}.{wh_db}.Financial AS SELECT 
  sk_companyid,
  fi_year,
  fi_qtr,
  fi_qtr_start_date,
  fi_revenue,
  fi_net_earn,
  fi_basic_eps,
  fi_dilut_eps,
  fi_margin,
  fi_inventory,
  fi_assets,
  fi_liability,
  fi_out_basic,
  fi_out_dilut
FROM (
  SELECT 
    * except(conameorcik),
    nvl(string(cast(conameorcik as bigint)), conameorcik) conameorcik
  FROM (
    SELECT
      to_timestamp(substring(value, 1, 15), 'yyyyMMdd-HHmmss') AS PTS,
      cast(substring(value, 19, 4) AS INT) AS fi_year,
      cast(substring(value, 23, 1) AS INT) AS fi_qtr,
      to_date(substring(value, 24, 8), 'yyyyMMdd') AS fi_qtr_start_date,
      --to_date(substring(value, 32, 8), 'yyyyMMdd') AS PostingDate,
      cast(substring(value, 40, 17) AS DOUBLE) AS fi_revenue,
      cast(substring(value, 57, 17) AS DOUBLE) AS fi_net_earn,
      cast(substring(value, 74, 12) AS DOUBLE) AS fi_basic_eps,
      cast(substring(value, 86, 12) AS DOUBLE) AS fi_dilut_eps,
      cast(substring(value, 98, 12) AS DOUBLE) AS fi_margin,
      cast(substring(value, 110, 17) AS DOUBLE) AS fi_inventory,
      cast(substring(value, 127, 17) AS DOUBLE) AS fi_assets,
      cast(substring(value, 144, 17) AS DOUBLE) AS fi_liability,
      cast(substring(value, 161, 13) AS BIGINT) AS fi_out_basic,
      cast(substring(value, 174, 13) AS BIGINT) AS fi_out_dilut,
      trim(substring(value, 187, 60)) AS conameorcik
    FROM {catalog}.{staging_db}.FinWire
    WHERE rectype = 'FIN'
  ) f 
) f
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
  f.conameorcik = dc.conameorcik 
  AND date(PTS) >= dc.effectivedate 
  AND date(PTS) < dc.enddate"""

# COMMAND ----------

display(serverless_client.sql(sql_statement = query))
