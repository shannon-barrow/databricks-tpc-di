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

query = f"""CREATE OR REPLACE {table_or_mv} {catalog}.{wh_db}.DimCompany AS SELECT 
  bigint(concat(companyid,date_format(effectivedate, 'DDDyyyy'))) sk_companyid,
  * FROM (
  SELECT
    cast(cik as BIGINT) companyid,
    st.st_name status,
    companyname name,
    ind.in_name industry,
    if(
      SPrating IN ('AAA','AA','AA+','AA-','A','A+','A-','BBB','BBB+','BBB-','BB','BB+','BB-','B','B+','B-','CCC','CCC+','CCC-','CC','C','D'), 
      SPrating, 
      cast(null as string)) sprating, 
    CASE
      WHEN SPrating IN ('AAA','AA','A','AA+','A+','AA-','A-','BBB','BBB+','BBB-') THEN false
      WHEN SPrating IN ('BB','B','CCC','CC','C','D','BB+','B+','CCC+','BB-','B-','CCC-') THEN true
      ELSE cast(null as boolean)
      END as islowgrade, 
    ceoname ceo,
    addrline1 addressline1,
    addrline2 addressline2,
    postalcode,
    city,
    stateprovince stateprov,
    country,
    description,
    foundingdate,
    nvl2(lead(pts) OVER (PARTITION BY cik ORDER BY pts), true, false) iscurrent,
    1 batchid,
    date(pts) effectivedate,
    coalesce(
      lead(date(pts)) OVER (PARTITION BY cik ORDER BY pts),
      cast('9999-12-31' as date)) enddate
  FROM (
    SELECT
      to_timestamp(substring(value, 1, 15), 'yyyyMMdd-HHmmss') AS PTS,
      trim(substring(value, 19, 60)) AS CompanyName,
      trim(substring(value, 79, 10)) AS CIK,
      trim(substring(value, 89, 4)) AS Status,
      trim(substring(value, 93, 2)) AS IndustryID,
      trim(substring(value, 95, 4)) AS SPrating,
      to_date(substring(value, 99, 8), 'yyyyMMdd') AS FoundingDate,
      trim(substring(value, 107, 80)) AS AddrLine1,
      trim(substring(value, 187, 80)) AS AddrLine2,
      trim(substring(value, 267, 12)) AS PostalCode,
      trim(substring(value, 279, 25)) AS City,
      trim(substring(value, 304, 20)) AS StateProvince,
      trim(substring(value, 324, 24)) AS Country,
      trim(substring(value, 348, 46)) AS CEOname,
      trim(substring(value, 394, 150)) AS Description
    FROM {catalog}.{staging_db}.FinWire
    WHERE rectype = 'CMP') cmp
  JOIN {catalog}.{wh_db}.StatusType st ON cmp.status = st.st_id
  JOIN {catalog}.{wh_db}.Industry ind ON cmp.industryid = ind.in_id
)
where effectivedate < enddate"""

# COMMAND ----------

display(serverless_client.sql(sql_statement = query))
