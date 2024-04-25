# Databricks notebook source
# MAGIC %md
# MAGIC # Setup

# COMMAND ----------

import string
  
user_name = spark.sql("select lower(regexp_replace(split(current_user(), '@')[0], '(\\\W+)', ' '))").collect()[0][0]
default_catalog = 'tpcdi' if spark.conf.get('spark.databricks.unityCatalog.enabled') == 'true' else 'hive_metastore'
default_wh = f"{string.capwords(user_name).replace(' ','_')}_TPCDI"

dbutils.widgets.text("catalog", default_catalog, 'Target Catalog')
dbutils.widgets.text("wh_db", default_wh,'Target Database')

catalog = dbutils.widgets.get("catalog")
wh_db = f"{dbutils.widgets.get('wh_db')}"
staging_db = f"{dbutils.widgets.get('wh_db')}_stage"
spark.sql(f"USE CATALOG {catalog}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Create View on FinWire Then Write into DimSecurity
# MAGIC * Need to get start/end date
# MAGIC * Join to DimCompany on EITHER companyid or name 
# MAGIC * conameorcik col of Fin record can be either cik or name - can only tell if value is numeric or not
# MAGIC * Then handle case where DimSecurity record crosses DimCompany effective/end date boundaries (will end up with multiple security records if so)

# COMMAND ----------

spark.sql(f"""
  CREATE OR REPLACE TABLE {staging_db}.FinwireSecStg AS
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
    FROM {staging_db}.finwire
    WHERE rectype = 'SEC'
  ) fws
  JOIN {wh_db}.StatusType s 
    ON s.ST_ID = fws.status
""")

# COMMAND ----------

spark.sql(f"""
  CREATE OR REPLACE TEMPORARY VIEW SecurityView AS SELECT
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
  FROM {staging_db}.FinwireSecStg fws
  JOIN (
    SELECT 
      sk_companyid,
      name conameorcik,
      EffectiveDate,
      EndDate
    FROM {wh_db}.DimCompany
    UNION ALL
    SELECT 
      sk_companyid,
      cast(companyid as string) conameorcik,
      EffectiveDate,
      EndDate
    FROM {wh_db}.DimCompany
  ) dc 
  ON
    fws.conameorcik = dc.conameorcik 
    AND fws.EffectiveDate < dc.EndDate
    AND fws.EndDate > dc.EffectiveDate
""")

# COMMAND ----------

spark.sql(f"""
  INSERT OVERWRITE {wh_db}.DimSecurity (
    symbol,
    issue,
    status,
    name,
    exchangeid,
    sk_companyid,
    sharesoutstanding,
    firsttrade,
    firsttradeonexchange,
    dividend,
    iscurrent,
    batchid,
    effectivedate,
    enddate)
  SELECT
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
    if(enddate = date('9999-12-31'), true, false) iscurrent,
    1 batchid,
    effectivedate,
    enddate
  FROM SecurityView
""")

# COMMAND ----------

spark.sql(f"ANALYZE TABLE {wh_db}.DimSecurity COMPUTE STATISTICS FOR ALL COLUMNS")
