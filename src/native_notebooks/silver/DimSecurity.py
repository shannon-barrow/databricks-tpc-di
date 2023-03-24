# Databricks notebook source
# MAGIC %md
# MAGIC # Setup

# COMMAND ----------

user_name = spark.sql("select current_user()").collect()[0][0].split("@")[0].replace(".","_")
dbutils.widgets.text("wh_db", f"{user_name}_TPCDI",'Root name of Target Warehouse')
wh_db = f"{dbutils.widgets.get('wh_db')}_wh"
staging_db = f"{dbutils.widgets.get('wh_db')}_stage"

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

# COMMAND ----------

# spark.sql(f"""
#   CREATE OR REPLACE TEMPORARY TABLE FinwireSecView AS
#   SELECT
#     fws.Symbol,
#     issue,
#     s.ST_NAME as status,
#     fws.Name,
#     exchangeid,
#     sharesoutstanding,
#     firsttrade,
#     firsttradeonexchange,
#     fws.Dividend,
#     fws.effectivedate,
#     coalesce(
#       lead(effectivedate) OVER (
#         PARTITION BY symbol
#         ORDER BY effectivedate),
#       date('9999-12-31')
#     ) enddate,
#     conameorcik coname,
#     cast(conameorcik as bigint) cik
#   FROM (
#     SELECT
#       date(to_timestamp(substring(value, 1, 15), 'yyyyMMdd-HHmmss')) AS effectivedate,
#       trim(substring(value, 19, 15)) AS Symbol,
#       trim(substring(value, 34, 6)) AS issue,
#       trim(substring(value, 40, 4)) AS Status,
#       trim(substring(value, 44, 70)) AS Name,
#       trim(substring(value, 114, 6)) AS exchangeid,
#       cast(substring(value, 120, 13) as BIGINT) AS sharesoutstanding,
#       to_date(substring(value, 133, 8), 'yyyyMMdd') AS firsttrade,
#       to_date(substring(value, 141, 8), 'yyyyMMdd') AS firsttradeonexchange,
#       cast(substring(value, 149, 12) AS DOUBLE) AS Dividend,
#       trim(substring(value, 161, 60)) AS conameorcik
#     FROM {staging_db}.finwire
#     WHERE rectype = 'SEC') fws
#   JOIN {wh_db}.StatusType s 
#     ON s.ST_ID = fws.status
# """)

# spark.sql(f"""
#   CREATE OR REPLACE TEMPORARY VIEW SecurityView AS SELECT
#     Symbol,
#     issue,
#     status,
#     Name,
#     exchangeid,
#     sk_companyid,
#     sharesoutstanding,
#     firsttrade,
#     firsttradeonexchange,
#     Dividend,
#     case 
#       when dc_effectivedate > effectivedate then dc_effectivedate
#       when dc_enddate < effectivedate then dc_enddate
#       else effectivedate end as effectivedate,
#     case 
#       when dc_enddate < enddate then dc_enddate
#       else enddate end as enddate
#   FROM (
#     SELECT
#       fws.Symbol,
#       fws.issue,
#       fws.status,
#       fws.Name,
#       fws.exchangeid,
#       fws.sharesoutstanding,
#       fws.firsttrade,
#       fws.firsttradeonexchange,
#       fws.Dividend,
#       fws.effectivedate,
#       fws.enddate,
#       nvl(dccik.sk_companyid, dcname.sk_companyid) sk_companyid,
#       nvl(dccik.EffectiveDate, dcname.EffectiveDate) dc_effectiveDate,
#       nvl(dccik.EndDate, dcname.EndDate) dc_endDate
#     FROM FinwireSecView fws
#     LEFT JOIN {wh_db}.DimCompany dcname 
#       ON 
#         isnull(fws.cik) 
#         AND fws.coname = dcname.name 
#         AND fws.EffectiveDate < dcname.EndDate
#         AND fws.EndDate > dcname.EffectiveDate
#     LEFT JOIN {wh_db}.DimCompany dccik 
#       ON 
#         fws.cik IS NOT NULL 
#         AND fws.cik = dccik.companyid 
#         AND fws.EffectiveDate < dccik.EndDate
#         AND fws.EndDate > dccik.EffectiveDate)
# """)

# spark.sql(f"""
#   INSERT INTO {wh_db}.DimSecurity (
#     symbol,
#     issue,
#     status,
#     name,
#     exchangeid,
#     sk_companyid,
#     sharesoutstanding,
#     firsttrade,
#     firsttradeonexchange,
#     dividend,
#     iscurrent,
#     batchid,
#     effectivedate,
#     enddate)
#   SELECT
#     Symbol,
#     issue,
#     status,
#     Name,
#     exchangeid,
#     sk_companyid,
#     sharesoutstanding,
#     firsttrade,
#     firsttradeonexchange,
#     Dividend,
#     CASE
#       WHEN lead(effectivedate) OVER (
#         PARTITION BY Symbol
#         ORDER BY effectivedate) IS NULL THEN true
#       ELSE false
#       END iscurrent,
#     1 batchid,
#     effectivedate,
#     enddate
#   FROM SecurityView
# """)
