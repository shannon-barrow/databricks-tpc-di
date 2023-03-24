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
# MAGIC # Create View on FinWire Then Write into DimCompany

# COMMAND ----------

spark.sql(f"""
  CREATE OR REPLACE TEMPORARY VIEW FinwireCmpView AS
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
  FROM {staging_db}.finwire
  WHERE rectype = 'CMP'
""")

# COMMAND ----------

spark.sql(f"""
INSERT INTO {wh_db}.DimCompany (
  companyid, 
  status, 
  name, 
  industry, 
  sprating, 
  islowgrade, 
  ceo, 
  addressline1, 
  addressline2, 
  postalcode, 
  city, 
  stateprov, 
  country, 
  description, 
  foundingdate, 
  iscurrent, 
  batchid, 
  effectivedate, 
  enddate
)
SELECT * 
FROM ( 
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
      lead(date(pts))
        OVER (
          PARTITION BY cik
          ORDER BY pts),
      cast('9999-12-31' as date)) enddate
  FROM FinwireCmpView cmp
  JOIN {wh_db}.StatusType st ON cmp.status = st.st_id
  JOIN {wh_db}.Industry ind ON cmp.industryid = ind.in_id
)
WHERE effectivedate < enddate
""")

# COMMAND ----------

spark.sql(f"ANALYZE TABLE {wh_db}.DimCompany COMPUTE STATISTICS FOR ALL COLUMNS")

# COMMAND ----------

# MAGIC %md
# MAGIC # Moved This DIMessages Alert to Batch Validation Script
# MAGIC * Remove Invalid SPRating's And Report to DIMessages as Alerts  
# MAGIC 4.5.3.3 A record will be inserted in the DImessages table if a company’s SPRating is not one of the valid values for Standard & Poor long-term credit-ratings. The MessageSource is “DimCompany”, the MessageType is “Alert” and the MessageText is “Invalid SPRating”. The MessageData field is “CO_ID = ” followed by the key value of the record, then “, CO_SP_RATE = ” and the CO_SP_RATE value. The SPRating and isLowGrade columns will be set to NULL in this case. The valid values are: AAA, AA[+/-], A[+/-], BBB[+/-], BB[+/-], B[+/-], CCC[+/-], CC, C,
# MAGIC D.

# COMMAND ----------

# spark.sql(f"""
#   INSERT INTO {wh_db}.DIMessages
#   SELECT
#     CURRENT_TIMESTAMP() as MessageDateAndTime,
#     1 BatchID,
#     'DimCompany' MessageSource,
#     'Invalid SPRating' MessageText,
#     'Alert' as MessageType,
#     concat('CO_ID = ', cik, ', CO_SP_RATE = ', sprating) MessageData
#   FROM FinwireCmpView
#   WHERE sprating IN ('AAA','AA','A','BBB','BB','B','CCC','CC','C','D','AA+','A+','BBB+','BB+','B+','CCC+','AA-','A-','BBB-','BB-','B-','CCC-')
# """)
