-- Databricks notebook source
-- MAGIC %md
-- MAGIC # This notebook is a Duplicate of the silver notebook, except this version has some tables with constraints added
-- MAGIC * If you want to add data quality checks to the pipeline, use this version of the silver notebook instead

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # DimBroker

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE DimBroker (${dimbrokerschema}) PARTITIONED BY (iscurrent) AS SELECT
  cast(employeeid as BIGINT) brokerid,
  cast(managerid as BIGINT) managerid,
  employeefirstname firstname,
  employeelastname lastname,
  employeemi middleinitial,
  employeebranch branch,
  employeeoffice office,
  employeephone phone,
  true iscurrent,
  1 batchid,
  (SELECT min(to_date(datevalue)) as effectivedate FROM LIVE.DimDate) effectivedate,
  cast('9999-12-31' as date) enddate
FROM LIVE.HR
WHERE employeejobcode = 314

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # DimCompany

-- COMMAND ----------

CREATE LIVE VIEW FinwireCmpView AS SELECT
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
FROM Live.FinWire
WHERE rectype = 'CMP'

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE DimCompany (
  ${dimcompanyschema}
  CONSTRAINT effectivedate_enddate_not_equal EXPECT (enddate != effectivedate) ON VIOLATION DROP ROW)
  PARTITIONED BY (iscurrent)
AS SELECT
  cast(cik as BIGINT) companyid,
  st.st_name status,
  companyname name,
  ind.in_name industry,
  CASE
    WHEN SPrating IN ('AAA','AA','A','BBB','BB','B','CCC','CC','C','D','AA+','A+','BBB+','BB+','B+','CCC+','AA-','A-','BBB-','BB-','B-','CCC-')  THEN SPrating
    ELSE cast(null as string)
    END as sprating, 
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
  CASE
    WHEN lead(pts) OVER (PARTITION BY cik ORDER BY pts) is null then true
    ELSE false END iscurrent,
  1 batchid,
  date(pts) effectivedate,
  coalesce(
    lead(date(pts)) OVER (PARTITION BY cik ORDER BY pts),
    cast('9999-12-31' as date)) enddate
FROM LIVE.FinwireCmpView cmp
JOIN LIVE.StatusType st ON cmp.status = st.st_id
JOIN LIVE.Industry ind ON cmp.industryid = ind.in_id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Financial

-- COMMAND ----------

CREATE OR REFRESH TEMPORARY LIVE TABLE FinwireFinTemp
AS SELECT
  to_timestamp(substring(value, 1, 15), 'yyyyMMdd-HHmmss') AS PTS,
  cast(substring(value, 19, 4) AS INT) AS Year,
  cast(substring(value, 23, 1) AS INT) AS Quarter,
  to_date(substring(value, 24, 8), 'yyyyMMdd') AS QtrStartDate,
  to_date(substring(value, 32, 8), 'yyyyMMdd') AS PostingDate,
  cast(substring(value, 40, 17) AS DOUBLE) AS Revenue,
  cast(substring(value, 57, 17) AS DOUBLE) AS Earnings,
  cast(substring(value, 74, 12) AS DOUBLE) AS EPS,
  cast(substring(value, 86, 12) AS DOUBLE) AS DilutedEPS,
  cast(substring(value, 98, 12) AS DOUBLE) AS Margin,
  cast(substring(value, 110, 17) AS DOUBLE) AS Inventory,
  cast(substring(value, 127, 17) AS DOUBLE) AS Assets,
  cast(substring(value, 144, 17) AS DOUBLE) AS Liabilities,
  cast(substring(value, 161, 13) AS BIGINT) AS ShOut,
  cast(substring(value, 174, 13) AS BIGINT) AS DilutedShOut,
  trim(substring(value, 187, 60)) coname,
  cast(trim(substring(value, 187, 60)) as bigint) cik,
  date(to_timestamp(substring(value, 1, 15), 'yyyyMMdd-HHmmss')) AS recorddate  -- Col not kept but is used in filtering for target table
FROM Live.FinWire
WHERE rectype = 'FIN'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Optimization Notes
-- MAGIC * Using conameorcik = dc.name OR conameorcik = dc.companyid forced a BNLJ
-- MAGIC * Split up the join and then union to get a broadcash hash join

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE Financial (${financialschema}
  CONSTRAINT valid_company_join EXPECT (sk_companyid IS NOT NULL))
AS SELECT 
  nvl(dccik.sk_companyid, dcname.sk_companyid) sk_companyid,
  year fi_year,
  quarter fi_qtr,
  qtrstartdate fi_qtr_start_date,
  revenue fi_revenue,
  earnings fi_net_earn,
  eps fi_basic_eps,
  dilutedeps fi_dilut_eps,
  margin fi_margin,
  inventory fi_inventory,
  assets fi_assets,
  liabilities fi_liability,
  shout fi_out_basic,
  dilutedshout fi_out_dilut
FROM LIVE.FinwireFinTemp f  
LEFT JOIN LIVE.DimCompany dcname
  ON 
    isnull(f.cik) 
    AND f.coname = dcname.name 
    AND recorddate >= dcname.effectivedate 
    AND recorddate < dcname.enddate
LEFT JOIN LIVE.DimCompany dccik 
  ON 
    f.cik IS NOT NULL 
    AND f.cik = dccik.companyid 
    AND recorddate >= dccik.effectivedate 
    AND recorddate < dccik.enddate

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # DimSecurity

-- COMMAND ----------

CREATE LIVE VIEW FinwireSecView AS SELECT
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
FROM Live.FinWire
WHERE rectype = 'SEC'

-- COMMAND ----------

CREATE OR REFRESH TEMPORARY LIVE TABLE SecurityTemp --TBLPROPERTIES (delta.tuneFileSizesForRewrites = true) 
AS SELECT
  fws.Symbol,
  issue,
  s.ST_NAME as status,
  fws.Name,
  exchangeid,
  sharesoutstanding,
  firsttrade,
  firsttradeonexchange,
  fws.Dividend,
  fws.effectivedate,
  coalesce(
    lead(effectivedate) OVER (
      PARTITION BY symbol
      ORDER BY effectivedate),
    date('9999-12-31')
  ) enddate,
  conameorcik coname,
  cast(conameorcik as bigint) cik
FROM LIVE.FinwireSecView fws
JOIN LIVE.StatusType s 
  ON s.ST_ID = fws.status

-- COMMAND ----------

CREATE LIVE VIEW DimSecurityView AS SELECT 
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
  case 
    when dc_effectivedate > effectivedate then dc_effectivedate
    when dc_enddate < effectivedate then dc_enddate
    else effectivedate end as effectivedate,
  case 
    when dc_enddate < enddate then dc_enddate
    else enddate end as enddate
FROM (
  SELECT
    fws.Symbol,
    fws.issue,
    fws.status,
    fws.Name,
    fws.exchangeid,
    fws.sharesoutstanding,
    fws.firsttrade,
    fws.firsttradeonexchange,
    fws.Dividend,
    fws.effectivedate,
    fws.enddate,
    nvl(dccik.sk_companyid, dcname.sk_companyid) sk_companyid,
    nvl(dccik.EffectiveDate, dcname.EffectiveDate) dc_effectiveDate,
    nvl(dccik.EndDate, dcname.EndDate) dc_endDate
  FROM LIVE.SecurityTemp fws
  LEFT JOIN LIVE.DimCompany dcname 
    ON 
      isnull(fws.cik) 
      AND fws.coname = dcname.name 
      AND fws.EffectiveDate < dcname.EndDate
      AND fws.EndDate > dcname.EffectiveDate
  LEFT JOIN LIVE.DimCompany dccik 
    ON 
      fws.cik IS NOT NULL 
      AND fws.cik = dccik.companyid 
      AND fws.EffectiveDate < dccik.EndDate
      AND fws.EndDate > dccik.EffectiveDate)

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE DimSecurity (${dimsecurityschema}
  CONSTRAINT valid_conameorcik_join EXPECT (sk_companyid IS NOT NULL)) 
  PARTITIONED BY (iscurrent) 
AS SELECT 
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
  CASE
    WHEN lead(effectivedate) OVER (
      PARTITION BY Symbol
      ORDER BY effectivedate) IS NULL THEN true
    ELSE false
    END iscurrent,
  1 batchid,
  effectivedate,
  enddate
FROM LIVE.DimSecurityView
