-- Databricks notebook source
-- MAGIC %md
-- MAGIC # DimBroker

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE DimBroker (${DimBroker.schema}) AS SELECT
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
  date('9999-12-31') enddate
FROM LIVE.HR
WHERE employeejobcode = 314

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # DimCompany

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE DimCompany (${DimCompany.schema} ${DimCompany.constraints}) AS SELECT 
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
    FROM Live.FinWire
    WHERE rectype = 'CMP') cmp
  JOIN LIVE.StatusType st ON cmp.status = st.st_id
  JOIN LIVE.Industry ind ON cmp.industryid = ind.in_id
) -- For NON-DQ versions handle cases where multiple records arrive on same day via a filter. same_day_filter doesn't trigger on the DQ version of this job
${same_day_filter};

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Financial

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Optimization Notes**
-- MAGIC * Using conameorcik = dc.name OR conameorcik = dc.companyid forced a BNLJ
-- MAGIC * Split up the join and then union to get a broadcash hash join

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE Financial (${Financial.schema} ${Financial.constraints}) AS SELECT 
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
    FROM Live.FinWire
    WHERE rectype = 'FIN'
  ) f 
) f
JOIN (
  SELECT 
    sk_companyid,
    name conameorcik,
    EffectiveDate,
    EndDate
  FROM LIVE.DimCompany
  UNION ALL
  SELECT 
    sk_companyid,
    cast(companyid as string) conameorcik,
    EffectiveDate,
    EndDate
  FROM LIVE.DimCompany
) dc 
ON
  f.conameorcik = dc.conameorcik 
  AND date(PTS) >= dc.effectivedate 
  AND date(PTS) < dc.enddate

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # DimSecurity

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * Some complexity in "how" to handle the conameorcik. If its numeric, its a cik ID. If its not numeric, its the coname. 
-- MAGIC * Therefore, handle the join to DimCompany by determining whether its numeric or not - but this requires 2 joins to DimCompany (when writing as 1 join and doing OR in the join condition it slowed it down because it used BNLJ)
-- MAGIC * Also need to handle the case where DimCompany record updates in-between the effective/end date for the Security record. So need to check if DimCompany record end date is before the end date of the security record (also test the other way where the effective date of DimCompany is after the effective date of security record).  This is because the join where date is between the DimCompany effective/end dates will match multiple records if the DimCompany record was updated during the duration of the Security record

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE DimSecurity (${DimSecurity.schema} ${DimSecurity.constraints}) AS SELECT 
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
      FROM Live.FinWire
      WHERE rectype = 'SEC'
      ) fws
    JOIN LIVE.StatusType s 
      ON s.ST_ID = fws.status
    ) fws
  JOIN (
    SELECT 
      sk_companyid,
      name conameorcik,
      EffectiveDate,
      EndDate
    FROM LIVE.DimCompany
    UNION ALL
    SELECT 
      sk_companyid,
      cast(companyid as string) conameorcik,
      EffectiveDate,
      EndDate
    FROM LIVE.DimCompany
  ) dc 
  ON
    fws.conameorcik = dc.conameorcik 
    AND fws.EffectiveDate < dc.EndDate
    AND fws.EndDate > dc.EffectiveDate
) fws
WHERE effectivedate != enddate
