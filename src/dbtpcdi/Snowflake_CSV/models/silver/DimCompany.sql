{{
    config(
        materialized = 'table'
    )
}}
WITH cmp AS (
  SELECT
    recdate,
    TRIM(SUBSTR(value, 1, 60)) AS CompanyName,
    TRIM(SUBSTR(value, 61, 10)) AS CIK,
    TRIM(SUBSTR(value, 71, 4)) AS Status,
    TRIM(SUBSTR(value, 75, 2)) AS IndustryID,
    TRIM(SUBSTR(value, 77, 4)) AS SPrating,
    TO_DATE(NULLIF(TRIM(SUBSTR(value, 81, 8)), ''), 'YYYYMMDD') AS FoundingDate,
    TRIM(SUBSTR(value, 89, 80)) AS AddrLine1,
    TRIM(SUBSTR(value, 169, 80)) AS AddrLine2,
    TRIM(SUBSTR(value, 249, 12)) AS PostalCode,
    TRIM(SUBSTR(value, 261, 25)) AS City,
    TRIM(SUBSTR(value, 286, 20)) AS StateProvince,
    TRIM(SUBSTR(value, 306, 24)) AS Country,
    TRIM(SUBSTR(value, 330, 46)) AS CEOname,
    TRIM(SUBSTR(value, 376, 150)) AS Description
  FROM {{ ref('FinWire') }}
  WHERE rectype = 'CMP'
),
cmp_transformed AS (
  SELECT
    CAST(CIK AS NUMBER) AS companyid,
    CASE
      WHEN cmp.Status = 'ACTV' THEN 'Active'
      WHEN cmp.Status = 'CMPT' THEN 'Completed'
      WHEN cmp.Status = 'CNCL' THEN 'Canceled'
      WHEN cmp.Status = 'PNDG' THEN 'Pending'
      WHEN cmp.Status = 'SBMT' THEN 'Submitted'
      WHEN cmp.Status = 'INAC' THEN 'Inactive'
      ELSE NULL
    END AS status,
    CompanyName AS name,
    ind.in_name AS industry,
    CASE 
      WHEN SPrating IN ('AAA','AA','AA+','AA-','A','A+','A-','BBB','BBB+','BBB-','BB','BB+','BB-','B','B+','B-','CCC','CCC+','CCC-','CC','C','D')
      THEN SPrating
      ELSE NULL 
    END AS sprating,
    CASE
      WHEN SPrating IN ('AAA','AA','A','AA+','A+','AA-','A-','BBB','BBB+','BBB-') THEN FALSE
      WHEN SPrating IN ('BB','B','CCC','CC','C','D','BB+','B+','CCC+','BB-','B-','CCC-') THEN TRUE
      ELSE NULL
    END AS islowgrade,
    CEOname AS ceo,
    AddrLine1 AS addressline1,
    AddrLine2 AS addressline2,
    PostalCode,
    City,
    StateProvince AS stateprov,
    Country,
    Description,
    FoundingDate,
    1 AS batchid,
    recdate AS effectivedate,
    coalesce(
      lead(date(recdate)) OVER (PARTITION BY cik ORDER BY recdate),
      cast('9999-12-31' as date)) enddate
  FROM cmp
  JOIN {{ ref('Industry') }} ind
    ON cmp.IndustryID = ind.in_id
)
SELECT 
  CAST(CONCAT(TO_VARCHAR(effectivedate, 'YYYYMMDD'), CAST(companyid AS VARCHAR)) AS NUMBER) AS sk_companyid,
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
  (enddate = date('9999-12-31')) AS iscurrent,
  batchid,
  effectivedate,
  enddate
FROM cmp_transformed
WHERE effectivedate < enddate