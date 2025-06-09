{{
    config(
        materialized = 'table'
    )
}}
SELECT 
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
    bigint(concat(date_format(effectivedate, 'yyyyMMdd'), cast(companyid as string))) as sk_companyid,
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
      to_date(iff(trim(substring(value, 99, 8))='',NULL,substring(value, 99, 8)), 'yyyyMMdd') AS FoundingDate,
      trim(substring(value, 107, 80)) AS AddrLine1,
      trim(substring(value, 187, 80)) AS AddrLine2,
      trim(substring(value, 267, 12)) AS PostalCode,
      trim(substring(value, 279, 25)) AS City,
      trim(substring(value, 304, 20)) AS StateProvince,
      trim(substring(value, 324, 24)) AS Country,
      trim(substring(value, 348, 46)) AS CEOname,
      trim(substring(value, 394, 150)) AS Description
    FROM {{ ref('FinWire') }}
    WHERE rectype = 'CMP'
       ) cmp
  JOIN {{ ref('StatusType') }} st ON cmp.status = st.st_id
  JOIN {{ ref('Industry') }} ind ON cmp.industryid = ind.in_id
)
