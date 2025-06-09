{{
    config(
        materialized = 'table'
    )
}}
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
  if(enddate = date('9999-12-31'), True, False) iscurrent,
  1 batchid,
  effectivedate,
  monotonically_increasing_id() as sk_securityid,
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
      nvl(string(try_cast(conameorcik as bigint)), conameorcik) conameorcik,
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
      FROM {{ ref('FinWire') }}
      WHERE rectype = 'SEC'
      ) fws
    JOIN {{ ref('StatusType') }} s
      ON s.ST_ID = fws.status
    ) fws
  JOIN (
    SELECT 
      sk_companyid,
      name conameorcik,
      EffectiveDate,
      EndDate
    FROM {{ ref('DimCompany') }}
    UNION ALL
    SELECT 
      sk_companyid,
      cast(companyid as string) conameorcik,
      EffectiveDate,
      EndDate
    FROM {{ ref('DimCompany') }}
  ) dc 
  ON
    fws.conameorcik = dc.conameorcik 
    AND fws.EffectiveDate < dc.EndDate
    AND fws.EndDate > dc.EffectiveDate
) fws
WHERE effectivedate != enddate
