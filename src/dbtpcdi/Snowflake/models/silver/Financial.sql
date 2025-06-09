{{
    config(
        materialized = 'table'

    )
}}


WITH FinWire AS (
  SELECT 
      CAST(SUBSTR(value::STRING, 1, 4) AS INT) AS fi_year,
      CAST(SUBSTR(value::STRING, 5, 1) AS INT) AS fi_qtr,
      TRY_TO_DATE(SUBSTR(value::STRING, 6, 8), 'YYYYMMDD') AS fi_qtr_start_date,
      CAST(SUBSTR(value::STRING, 22, 17) AS FLOAT) AS fi_revenue,
      CAST(SUBSTR(value::STRING, 39, 17) AS FLOAT) AS fi_net_earn,
      CAST(SUBSTR(value::STRING, 56, 12) AS FLOAT) AS fi_basic_eps,
      CAST(SUBSTR(value::STRING, 68, 12) AS FLOAT) AS fi_dilut_eps,
      CAST(SUBSTR(value::STRING, 80, 12) AS FLOAT) AS fi_margin,
      CAST(SUBSTR(value::STRING, 92, 17) AS FLOAT) AS fi_inventory,
      CAST(SUBSTR(value::STRING, 109, 17) AS FLOAT) AS fi_assets,
      CAST(SUBSTR(value::STRING, 126, 17) AS FLOAT) AS fi_liability,
      CAST(SUBSTR(value::STRING, 143, 13) AS INT) AS fi_out_basic,
      CAST(SUBSTR(value::STRING, 156, 13) AS INT) AS fi_out_dilut,
      TRY_CAST(TRIM(SUBSTR(value::STRING, 169, 60)) AS INT) AS _companyid,
      TRIM(SUBSTR(value::STRING, 169, 60)) AS _name,
      rectype,
      recdate
  FROM {{ ref('FinWire') }}
)

SELECT 
    dc.sk_companyid, 
    f.fi_year,
    f.fi_qtr,
    f.fi_qtr_start_date,
    f.fi_revenue,
    f.fi_net_earn,
    f.fi_basic_eps,
    f.fi_dilut_eps,
    f.fi_margin,
    f.fi_inventory,
    f.fi_assets,
    f.fi_liability,
    f.fi_out_basic,
    f.fi_out_dilut,
    f._companyid,
    f._name,
    f.rectype,
    f.recdate
FROM FinWire f
JOIN {{ ref('DimCompany') }} dc
ON rectype = 'FIN_NAME'
  AND f._name = dc.name
  AND f.recdate >= dc.effectivedate
  AND f.recdate < dc.enddate

UNION ALL

SELECT 
   dc.sk_companyid,
   f.fi_year,
   f.fi_qtr,
   f.fi_qtr_start_date,
   f.fi_revenue,
   f.fi_net_earn,
   f.fi_basic_eps,
   f.fi_dilut_eps,
   f.fi_margin,
   f.fi_inventory,
   f.fi_assets,
   f.fi_liability,
   f.fi_out_basic,
   f.fi_out_dilut,
   f._companyid,
   f._name,
   f.rectype,
   f.recdate
FROM FinWire f
JOIN {{ ref('DimCompany') }} dc
ON rectype = 'FIN_COMPANYID'
  AND f._companyid = dc.companyid
  AND f.recdate >= dc.effectivedate
  AND f.recdate < dc.enddate