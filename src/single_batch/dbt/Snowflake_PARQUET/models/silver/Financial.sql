{{
    config(
        materialized = 'table',
    )
}}
SELECT 
    dc.sk_companyid, 
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
    CAST(SUBSTR(value::STRING, 156, 13) AS INT) AS fi_out_dilut
FROM {{ ref('FinWire') }} f
JOIN {{ ref('DimCompany') }} dc
ON rectype = 'FIN_NAME'
  AND TRIM(SUBSTR(value::STRING, 169, 60)) = dc.name
  AND f.recdate >= dc.effectivedate
  AND f.recdate < dc.enddate
UNION ALL
SELECT 
    dc.sk_companyid,
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
    CAST(SUBSTR(value::STRING, 156, 13) AS INT) AS fi_out_dilut
FROM {{ ref('FinWire') }} f
JOIN {{ ref('DimCompany') }} dc
ON rectype = 'FIN_COMPANYID'
  AND TRY_CAST(TRIM(SUBSTR(value::STRING, 169, 60)) AS INT) = dc.companyid
  AND f.recdate >= dc.effectivedate
  AND f.recdate < dc.enddate