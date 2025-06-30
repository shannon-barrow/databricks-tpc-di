{{
    config(
        materialized = 'table'
    )
}}
WITH customer_incremental as (
  select
    $1:cdc_flag::STRING       AS cdc_flag,
    $1:cdc_dsn::BIGINT        AS cdc_dsn,
    $1:customerid::BIGINT     AS customerid,
    $1:taxid::STRING          AS taxid,
    $1:status::STRING         AS status,
    $1:lastname::STRING       AS lastname,
    $1:firstname::STRING      AS firstname,
    $1:middleinitial::STRING  AS middleinitial,
    $1:gender::STRING         AS gender,
    $1:tier::TINYINT          AS tier,
    $1:dob::DATE              AS dob,
    $1:addressline1::STRING   AS addressline1,
    $1:addressline2::STRING   AS addressline2,
    $1:postalcode::STRING     AS postalcode,
    $1:city::STRING           AS city,
    $1:stateprov::STRING      AS stateprov,
    $1:country::STRING        AS country,
    $1:c_ctry_1::STRING       AS c_ctry_1,
    $1:c_area_1::STRING       AS c_area_1,
    $1:c_local_1::STRING      AS c_local_1,
    $1:c_ext_1::STRING        AS c_ext_1,
    $1:c_ctry_2::STRING       AS c_ctry_2,
    $1:c_area_2::STRING       AS c_area_2,
    $1:c_local_2::STRING      AS c_local_2,
    $1:c_ext_2::STRING        AS c_ext_2,
    $1:c_ctry_3::STRING       AS c_ctry_3,
    $1:c_area_3::STRING       AS c_area_3,
    $1:c_local_3::STRING      AS c_local_3,
    $1:c_ext_3::STRING        AS c_ext_3,
    $1:email1::STRING         AS email1,
    $1:email2::STRING         AS email2,
    $1:lcl_tx_id::STRING      AS lcl_tx_id,
    $1:nat_tx_id::STRING      AS nat_tx_id,
    try_cast(SUBSTR(METADATA$FILENAME, position('/Batch' in METADATA$FILENAME) + 6, 1) as int) batchid
  from
    @{{ var('stage') }}
    (
      FILE_FORMAT => 'parquet_format',
      PATTERN     => '.*Customer[.]parquet'
    ) t
),
Customers AS (
    SELECT
      customerid,
      taxid,
      status,
      lastname,
      firstname,
      middleinitial,
      gender,
      tier,
      dob,
      addressline1,
      addressline2,
      postalcode,
      city,
      stateprov,
      country,
      phone1,
      phone2,
      phone3,
      email1,
      email2,
      lcl_tx_id,
      nat_tx_id,
      1 AS batchid,
      update_ts
    FROM {{var('catalog')}}.{{var('stagingschema')}}.customermgmt_clean c
    WHERE ActionType IN ('NEW', 'INACT', 'UPDCUST')
    UNION ALL
    SELECT
        customerid,
        NULLIF(taxid, '') AS taxid,
        CASE
            WHEN status = 'ACTV' THEN 'Active'
            WHEN status = 'CMPT' THEN 'Completed'
            WHEN status = 'CNCL' THEN 'Canceled'
            WHEN status = 'PNDG' THEN 'Pending'
            WHEN status = 'SBMT' THEN 'Submitted'
            WHEN status = 'INAC' THEN 'Inactive'
            ELSE NULL
        END AS status,
        NULLIF(lastname, '') AS lastname,
        NULLIF(firstname, '') AS firstname,
        NULLIF(middleinitial, '') AS middleinitial,
        NULLIF(gender, '') AS gender,
        tier,
        dob,
        NULLIF(addressline1, '') AS addressline1,
        NULLIF(addressline2, '') AS addressline2,
        NULLIF(postalcode, '') AS postalcode,
        NULLIF(city, '') AS city,
        NULLIF(stateprov, '') AS stateprov,
        country,
        IFF(
            NULLIF(c_local_1, '') IS NOT NULL,
            CONCAT(
                IFF(NULLIF(c_ctry_1, '') IS NOT NULL, CONCAT('+', c_ctry_1, ' '), ''),
                IFF(NULLIF(c_area_1, '') IS NOT NULL, CONCAT('(', c_area_1, ') '), ''),
                c_local_1,
                COALESCE(c_ext_1, '')
            ),
            NULL
        ) AS phone1,
        IFF(
            NULLIF(c_local_2, '') IS NOT NULL,
            CONCAT(
                IFF(NULLIF(c_ctry_2, '') IS NOT NULL, CONCAT('+', c_ctry_2, ' '), ''),
                IFF(NULLIF(c_area_2, '') IS NOT NULL, CONCAT('(', c_area_2, ') '), ''),
                c_local_2,
                COALESCE(c_ext_2, '')
            ),
            NULL
        ) AS phone2,
        IFF(
            NULLIF(c_local_3, '') IS NOT NULL,
            CONCAT(
                IFF(NULLIF(c_ctry_3, '') IS NOT NULL, CONCAT('+', c_ctry_3, ' '), ''),
                IFF(NULLIF(c_area_3, '') IS NOT NULL, CONCAT('(', c_area_3, ') '), ''),
                c_local_3,
                COALESCE(c_ext_3, '')
            ),
            NULL
        ) AS phone3,
        NULLIF(email1, '') AS email1,
        NULLIF(email2, '') AS email2,
        NULLIF(lcl_tx_id, '') AS lcl_tx_id,
        NULLIF(nat_tx_id, '') AS nat_tx_id,
        c.batchid,
        TO_TIMESTAMP(bd.batchdate) AS update_ts
    FROM customer_incremental c
    JOIN {{ ref('BatchDate') }} bd
      ON c.batchid = bd.batchid
),
CustomerFinal AS (
    SELECT
      customerid,
      COALESCE(
        taxid,
        LAST_VALUE(taxid IGNORE NULLS) OVER (PARTITION BY customerid ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
      ) AS taxid,
      status,
      COALESCE(
        lastname,
        LAST_VALUE(lastname IGNORE NULLS) OVER (PARTITION BY customerid ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
      ) AS lastname,
      COALESCE(
        firstname,
        LAST_VALUE(firstname IGNORE NULLS) OVER (PARTITION BY customerid ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
      ) AS firstname,
      COALESCE(
        middleinitial,
        LAST_VALUE(middleinitial IGNORE NULLS) OVER (PARTITION BY customerid ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
      ) AS middleinitial,
      COALESCE(
        gender,
        LAST_VALUE(gender IGNORE NULLS) OVER (PARTITION BY customerid ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
      ) AS gender,
      COALESCE(
        tier,
        LAST_VALUE(tier IGNORE NULLS) OVER (PARTITION BY customerid ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
      ) AS tier,
      COALESCE(
        dob,
        LAST_VALUE(dob IGNORE NULLS) OVER (PARTITION BY customerid ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
      ) AS dob,
      COALESCE(
        addressline1,
        LAST_VALUE(addressline1 IGNORE NULLS) OVER (PARTITION BY customerid ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
      ) AS addressline1,
      COALESCE(
        addressline2,
        LAST_VALUE(addressline2 IGNORE NULLS) OVER (PARTITION BY customerid ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
      ) AS addressline2,
      COALESCE(
        postalcode,
        LAST_VALUE(postalcode IGNORE NULLS) OVER (PARTITION BY customerid ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
      ) AS postalcode,
      COALESCE(
        city,
        LAST_VALUE(city IGNORE NULLS) OVER (PARTITION BY customerid ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
      ) AS city,
      COALESCE(
        stateprov,
        LAST_VALUE(stateprov IGNORE NULLS) OVER (PARTITION BY customerid ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
      ) AS stateprov,
      COALESCE(
        country,
        LAST_VALUE(country IGNORE NULLS) OVER (PARTITION BY customerid ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
      ) AS country,
      COALESCE(
        phone1,
        LAST_VALUE(phone1 IGNORE NULLS) OVER (PARTITION BY customerid ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
      ) AS phone1,
      COALESCE(
        phone2,
        LAST_VALUE(phone2 IGNORE NULLS) OVER (PARTITION BY customerid ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
      ) AS phone2,
      COALESCE(
        phone3,
        LAST_VALUE(phone3 IGNORE NULLS) OVER (PARTITION BY customerid ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
      ) AS phone3,
      COALESCE(
        email1,
        LAST_VALUE(email1 IGNORE NULLS) OVER (PARTITION BY customerid ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
      ) AS email1,
      COALESCE(
        email2,
        LAST_VALUE(email2 IGNORE NULLS) OVER (PARTITION BY customerid ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
      ) AS email2,
      COALESCE(
        lcl_tx_id,
        LAST_VALUE(lcl_tx_id IGNORE NULLS) OVER (PARTITION BY customerid ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
      ) AS lcl_tx_id,
      COALESCE(
        nat_tx_id,
        LAST_VALUE(nat_tx_id IGNORE NULLS) OVER (PARTITION BY customerid ORDER BY update_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
      ) AS nat_tx_id,
      LEAD(update_ts) OVER (PARTITION BY customerid ORDER BY update_ts) IS NULL AS iscurrent,
      DATE(update_ts) AS effectivedate,
      COALESCE(
        LEAD(DATE(update_ts)) OVER (PARTITION BY customerid ORDER BY update_ts),
        DATE('9999-12-31')
      ) AS enddate,
      batchid
    FROM Customers
)
SELECT
  CONCAT(TO_VARCHAR(c.effectivedate, 'YYYYMMDD'), c.customerid) AS sk_customerid,
  c.customerid,
  c.taxid,
  c.status,
  c.lastname,
  c.firstname,
  c.middleinitial,
  IFF(c.gender IN ('M', 'F'), c.gender, 'U') AS gender,
  c.tier,
  c.dob,
  c.addressline1,
  c.addressline2,
  c.postalcode,
  c.city,
  c.stateprov,
  c.country,
  c.phone1,
  c.phone2,
  c.phone3,
  c.email1,
  c.email2,
  r_nat.tx_name AS nationaltaxratedesc,
  r_nat.tx_rate AS nationaltaxrate,
  r_lcl.tx_name AS localtaxratedesc,
  r_lcl.tx_rate AS localtaxrate,
  p.agencyid,
  p.creditrating,
  p.networth,
  p.marketingnameplate,
  c.iscurrent,
  c.batchid,
  c.effectivedate,
  c.enddate
FROM CustomerFinal c
JOIN {{ ref('TaxRate') }} r_lcl
  ON c.lcl_tx_id = r_lcl.tx_id
JOIN {{ ref('TaxRate') }} r_nat
  ON c.nat_tx_id = r_nat.tx_id
LEFT JOIN {{ ref('ProspectIncremental') }} p
  ON UPPER(p.lastname) = UPPER(c.lastname)
  AND UPPER(p.firstname) = UPPER(c.firstname)
  AND UPPER(p.addressline1) = UPPER(c.addressline1)
  AND UPPER(NVL(p.addressline2, '')) = UPPER(NVL(c.addressline2, ''))
  AND UPPER(p.postalcode) = UPPER(c.postalcode)
WHERE c.effectivedate < c.enddate