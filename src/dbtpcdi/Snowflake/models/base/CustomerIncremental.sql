
{{
    config(
        materialized = 'view'
    )
}}

WITH customerRaw AS (
    SELECT
        * EXCLUDE(Value) ,
        2 AS batchid
    FROM
       {{source('tpcdi', 'customerincremental_batch_2') }}

    UNION ALL

    SELECT
        *  EXCLUDE(Value),
        3 AS batchid
    FROM
         {{source('tpcdi', 'customerincremental_batch_3') }}
)

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
    batchid
FROM
    customerRaw