{{
    config(
        materialized = 'view'
    )
}}


WITH customerRaw AS (
    SELECT
        *  ,
        2 AS batchid
    FROM
       {{source('tpcdi', 'customerincremental_batch_2') }}

    UNION ALL

    SELECT
        * ,
        3 AS batchid
    FROM
         {{source('tpcdi', 'customerincremental_batch_3') }}
)
SELECT
    customerid,
    nullif(taxid, '') taxid,
    decode(status, 
      'ACTV',	'Active',
      'CMPT','Completed',
      'CNCL','Canceled',
      'PNDG','Pending',
      'SBMT','Submitted',
      'INAC','Inactive') status,
    nullif(lastname, '') lastname,
    nullif(firstname, '') firstname,
    nullif(middleinitial, '') middleinitial,
    nullif(gender, '') gender,
    tier,
    dob,
    nullif(addressline1, '') addressline1,
    nullif(addressline2, '') addressline2,
    nullif(postalcode, '') postalcode,
    nullif(city, '') city,
    nullif(stateprov, '') stateprov,
    country,
    nvl2(
      nullif(c_local_1, ''),
      concat(
        nvl2(nullif(c_ctry_1, ''), '+' || c_ctry_1 || ' ', ''),
        nvl2(nullif(c_area_1, ''), '(' || c_area_1 || ') ', ''),
        c_local_1,
        nvl(c_ext_1, '')),
      try_cast(null as string)) phone1,
    nvl2(
      nullif(c_local_2, ''),
      concat(
        nvl2(nullif(c_ctry_2, ''), '+' || c_ctry_2 || ' ', ''),
        nvl2(nullif(c_area_2, ''), '(' || c_area_2 || ') ', ''),
        c_local_2,
        nvl(c_ext_2, '')),
      try_cast(null as string)) phone2,
    nvl2(
      nullif(c_local_3, ''),
      concat(
        nvl2(nullif(c_ctry_3, ''), '+' || c_ctry_3 || ' ', ''),
        nvl2(nullif(c_area_3, ''), '(' || c_area_3 || ') ', ''),
        c_local_3,
        nvl(c_ext_3, '')),
      try_cast(null as string)) phone3,
    nullif(email1, '') email1,
    nullif(email2, '') email2,
    nullif(lcl_tx_id, '') lcl_tx_id,
    nullif(nat_tx_id, '') nat_tx_id,
    batchid
  FROM  customerRaw