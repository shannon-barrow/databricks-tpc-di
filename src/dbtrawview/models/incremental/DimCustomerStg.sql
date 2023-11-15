{{
    config(
        materialized = 'table'
    )
}}
SELECT * FROM (
  SELECT
    sk_customerid,
    customerid,
    coalesce(taxid, last_value(taxid) IGNORE NULLS OVER (
        PARTITION BY customerid
        ORDER BY update_ts)) taxid,
    status,
    coalesce(lastname, last_value(lastname) IGNORE NULLS OVER (
        PARTITION BY customerid
        ORDER BY update_ts)) lastname,
    coalesce(firstname, last_value(firstname) IGNORE NULLS OVER (
        PARTITION BY customerid
        ORDER BY update_ts)) firstname,
    coalesce(middleinitial, last_value(middleinitial) IGNORE NULLS OVER (
        PARTITION BY customerid
        ORDER BY update_ts)) middleinitial,
    coalesce(gender, last_value(gender) IGNORE NULLS OVER (
        PARTITION BY customerid
        ORDER BY update_ts)) gender,
    coalesce(tier, last_value(tier) IGNORE NULLS OVER (
        PARTITION BY customerid
        ORDER BY update_ts)) tier,
    coalesce(dob, last_value(dob) IGNORE NULLS OVER (
        PARTITION BY customerid
        ORDER BY update_ts)) dob,
    coalesce(addressline1, last_value(addressline1) IGNORE NULLS OVER (
        PARTITION BY customerid
        ORDER BY update_ts)) addressline1,
    coalesce(addressline2, last_value(addressline2) IGNORE NULLS OVER (
        PARTITION BY customerid
        ORDER BY update_ts)) addressline2,
    coalesce(postalcode, last_value(postalcode) IGNORE NULLS OVER (
        PARTITION BY customerid
        ORDER BY update_ts)) postalcode,
    coalesce(CITY, last_value(CITY) IGNORE NULLS OVER (
        PARTITION BY customerid
        ORDER BY update_ts)) CITY,
    coalesce(stateprov, last_value(stateprov) IGNORE NULLS OVER (
        PARTITION BY customerid
        ORDER BY update_ts)) stateprov,
    coalesce(country, last_value(country) IGNORE NULLS OVER (
        PARTITION BY customerid
        ORDER BY update_ts)) country,
    coalesce(phone1, last_value(phone1) IGNORE NULLS OVER (
        PARTITION BY customerid
        ORDER BY update_ts)) phone1,
    coalesce(phone2, last_value(phone2) IGNORE NULLS OVER (
        PARTITION BY customerid
        ORDER BY update_ts)) phone2,
    coalesce(phone3, last_value(phone3) IGNORE NULLS OVER (
        PARTITION BY customerid
        ORDER BY update_ts)) phone3,
    coalesce(email1, last_value(email1) IGNORE NULLS OVER (
        PARTITION BY customerid
        ORDER BY update_ts)) email1,
    coalesce(email2, last_value(email2) IGNORE NULLS OVER (
        PARTITION BY customerid
        ORDER BY update_ts)) email2,
    coalesce(LCL_TX_ID, last_value(LCL_TX_ID) IGNORE NULLS OVER (
        PARTITION BY customerid
        ORDER BY update_ts)) LCL_TX_ID,
    coalesce(NAT_TX_ID, last_value(NAT_TX_ID) IGNORE NULLS OVER (
        PARTITION BY customerid
        ORDER BY update_ts)) NAT_TX_ID,
    batchid,
    nvl2(lead(update_ts) OVER (PARTITION BY customerid ORDER BY update_ts), false, true) iscurrent,
    date(update_ts) effectivedate,
    coalesce(lead(date(update_ts)) OVER (PARTITION BY customerid ORDER BY update_ts), date('9999-12-31')) enddate
  FROM (
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
      1 batchid,
      update_ts,
      bigint(concat(date_format(update_ts, 'yyyyMMdd'), cast(customerid as string))) as sk_customerid
    FROM {{ ref('CustomerMgmtView') }} c
    WHERE ActionType in ('NEW', 'INACT', 'UPDCUST')
    UNION ALL
    SELECT

      c.customerid,
      nullif(c.taxid, '') taxid,
      nullif(s.st_name, '') as status,
      nullif(c.lastname, '') lastname,
      nullif(c.firstname, '') firstname,
      nullif(c.middleinitial, '') middleinitial,
      gender,
      c.tier,
      c.dob,
      nullif(c.addressline1, '') addressline1,
      nullif(c.addressline2, '') addressline2,
      nullif(c.postalcode, '') postalcode,
      nullif(c.city, '') city,
      nullif(c.stateprov, '') stateprov,
      nullif(c.country, '') country,
      CASE
        WHEN isnull(c_local_1) then c_local_1
        ELSE concat(
          nvl2(c_ctry_1, '+' || c_ctry_1 || ' ', ''),
          nvl2(c_area_1, '(' || c_area_1 || ') ', ''),
          c_local_1,
          nvl(c_ext_1, '')) END as phone1,
      CASE
        WHEN isnull(c_local_2) then c_local_2
        ELSE concat(
          nvl2(c_ctry_2, '+' || c_ctry_2 || ' ', ''),
          nvl2(c_area_2, '(' || c_area_2 || ') ', ''),
          c_local_2,
          nvl(c_ext_2, '')) END as phone2,
      CASE
        WHEN isnull(c_local_3) then c_local_3
        ELSE concat(
          nvl2(c_ctry_3, '+' || c_ctry_3 || ' ', ''),
          nvl2(c_area_3, '(' || c_area_3 || ') ', ''),
          c_local_3,
          nvl(c_ext_3, '')) END as phone3,
      nullif(c.email1, '') email1,
      nullif(c.email2, '') email2,
      c.LCL_TX_ID, 
      c.NAT_TX_ID,
      c.batchid,
      timestamp(bd.batchdate) update_ts,
      bigint(concat(date_format(update_ts, 'yyyyMMdd'), cast(customerid as string))) as sk_customerid
    FROM {{ ref('CustomerIncremental') }} c
    JOIN {{ ref('BatchDate') }} bd
      ON c.batchid = bd.batchid
    JOIN {{ source('tpcdi', 'StatusType') }} s 
      ON c.status = s.st_id
  ) c
  )
