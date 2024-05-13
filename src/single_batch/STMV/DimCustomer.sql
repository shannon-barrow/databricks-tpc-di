-- Databricks notebook source
CREATE MATERIALIZED VIEW IF NOT EXISTS ${catalog}.${wh_db}_${scale_factor}.DimCustomer AS
WITH Customers as (
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
    update_ts
  FROM
    ${catalog}.${wh_db}_${scale_factor}_stage.CustomerMgmt c
  WHERE
    ActionType in ('NEW', 'INACT', 'UPDCUST')
  UNION ALL
  SELECT
    c.customerid,
    c.taxid,
    c.status,
    c.lastname,
    c.firstname,
    c.middleinitial,
    c.gender,
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
    c.lcl_tx_id,
    c.nat_tx_id,
    c.batchid,
    timestamp(bd.batchdate) update_ts
  FROM
    ${catalog}.${wh_db}_${scale_factor}_stage.v_CustomerIncremental c
  JOIN ${catalog}.${wh_db}_${scale_factor}.BatchDate bd 
    ON c.batchid = bd.batchid
),
CustomerFinal AS (
  SELECT
    customerid,
    coalesce(
      taxid,
      last_value(taxid) IGNORE NULLS OVER (
        PARTITION BY customerid
        ORDER BY update_ts
      )
    ) taxid,
    status,
    coalesce(
      lastname,
      last_value(lastname) IGNORE NULLS OVER (
        PARTITION BY customerid
        ORDER BY update_ts
      )
    ) lastname,
    coalesce(
      firstname,
      last_value(firstname) IGNORE NULLS OVER (
        PARTITION BY customerid
        ORDER BY update_ts
      )
    ) firstname,
    coalesce(
      middleinitial,
      last_value(middleinitial) IGNORE NULLS OVER (
        PARTITION BY customerid
        ORDER BY update_ts
      )
    ) middleinitial,
    coalesce(
      gender,
      last_value(gender) IGNORE NULLS OVER (
        PARTITION BY customerid
        ORDER BY update_ts
      )
    ) gender,
    coalesce(
      tier,
      last_value(tier) IGNORE NULLS OVER (
        PARTITION BY customerid
        ORDER BY update_ts
      )
    ) tier,
    coalesce(
      dob,
      last_value(dob) IGNORE NULLS OVER (
        PARTITION BY customerid
        ORDER BY update_ts
      )
    ) dob,
    coalesce(
      addressline1,
      last_value(addressline1) IGNORE NULLS OVER (
        PARTITION BY customerid
        ORDER BY update_ts
      )
    ) addressline1,
    coalesce(
      addressline2,
      last_value(addressline2) IGNORE NULLS OVER (
        PARTITION BY customerid
        ORDER BY update_ts
      )
    ) addressline2,
    coalesce(
      postalcode,
      last_value(postalcode) IGNORE NULLS OVER (
        PARTITION BY customerid
        ORDER BY update_ts
      )
    ) postalcode,
    coalesce(
      CITY,
      last_value(CITY) IGNORE NULLS OVER (
        PARTITION BY customerid
        ORDER BY update_ts
      )
    ) CITY,
    coalesce(
      stateprov,
      last_value(stateprov) IGNORE NULLS OVER (
        PARTITION BY customerid
        ORDER BY update_ts
      )
    ) stateprov,
    coalesce(
      country,
      last_value(country) IGNORE NULLS OVER (
        PARTITION BY customerid
        ORDER BY update_ts
      )
    ) country,
    coalesce(
      phone1,
      last_value(phone1) IGNORE NULLS OVER (
        PARTITION BY customerid
        ORDER BY update_ts
      )
    ) phone1,
    coalesce(
      phone2,
      last_value(phone2) IGNORE NULLS OVER (
        PARTITION BY customerid
        ORDER BY update_ts
      )
    ) phone2,
    coalesce(
      phone3,
      last_value(phone3) IGNORE NULLS OVER (
        PARTITION BY customerid
        ORDER BY update_ts
      )
    ) phone3,
    coalesce(
      email1,
      last_value(email1) IGNORE NULLS OVER (
        PARTITION BY customerid
        ORDER BY update_ts
      )
    ) email1,
    coalesce(
      email2,
      last_value(email2) IGNORE NULLS OVER (
        PARTITION BY customerid
        ORDER BY update_ts
      )
    ) email2,
    coalesce(
      LCL_TX_ID,
      last_value(LCL_TX_ID) IGNORE NULLS OVER (
        PARTITION BY customerid
        ORDER BY update_ts
      )
    ) LCL_TX_ID,
    coalesce(
      NAT_TX_ID,
      last_value(NAT_TX_ID) IGNORE NULLS OVER (
        PARTITION BY customerid
        ORDER BY update_ts
      )
    ) NAT_TX_ID,
    batchid,
    nvl2(
      lead(update_ts) OVER (
        PARTITION BY customerid
        ORDER BY update_ts
      ),
      false,
      true
    ) iscurrent,
    date(update_ts) effectivedate,
    coalesce(
      lead(date(update_ts)) OVER (
        PARTITION BY customerid
        ORDER BY update_ts
      ),
      date('9999-12-31')
    ) enddate
  FROM
    Customers
)
SELECT 
  bigint(concat(date_format(c.effectivedate, 'yyyyMMdd'), customerid)) sk_customerid,
  c.customerid,
  c.taxid,
  c.status,
  c.lastname,
  c.firstname,
  c.middleinitial,
  if(c.gender IN ('M', 'F'), c.gender, 'U') gender,
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
  r_nat.TX_NAME as nationaltaxratedesc,
  r_nat.TX_RATE as nationaltaxrate,
  r_lcl.TX_NAME as localtaxratedesc,
  r_lcl.TX_RATE as localtaxrate,
  p.agencyid,
  p.creditrating,
  p.networth,
  p.marketingnameplate,
  c.iscurrent,
  c.batchid,
  c.effectivedate,
  c.enddate
FROM CustomerFinal c
JOIN ${catalog}.${wh_db}_${scale_factor}.TaxRate r_lcl 
  ON c.lcl_tx_id = r_lcl.TX_ID
JOIN ${catalog}.${wh_db}_${scale_factor}.TaxRate r_nat 
  ON c.nat_tx_id = r_nat.TX_ID
LEFT JOIN ${catalog}.${wh_db}_${scale_factor}_stage.ProspectIncremental p 
  on 
    upper(p.lastname) = upper(c.lastname)
    and upper(p.firstname) = upper(c.firstname)
    and upper(p.addressline1) = upper(c.addressline1)
    and upper(nvl(p.addressline2, '')) = upper(nvl(c.addressline2, ''))
    and upper(p.postalcode) = upper(c.postalcode)
WHERE c.effectivedate < c.enddate
