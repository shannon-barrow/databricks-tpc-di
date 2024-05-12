-- Databricks notebook source
INSERT OVERWRITE ${catalog}.${wh_db}_${scale_factor}.DimCustomer
WITH CustomerHistory AS (
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
      lcl_tx_id,
      last_value(lcl_tx_id) IGNORE NULLS OVER (
        PARTITION BY customerid
        ORDER BY update_ts
      )
    ) lcl_tx_id,
    coalesce(
      nat_tx_id,
      last_value(nat_tx_id) IGNORE NULLS OVER (
        PARTITION BY customerid
        ORDER BY update_ts
      )
    ) nat_tx_id,
    1 batchid,
    date(update_ts) effectivedate,
    coalesce(
      lead(date(update_ts)) OVER (
        PARTITION BY customerid
        ORDER BY update_ts
      ),
      date('9999-12-31')
    ) enddate
  FROM 
    ${catalog}.${wh_db}_${scale_factor}_stage.CustomerMgmt c
  WHERE
    ActionType in ('NEW', 'INACT', 'UPDCUST')
)
SELECT 
  bigint(concat(date_format(c.effectivedate, 'yyyyMMdd'), customerid)) sk_customerid,
  c.customerid,
  c.taxid,
  c.status,
  c.lastname,
  c.firstname,
  c.middleinitial,
  if(upper(c.gender) IN ('M', 'F'), upper(c.gender), 'U') gender,
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
  r_nat.tx_name as nationaltaxratedesc,
  r_nat.tx_rate as nationaltaxrate,
  r_lcl.tx_name as localtaxratedesc,
  r_lcl.tx_rate as localtaxrate,
  p.agencyid,
  p.creditrating,
  p.networth,
  p.marketingnameplate,
  if(enddate = date('9999-12-31'), true, false) iscurrent,
  c.batchid,
  c.effectivedate,
  c.enddate
FROM CustomerHistory c
JOIN ${catalog}.${wh_db}_${scale_factor}.TaxRate r_lcl 
  ON c.lcl_tx_id = r_lcl.TX_ID
JOIN ${catalog}.${wh_db}_${scale_factor}.TaxRate r_nat 
  ON c.nat_tx_id = r_nat.TX_ID
LEFT JOIN ${catalog}.${wh_db}_${scale_factor}_stage.ProspectIncremental p 
  ON
    p.batchid = 1
    and upper(p.lastname) = upper(c.lastname)
    and upper(p.firstname) = upper(c.firstname)
    and upper(p.addressline1) = upper(c.addressline1)
    and upper(nvl(p.addressline2, '')) = upper(nvl(c.addressline2, ''))
    and upper(p.postalcode) = upper(c.postalcode)
WHERE c.effectivedate < c.enddate
