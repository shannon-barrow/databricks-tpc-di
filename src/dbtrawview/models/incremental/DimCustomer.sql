{{
    config(
        materialized = 'table'
    )
}}

SELECT 
  c.sk_customerid,
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
FROM {{ ref('DimCustomerStg') }} c
LEFT JOIN {{ source('tpcdi', 'TaxRate') }} r_lcl 
  ON c.LCL_TX_ID = r_lcl.TX_ID
LEFT JOIN {{ source('tpcdi', 'TaxRate') }} r_nat 
  ON c.NAT_TX_ID = r_nat.TX_ID
LEFT JOIN {{ ref('Prospect') }} p 
  on upper(p.lastname) = upper(c.lastname)
  and upper(p.firstname) = upper(c.firstname)
  and upper(p.addressline1) = upper(c.addressline1)
  and upper(nvl(p.addressline2, '')) = upper(nvl(c.addressline2, ''))
  and upper(p.postalcode) = upper(c.postalcode);
