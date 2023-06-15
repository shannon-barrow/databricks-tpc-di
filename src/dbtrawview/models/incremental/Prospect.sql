{{
    config(
        materialized = 'table'
    )
}}
SELECT 
  agencyid,
  recdate.sk_dateid sk_recorddateid,
  origdate.sk_dateid sk_updatedateid,
  p.batchid,
  nvl2(c.customerid, True, False) iscustomer, 
  p.lastname,
  p.firstname,
  p.middleinitial,
  p.gender,
  p.addressline1,
  p.addressline2,
  p.postalcode,
  city,
  state,
  country,
  phone,
  income,
  numbercars,
  numberchildren,
  maritalstatus,
  age,
  creditrating,
  ownorrentflag,
  employer,
  numbercreditcards,
  networth,
  if(
    isnotnull(
      if(networth > 1000000 or income > 200000,"HighValue+","") || 
      if(numberchildren > 3 or numbercreditcards > 5,"Expenses+","") ||
      if(age > 45, "Boomer+", "") ||
      if(income < 50000 or creditrating < 600 or networth < 100000, "MoneyAlert+","") ||
      if(numbercars > 3 or numbercreditcards > 7, "Spender+","") ||
      if(age < 25 and networth > 1000000, "Inherited+","")),
    left(
      if(networth > 1000000 or income > 200000,"HighValue+","") || 
      if(numberchildren > 3 or numbercreditcards > 5,"Expenses+","") ||
      if(age > 45, "Boomer+", "") ||
      if(income < 50000 or creditrating < 600 or networth < 100000, "MoneyAlert+","") ||
      if(numbercars > 3 or numbercreditcards > 7, "Spender+","") ||
      if(age < 25 and networth > 1000000, "Inherited+",""),
      length(
        if(networth > 1000000 or income > 200000,"HighValue+","") || 
        if(numberchildren > 3 or numbercreditcards > 5,"Expenses+","") ||
        if(age > 45, "Boomer+", "") ||
        if(income < 50000 or creditrating < 600 or networth < 100000, "MoneyAlert+","") ||
        if(numbercars > 3 or numbercreditcards > 7, "Spender+","") ||
        if(age < 25 and networth > 1000000, "Inherited+",""))
      -1),
    NULL) marketingnameplate
FROM (
  SELECT 
    * FROM (
    SELECT
      agencyid,
      max(batchid) recordbatchid,
      lastname,
      firstname,
      middleinitial,
      gender,
      addressline1,
      addressline2,
      postalcode,
      city,
      state,
      country,
      phone,
      income,
      numbercars,
      numberchildren,
      maritalstatus,
      age,
      creditrating,
      ownorrentflag,
      employer,
      numbercreditcards,
      networth,
      min(batchid) batchid
    FROM {{ ref('ProspectRaw') }} p
    GROUP BY
      agencyid,
      lastname,
      firstname,
      middleinitial,
      gender,
      addressline1,
      addressline2,
      postalcode,
      city,
      state,
      country,
      phone,
      income,
      numbercars,
      numberchildren,
      maritalstatus,
      age,
      creditrating,
      ownorrentflag,
      employer,
      numbercreditcards,
      networth)
  QUALIFY ROW_NUMBER() OVER (PARTITION BY agencyid ORDER BY batchid DESC) = 1) p
JOIN (
  SELECT 
    sk_dateid,
    batchid
  FROM {{ ref('BatchDate') }} b 
  JOIN {{ source('tpcdi', 'DimDate') }} d 
    ON b.batchdate = d.datevalue) recdate
  ON p.recordbatchid = recdate.batchid
JOIN (
  SELECT 
    sk_dateid,
    batchid
  FROM {{ ref('BatchDate') }} b 
  JOIN {{ source('tpcdi', 'DimDate') }} d 
    ON b.batchdate = d.datevalue) origdate
  ON p.batchid = origdate.batchid
LEFT JOIN (
  SELECT 
    customerid,
    lastname,
    firstname,
    addressline1,
    addressline2,
    postalcode
  FROM {{ ref('DimCustomerStg') }}
  WHERE iscurrent) c
  ON 
    upper(p.LastName) = upper(c.lastname)
    and upper(p.FirstName) = upper(c.firstname)
    and upper(p.AddressLine1) = upper(c.addressline1)
    and upper(nvl(p.addressline2, '')) = upper(nvl(c.addressline2, ''))
    and upper(p.PostalCode) = upper(c.postalcode)
