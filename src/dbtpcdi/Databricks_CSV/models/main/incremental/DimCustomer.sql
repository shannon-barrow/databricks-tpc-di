{{
    config(
        materialized = 'table'
    )
}}
WITH customerincremental as (
    SELECT
      customerid,
        nullif(taxid, '') taxid,
        decode(status,
          'ACTV', 'Active',
          'CMPT', 'Completed',
          'CNCL', 'Canceled',
          'PNDG', 'Pending',
          'SBMT', 'Submitted',
          'INAC', 'Inactive') status,
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
        int(substring(_metadata.file_path FROM (position('/Batch', _metadata.file_path) + 6) FOR 1)) batchid
    FROM read_files(
      '{{var("rawfilelocation")}}/sf={{var("scalefactor")}}/Batch{2,3}',
      format          => "csv",
      header          => "false",
      inferSchema     => false,
      sep             => "|",
      schemaEvolutionMode => 'none',
      fileNamePattern => "Customer\\.txt",
      schema          => """
        cdc_flag     STRING,
        cdc_dsn      BIGINT,
        customerid   BIGINT,
        taxid        STRING,
        status       STRING,
        lastname     STRING,
        firstname    STRING,
        middleinitial STRING,
        gender       STRING,
        tier         TINYINT,
        dob          DATE,
        addressline1 STRING,
        addressline2 STRING,
        postalcode   STRING,
        city         STRING,
        stateprov    STRING,
        country      STRING,
        c_ctry_1     STRING,
        c_area_1     STRING,
        c_local_1    STRING,
        c_ext_1      STRING,
        c_ctry_2     STRING,
        c_area_2     STRING,
        c_local_2    STRING,
        c_ext_2      STRING,
        c_ctry_3     STRING,
        c_area_3     STRING,
        c_local_3    STRING,
        c_ext_3      STRING,
        email1       STRING,
        email2       STRING,
        lcl_tx_id    STRING,
        nat_tx_id    STRING
      """
    )
),
Customers as (
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
    {{ ref('CustomerMgmt') }}  c
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
  FROM customerincremental AS c
  JOIN {{ref("BatchDate")}}   bd
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
JOIN {{ref("TaxRate")}} r_lcl 
  ON c.lcl_tx_id = r_lcl.TX_ID
JOIN {{ref("TaxRate")}}  r_nat 
  ON c.nat_tx_id = r_nat.TX_ID
LEFT JOIN  {{ref("ProspectIncremental")}} p 
  on 
    upper(p.lastname) = upper(c.lastname)
    and upper(p.firstname) = upper(c.firstname)
    and upper(p.addressline1) = upper(c.addressline1)
    and upper(nvl(p.addressline2, '')) = upper(nvl(c.addressline2, ''))
    and upper(p.postalcode) = upper(c.postalcode)
WHERE c.effectivedate < c.enddate;