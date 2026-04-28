-- Databricks notebook source
CREATE WIDGET DROPDOWN scale_factor DEFAULT "10" CHOICES SELECT * FROM (VALUES ("10"), ("100"), ("1000"), ("5000"), ("10000"), ("20000"));
CREATE WIDGET TEXT tpcdi_directory DEFAULT "/Volumes/tpcdi/tpcdi_raw_data/tpcdi_volume/";
CREATE WIDGET TEXT target_dir DEFAULT 'split_data';
CREATE WIDGET TEXT catalog DEFAULT 'tpcdi';

-- COMMAND ----------

CREATE OR REPLACE TABLE ${catalog}.tpcdi_raw_data.rawcustomer${scale_factor}
PARTITIONED BY (update_dt)
TBLPROPERTIES (
  'delta.autoOptimize.autoCompact'=True, 
  'delta.autoOptimize.optimizeWrite'=True
)
with hist as (
  select 
    decode(_ActionType, 
      "NEW","I",
      "ADDACCT","I",
      "UPDACCT","U",
      "UPDCUST","U",
      "CLOSEACCT","U",
      "INACT","U") cdc_flag,
    row_number() over (order by _ActionTS) - 1 cdc_dsn,
    nullif(Customer._C_ID, '') customerid,
    nullif(Customer._C_TAX_ID, '') taxid,
    decode(_ActionType, 
      "NEW","ACTV",
      "ADDACCT","ACTV",
      "UPDACCT","ACTV",
      "UPDCUST","ACTV",
      "CLOSEACCT","INAC",
      "INACT","INAC") status,
    nullif(Customer.Name.C_L_NAME, '') lastname, 
    nullif(Customer.Name.C_F_NAME, '') firstname, 
    nullif(Customer.Name.C_M_NAME, '') middleinitial, 
    nullif(upper(Customer._C_GNDR), '') gender,
    nullif(Customer._C_TIER, '') tier, 
    nullif(Customer._C_DOB, '') dob,
    nullif(Customer.Address.C_ADLINE1, '') addressline1, 
    nullif(Customer.Address.C_ADLINE2, '') addressline2, 
    nullif(Customer.Address.C_ZIPCODE, '') postalcode,
    nullif(Customer.Address.C_CITY, '') city, 
    nullif(Customer.Address.C_STATE_PROV, '') stateprov,
    nullif(Customer.Address.C_CTRY, '') country, 
    nvl2(
      nullif(Customer.ContactInfo.C_PHONE_1.C_LOCAL, ''),
      concat(
          nvl2(nullif(Customer.ContactInfo.C_PHONE_1.C_CTRY_CODE, ''), '+' || Customer.ContactInfo.C_PHONE_1.C_CTRY_CODE || ' ', ''),
          nvl2(nullif(Customer.ContactInfo.C_PHONE_1.C_AREA_CODE, ''), '(' || Customer.ContactInfo.C_PHONE_1.C_AREA_CODE || ') ', ''),
          Customer.ContactInfo.C_PHONE_1.C_LOCAL,
          nvl(Customer.ContactInfo.C_PHONE_1.C_EXT, '')),
      try_cast(null as string)) phone1,
    nvl2(
      nullif(Customer.ContactInfo.C_PHONE_2.C_LOCAL, ''),
      concat(
          nvl2(nullif(Customer.ContactInfo.C_PHONE_2.C_CTRY_CODE, ''), '+' || Customer.ContactInfo.C_PHONE_2.C_CTRY_CODE || ' ', ''),
          nvl2(nullif(Customer.ContactInfo.C_PHONE_2.C_AREA_CODE, ''), '(' || Customer.ContactInfo.C_PHONE_2.C_AREA_CODE || ') ', ''),
          Customer.ContactInfo.C_PHONE_2.C_LOCAL,
          nvl(Customer.ContactInfo.C_PHONE_2.C_EXT, '')),
      try_cast(null as string)) phone2,
    nvl2(
      nullif(Customer.ContactInfo.C_PHONE_3.C_LOCAL, ''),
      concat(
          nvl2(nullif(Customer.ContactInfo.C_PHONE_3.C_CTRY_CODE, ''), '+' || Customer.ContactInfo.C_PHONE_3.C_CTRY_CODE || ' ', ''),
          nvl2(nullif(Customer.ContactInfo.C_PHONE_3.C_AREA_CODE, ''), '(' || Customer.ContactInfo.C_PHONE_3.C_AREA_CODE || ') ', ''),
          Customer.ContactInfo.C_PHONE_3.C_LOCAL,
          nvl(Customer.ContactInfo.C_PHONE_3.C_EXT, '')),
      try_cast(null as string)) phone3,
    nvl2(phone1,nullif(Customer.ContactInfo.C_PHONE_1.C_CTRY_CODE, ''),phone1) c_ctry_1,
    nvl2(phone1,nullif(Customer.ContactInfo.C_PHONE_1.C_AREA_CODE, ''),phone1) c_area_1,
    nvl2(phone1,nullif(Customer.ContactInfo.C_PHONE_1.C_LOCAL, ''),phone1) c_local_1,
    nvl2(phone1,nullif(Customer.ContactInfo.C_PHONE_1.C_EXT, ''),phone1) c_ext_1,
    nvl2(phone2,nullif(Customer.ContactInfo.C_PHONE_2.C_CTRY_CODE, ''),phone2) c_ctry_2,
    nvl2(phone2,nullif(Customer.ContactInfo.C_PHONE_2.C_AREA_CODE, ''),phone2) c_area_2,
    nvl2(phone2,nullif(Customer.ContactInfo.C_PHONE_2.C_LOCAL, ''),phone2) c_local_2,
    nvl2(phone2,nullif(Customer.ContactInfo.C_PHONE_2.C_EXT, ''),phone2) c_ext_2,
    nvl2(phone3,nullif(Customer.ContactInfo.C_PHONE_3.C_CTRY_CODE, ''),phone3) c_ctry_3,
    nvl2(phone3,nullif(Customer.ContactInfo.C_PHONE_3.C_AREA_CODE, ''),phone3) c_area_3,
    nvl2(phone3,nullif(Customer.ContactInfo.C_PHONE_3.C_LOCAL, ''),phone3) c_local_3,
    nvl2(phone3,nullif(Customer.ContactInfo.C_PHONE_3.C_EXT, ''),phone3) c_ext_3,
    nullif(Customer.ContactInfo.C_PRIM_EMAIL, '') email1,
    nullif(Customer.ContactInfo.C_ALT_EMAIL, '') email2,
    nullif(Customer.TaxInfo.C_LCL_TX_ID, '') lcl_tx_id, 
    nullif(Customer.TaxInfo.C_NAT_TX_ID, '') nat_tx_id, 
    _ActionTS update_ts
  FROM read_files(
    "${tpcdi_directory}sf=${scale_factor}/Batch1",
    format => "xml",
    inferSchema => False, 
    rowTag => "TPCDI:Action",
    fileNamePattern => "CustomerMgmt.xml"
  )
  WHERE
    _ActionType in ('NEW', 'INACT', 'UPDCUST')
),
changes as (
  SELECT
    cdc_flag,
    cdc_dsn,
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
      city,
      last_value(CITY) IGNORE NULLS OVER (
        PARTITION BY customerid
        ORDER BY update_ts
      )
    ) city,
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
      c_ctry_1,
      last_value(c_ctry_1) IGNORE NULLS OVER (
        PARTITION BY customerid
        ORDER BY update_ts
      )
    ) c_ctry_1,
    coalesce(
      c_area_1,
      last_value(c_area_1) IGNORE NULLS OVER (
        PARTITION BY customerid
        ORDER BY update_ts
      )
    ) c_area_1,
    coalesce(
      c_local_1,
      last_value(c_local_1) IGNORE NULLS OVER (
        PARTITION BY customerid
        ORDER BY update_ts
      )
    ) c_local_1,
    coalesce(
      c_ext_1,
      last_value(c_ext_1) IGNORE NULLS OVER (
        PARTITION BY customerid
        ORDER BY update_ts
      )
    ) c_ext_1,
    coalesce(
      c_ctry_2,
      last_value(c_ctry_2) IGNORE NULLS OVER (
        PARTITION BY customerid
        ORDER BY update_ts
      )
    ) c_ctry_2,
    coalesce(
      c_area_2,
      last_value(c_area_2) IGNORE NULLS OVER (
        PARTITION BY customerid
        ORDER BY update_ts
      )
    ) c_area_2,
    coalesce(
      c_local_2,
      last_value(c_local_2) IGNORE NULLS OVER (
        PARTITION BY customerid
        ORDER BY update_ts
      )
    ) c_local_2,
    coalesce(
      c_ext_2,
      last_value(c_ext_2) IGNORE NULLS OVER (
        PARTITION BY customerid
        ORDER BY update_ts
      )
    ) c_ext_2,
    coalesce(
      c_ctry_3,
      last_value(c_ctry_3) IGNORE NULLS OVER (
        PARTITION BY customerid
        ORDER BY update_ts
      )
    ) c_ctry_3,
    coalesce(
      c_area_3,
      last_value(c_area_3) IGNORE NULLS OVER (
        PARTITION BY customerid
        ORDER BY update_ts
      )
    ) c_area_3,
    coalesce(
      c_local_3,
      last_value(c_local_3) IGNORE NULLS OVER (
        PARTITION BY customerid
        ORDER BY update_ts
      )
    ) c_local_3,
    coalesce(
      c_ext_3,
      last_value(c_ext_3) IGNORE NULLS OVER (
        PARTITION BY customerid
        ORDER BY update_ts
      )
    ) c_ext_3,
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
    date(update_ts) update_dt,
    row_number() over (
      PARTITION BY 
        customerid, 
        date(update_ts)
      ORDER BY update_ts desc
    ) rn
  from hist
)
select
  * except (rn)
from changes
where rn = 1