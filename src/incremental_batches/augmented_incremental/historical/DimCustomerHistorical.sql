-- Databricks notebook source
-- CREATE WIDGET DROPDOWN scale_factor DEFAULT "10" CHOICES SELECT * FROM (VALUES ("10"), ("100"), ("1000"), ("5000"), ("10000"), ("20000"));
-- CREATE WIDGET TEXT tpcdi_directory DEFAULT "/Volumes/tpcdi/tpcdi_raw_data/tpcdi_volume/";
-- CREATE WIDGET TEXT wh_db DEFAULT "";
-- CREATE WIDGET TEXT catalog DEFAULT 'tpcdi';

-- COMMAND ----------

CREATE OR REPLACE TABLE IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimCustomer') (
  sk_customerid BIGINT NOT NULL COMMENT 'Surrogate key for CustomerID',
  customerid BIGINT COMMENT 'Customer identifier',
  taxid STRING COMMENT 'Customer’s tax identifier',
  status STRING COMMENT 'Customer status type',
  lastname STRING COMMENT 'Customers last name.',
  firstname STRING COMMENT 'Customers first name.',
  middleinitial STRING COMMENT 'Customers middle name initial',
  gender STRING COMMENT 'Gender of the customer',
  tier TINYINT COMMENT 'Customer tier',
  dob DATE COMMENT 'Customer’s date of birth.',
  addressline1 STRING COMMENT 'Address Line 1',
  addressline2 STRING COMMENT 'Address Line 2',
  postalcode STRING COMMENT 'Zip or Postal Code',
  city STRING COMMENT 'City',
  stateprov STRING COMMENT 'State or Province',
  country STRING COMMENT 'Country',
  phone1 STRING COMMENT 'Phone number 1',
  phone2 STRING COMMENT 'Phone number 2',
  phone3 STRING COMMENT 'Phone number 3',
  email1 STRING COMMENT 'Email address 1',
  email2 STRING COMMENT 'Email address 2',
  nationaltaxratedesc STRING COMMENT 'National Tax rate description',
  nationaltaxrate FLOAT COMMENT 'National Tax rate',
  localtaxratedesc STRING COMMENT 'Local Tax rate description',
  localtaxrate FLOAT COMMENT 'Local Tax rate',
  effectivedate DATE COMMENT 'Beginning of date range when this record was the current record',
  enddate DATE COMMENT 'Ending of date range when this record was the current record. A record that is not expired will use the date 9999-12-31.',
  iscurrent BOOLEAN COMMENT 'True if this is the current record',
  CONSTRAINT dimcustomer_pk PRIMARY KEY(sk_customerid)
) 
PARTITIONED BY (iscurrent)
TBLPROPERTIES (
  'delta.autoOptimize.autoCompact' = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true'
);

-- COMMAND ----------

INSERT OVERWRITE IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimCustomer')
with cust as (
  SELECT
    customerid,
    taxid,
    decode(status, 
      'ACTV',	'Active',
      'CMPT','Completed',
      'CNCL','Canceled',
      'PNDG','Pending',
      'SBMT','Submitted',
      'INAC','Inactive') status,
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
    nvl2(
      c_local_1,
      concat(
        nvl2(c_ctry_1, '+' || c_ctry_1 || ' ', ''),
        nvl2(c_area_1, '(' || c_area_1 || ') ', ''),
        c_local_1,
        nvl(c_ext_1, '')),
      c_local_1) phone1,
    nvl2(
      c_local_2,
      concat(
        nvl2(c_ctry_2, '+' || c_ctry_2 || ' ', ''),
        nvl2(c_area_2, '(' || c_area_2 || ') ', ''),
        c_local_2,
        nvl(c_ext_2, '')),
      c_local_2) phone2,
    nvl2(
      c_local_3,
      concat(
        nvl2(c_ctry_3, '+' || c_ctry_3 || ' ', ''),
        nvl2(c_area_3, '(' || c_area_3 || ') ', ''),
        c_local_3,
        nvl(c_ext_3, '')),
      c_local_3) phone3,
    email1,
    email2,
    lcl_tx_id,
    nat_tx_id,
    update_dt
  FROM IDENTIFIER(:catalog || '.tpcdi_raw_data.rawcustomer' || :scale_factor)
  WHERE update_dt < '2015-07-06' 
),
cust_updates as (
  SELECT
    customerid,
    coalesce(
      taxid,
      last_value(taxid) IGNORE NULLS OVER (
        PARTITION BY customerid
        ORDER BY update_dt
      )
    ) taxid,
    status,
    coalesce(
      lastname,
      last_value(lastname) IGNORE NULLS OVER (
        PARTITION BY customerid
        ORDER BY update_dt
      )
    ) lastname,
    coalesce(
      firstname,
      last_value(firstname) IGNORE NULLS OVER (
        PARTITION BY customerid
        ORDER BY update_dt
      )
    ) firstname,
    coalesce(
      middleinitial,
      last_value(middleinitial) IGNORE NULLS OVER (
        PARTITION BY customerid
        ORDER BY update_dt
      )
    ) middleinitial,
    coalesce(
      gender,
      last_value(gender) IGNORE NULLS OVER (
        PARTITION BY customerid
        ORDER BY update_dt
      )
    ) gender,
    coalesce(
      tier,
      last_value(tier) IGNORE NULLS OVER (
        PARTITION BY customerid
        ORDER BY update_dt
      )
    ) tier,
    coalesce(
      dob,
      last_value(dob) IGNORE NULLS OVER (
        PARTITION BY customerid
        ORDER BY update_dt
      )
    ) dob,
    coalesce(
      addressline1,
      last_value(addressline1) IGNORE NULLS OVER (
        PARTITION BY customerid
        ORDER BY update_dt
      )
    ) addressline1,
    coalesce(
      addressline2,
      last_value(addressline2) IGNORE NULLS OVER (
        PARTITION BY customerid
        ORDER BY update_dt
      )
    ) addressline2,
    coalesce(
      postalcode,
      last_value(postalcode) IGNORE NULLS OVER (
        PARTITION BY customerid
        ORDER BY update_dt
      )
    ) postalcode,
    coalesce(
      CITY,
      last_value(CITY) IGNORE NULLS OVER (
        PARTITION BY customerid
        ORDER BY update_dt
      )
    ) CITY,
    coalesce(
      stateprov,
      last_value(stateprov) IGNORE NULLS OVER (
        PARTITION BY customerid
        ORDER BY update_dt
      )
    ) stateprov,
    coalesce(
      country,
      last_value(country) IGNORE NULLS OVER (
        PARTITION BY customerid
        ORDER BY update_dt
      )
    ) country,
    coalesce(
      phone1,
      last_value(phone1) IGNORE NULLS OVER (
        PARTITION BY customerid
        ORDER BY update_dt
      )
    ) phone1,
    coalesce(
      phone2,
      last_value(phone2) IGNORE NULLS OVER (
        PARTITION BY customerid
        ORDER BY update_dt
      )
    ) phone2,
    coalesce(
      phone3,
      last_value(phone3) IGNORE NULLS OVER (
        PARTITION BY customerid
        ORDER BY update_dt
      )
    ) phone3,
    coalesce(
      email1,
      last_value(email1) IGNORE NULLS OVER (
        PARTITION BY customerid
        ORDER BY update_dt
      )
    ) email1,
    coalesce(
      email2,
      last_value(email2) IGNORE NULLS OVER (
        PARTITION BY customerid
        ORDER BY update_dt
      )
    ) email2,
    coalesce(
      lcl_tx_id,
      last_value(lcl_tx_id) IGNORE NULLS OVER (
        PARTITION BY customerid
        ORDER BY update_dt
      )
    ) lcl_tx_id,
    coalesce(
      nat_tx_id,
      last_value(nat_tx_id) IGNORE NULLS OVER (
        PARTITION BY customerid
        ORDER BY update_dt
      )
    ) nat_tx_id,
    update_dt effectivedate,
    coalesce(
      lead(update_dt) OVER (
        PARTITION BY customerid
        ORDER BY update_dt
      ),
      date('9999-12-31')
    ) enddate
  FROM cust
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
  c.effectivedate,
  c.enddate,
  if(enddate = date('9999-12-31'), true, false) iscurrent
FROM cust_updates c
JOIN IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.TaxRate') r_lcl 
  ON c.lcl_tx_id = r_lcl.TX_ID
JOIN IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.TaxRate') r_nat 
  ON c.nat_tx_id = r_nat.TX_ID