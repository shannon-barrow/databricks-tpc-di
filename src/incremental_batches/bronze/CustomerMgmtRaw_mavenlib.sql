-- Databricks notebook source
-- CREATE WIDGET DROPDOWN scale_factor DEFAULT "10" CHOICES SELECT * FROM (VALUES ("10"), ("100"), ("1000"), ("5000"), ("10000"));
-- CREATE WIDGET TEXT tpcdi_directory DEFAULT "/Volumes/tpcdi/tpcdi_raw_data/tpcdi_volume/";
-- CREATE WIDGET TEXT wh_db DEFAULT '';
-- CREATE WIDGET TEXT catalog DEFAULT 'tpcdi';

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW v_cust_mgmt
USING ${xml_lib}
OPTIONS (path "${tpcdi_directory}sf=${scale_factor}/Batch1/CustomerMgmt.xml", rowTag "TPCDI:Action", inferSchema "false");

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS ${catalog}.${wh_db}_${scale_factor}_stage;
CREATE TABLE IF NOT EXISTS ${catalog}.${wh_db}_${scale_factor}_stage.CustomerMgmt PARTITIONED BY (ActionType) AS 
SELECT 
  try_cast(Customer._C_ID as BIGINT) customerid, 
  try_cast(Customer.Account._CA_ID as BIGINT) accountid,
  try_cast(Customer.Account.CA_B_ID as BIGINT) brokerid, 
  nullif(Customer._C_TAX_ID, '') taxid,
  nullif(Customer.Account.CA_NAME, '') accountdesc, 
  try_cast(Customer.Account._CA_TAX_ST as TINYINT) taxstatus,
  decode(_ActionType,
    "NEW","Active",
    "ADDACCT","Active",
    "UPDACCT","Active",
    "UPDCUST","Active",
    "CLOSEACCT","Inactive",
    "INACT","Inactive") status,
  nullif(Customer.Name.C_L_NAME, '') lastname, 
  nullif(Customer.Name.C_F_NAME, '') firstname, 
  nullif(Customer.Name.C_M_NAME, '') middleinitial, 
  nullif(Customer._C_GNDR, '') gender,
  try_cast(Customer._C_TIER as TINYINT) tier, 
  try_cast(Customer._C_DOB as DATE) dob,
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
  nullif(Customer.ContactInfo.C_PRIM_EMAIL, '') email1,
  nullif(Customer.ContactInfo.C_ALT_EMAIL, '') email2,
  nullif(Customer.TaxInfo.C_LCL_TX_ID, '') lcl_tx_id, 
  nullif(Customer.TaxInfo.C_NAT_TX_ID, '') nat_tx_id, 
  to_timestamp(_ActionTS) update_ts,
  _ActionType ActionType
FROM v_cust_mgmt
