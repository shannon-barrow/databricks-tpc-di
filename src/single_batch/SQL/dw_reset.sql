-- Databricks notebook source
CREATE CATALOG IF NOT EXISTS ${catalog};
GRANT ALL PRIVILEGES ON CATALOG ${catalog} TO `account users`;
DROP DATABASE IF EXISTS ${catalog}.${wh_db}_${scale_factor} cascade;
CREATE DATABASE ${catalog}.${wh_db}_${scale_factor};
CREATE DATABASE IF NOT EXISTS ${catalog}.${wh_db}_${scale_factor}_stage;
DROP TABLE IF EXISTS ${catalog}.${wh_db}_${scale_factor}_stage.ProspectIncremental;
DROP TABLE IF EXISTS ${wh_db}_${scale_factor}_stage.finwire;
-- Enable Predictive Optimization for those workspaces that it is available
ALTER DATABASE ${catalog}.${wh_db}_${scale_factor}_stage ${pred_opt} PREDICTIVE OPTIMIZATION;
ALTER DATABASE ${catalog}.${wh_db}_${scale_factor} ${pred_opt} PREDICTIVE OPTIMIZATION;

-- COMMAND ----------

CREATE OR REPLACE VIEW ${catalog}.${wh_db}_${scale_factor}_stage.v_CustomerIncremental AS
with c as (
  SELECT
    try_cast(val[2] as BIGINT) customerid,
    val[3] taxid,
    val[4] status,
    val[5] lastname,
    val[6] firstname,
    val[7] middleinitial,
    val[8] gender,
    try_cast(val[9] as TINYINT) tier,
    try_cast(val[10] as DATE) dob,
    val[11] addressline1,
    val[12] addressline2,
    val[13] postalcode,
    val[14] city,
    val[15] stateprov,
    val[16] country,
    val[17] c_ctry_1,
    val[18] c_area_1,
    val[19] c_local_1,
    val[20] c_ext_1,
    val[21] c_ctry_2,
    val[22] c_area_2,
    val[23] c_local_2,
    val[24] c_ext_2,
    val[25] c_ctry_3,
    val[26] c_area_3,
    val[27] c_local_3,
    val[28] c_ext_3,
    val[29] email1,
    val[30] email2,
    val[31] lcl_tx_id,
    val[32] nat_tx_id,
    INT(batchid) batchid
  FROM
    (
      SELECT
        split(value, "[|]") val,
        substring(_metadata.file_path FROM (position('/Batch', _metadata.file_path) + 6) FOR 1) batchid 
      FROM
        text.`${tpcdi_directory}sf=${scale_factor}/Batch*/Customer.txt`
    )
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
FROM c
