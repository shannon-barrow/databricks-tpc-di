-- Databricks notebook source
-- CREATE WIDGET DROPDOWN scale_factor DEFAULT "10" CHOICES SELECT * FROM (VALUES ("10"), ("100"), ("1000"), ("5000"), ("10000"));
-- CREATE WIDGET TEXT tpcdi_directory DEFAULT "/Volumes/tpcdi/tpcdi_raw_data/tpcdi_volume/";
-- CREATE WIDGET TEXT wh_db DEFAULT '';
-- CREATE WIDGET TEXT catalog DEFAULT 'tpcdi';
-- CREATE WIDGET DROPDOWN pred_opt DEFAULT "DISABLE" CHOICES SELECT * FROM (VALUES ("ENABLE"), ("DISABLE")); -- Predictive Optimization

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS ${catalog};
GRANT ALL PRIVILEGES ON CATALOG ${catalog} TO `account users`;
DROP DATABASE IF EXISTS ${catalog}.${wh_db}_${scale_factor} cascade;
CREATE DATABASE ${catalog}.${wh_db}_${scale_factor};
CREATE DATABASE IF NOT EXISTS ${catalog}.${wh_db}_${scale_factor}_stage;
DROP TABLE IF EXISTS ${catalog}.${wh_db}_${scale_factor}_stage.ProspectIncremental;
DROP TABLE IF EXISTS ${wh_db}_${scale_factor}_stage.finwire;

-- COMMAND ----------

CREATE OR REPLACE VIEW ${catalog}.${wh_db}_${scale_factor}_stage.v_TradeIncremental AS
SELECT
  val[0] cdc_flag,
  try_cast(val[2] as BIGINT) tradeid,
  try_cast(val[3] as TIMESTAMP) t_dts,
  val[4] status,
  val[5] t_tt_id,
  try_cast(val[6] as TINYINT) cashflag,
  val[7] t_s_symb,
  try_cast(val[8] as INT) quantity,
  try_cast(val[9] as DOUBLE) bidprice,
  try_cast(val[10] as BIGINT) t_ca_id,
  val[11] executedby,
  try_cast(val[12] as DOUBLE) tradeprice,
  try_cast(val[13] as DOUBLE) fee,
  try_cast(val[14] as DOUBLE) commission,
  try_cast(val[15] as DOUBLE) tax,
  INT(batchid) batchid
FROM (
  SELECT 
    split(value, "[|]") val, 
    substring(_metadata.file_path FROM (position('/Batch', _metadata.file_path) + 6) FOR 1) batchid 
  FROM text.`${tpcdi_directory}/sf=${scale_factor}/Batch[23]/Trade.txt`)

-- COMMAND ----------

CREATE OR REPLACE VIEW ${catalog}.${wh_db}_${scale_factor}_stage.v_Trade AS
SELECT
  try_cast(val[0] as BIGINT) t_id,
  try_cast(val[1] as TIMESTAMP) t_dts,
  val[2] t_st_id,
  val[3] t_tt_id,
  try_cast(val[4] as TINYINT) t_is_cash,
  val[5] t_s_symb,
  try_cast(val[6] as INT) quantity,
  try_cast(val[7] as DOUBLE) bidprice,
  try_cast(val[8] as BIGINT) t_ca_id,
  val[9] executedby,
  try_cast(val[10] as DOUBLE) tradeprice,
  try_cast(val[11] as DOUBLE) fee,
  try_cast(val[12] as DOUBLE) commission,
  try_cast(val[13] as DOUBLE) tax,
  1 batchid
FROM
  (
    SELECT
      split(value, "[|]") val
    FROM
      text.`${tpcdi_directory}sf=${scale_factor}/Batch1/Trade.txt`
  );

-- COMMAND ----------

CREATE OR REPLACE VIEW ${catalog}.${wh_db}_${scale_factor}_stage.v_TradeHistory AS
SELECT
  try_cast(val[0] as BIGINT) tradeid,
  try_cast(val[1] as TIMESTAMP) th_dts,
  val[2] status
FROM
  (
    SELECT
      split(value, "[|]") val
    FROM
      text.`${tpcdi_directory}sf=${scale_factor}/Batch1/TradeHistory.txt`
  );

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

-- COMMAND ----------

CREATE OR REPLACE VIEW ${catalog}.${wh_db}_${scale_factor}_stage.v_HR AS
SELECT
  try_cast(val[0] as BIGINT) employeeid,
  try_cast(val[1] as BIGINT) managerid,
  val[2] employeefirstname,
  val[3] employeelastname,
  val[4] employeemi,
  try_cast(val[5] as int) employeejobcode,
  val[6] employeebranch,
  val[7] employeeoffice,
  val[8] employeephone
FROM
  (
    SELECT
      split(value, "[,]") val
    FROM
      text.`${tpcdi_directory}sf=${scale_factor}/Batch1/HR.csv`
  );

-- COMMAND ----------

CREATE OR REPLACE VIEW ${catalog}.${wh_db}_${scale_factor}_stage.v_AccountIncremental AS
SELECT
  try_cast(val[2] as BIGINT) accountid,
  try_cast(val[3] as BIGINT) brokerid,
  try_cast(val[4] as BIGINT) customerid,
  val[5] accountDesc,
  try_cast(val[6] as TINYINT) taxstatus,
  val[7] status,
  INT(batchid) batchid
FROM
  (
    SELECT
      split(value, "[|]") val,
      substring(_metadata.file_path FROM (position('/Batch', _metadata.file_path) + 6) FOR 1) batchid 
    FROM
      text.`${tpcdi_directory}sf=${scale_factor}/Batch*/Account.txt`
  );

-- COMMAND ----------

CREATE OR REPLACE VIEW ${catalog}.${wh_db}_${scale_factor}_stage.v_CashTransactionIncremental AS
with CashTransactions as (
  SELECT
    try_cast(val[0] as BIGINT) accountid,
    to_date(val[1]) datevalue,
    sum(try_cast(val[2] as DOUBLE)) account_daily_total,
    1 batchid
  FROM (
    SELECT
      split(value, "[|]") val
    FROM
      text.`${tpcdi_directory}sf=${scale_factor}/Batch1/CashTransaction.txt`
  )
  GROUP BY
    accountid,
    datevalue,
    batchid
  UNION ALL
  SELECT
    try_cast(val[2] as BIGINT) accountid,
    to_date(val[3]) datevalue,
    sum(try_cast(val[4] as DOUBLE)) account_daily_total,
    INT(batchid) batchid
  FROM (
    SELECT
      split(value, "[|]") val,
      substring(_metadata.file_path FROM (position('/Batch', _metadata.file_path) + 6) FOR 1) batchid 
    FROM
      text.`${tpcdi_directory}sf=${scale_factor}/Batch[23]/CashTransaction.txt`
  )
  GROUP BY
    accountid,
    datevalue,
    batchid
)
SELECT 
  accountid,
  datevalue,
  sum(account_daily_total) OVER (partition by accountid order by datevalue) cash,
  batchid
FROM CashTransactions;  

-- COMMAND ----------

CREATE OR REPLACE VIEW ${catalog}.${wh_db}_${scale_factor}_stage.v_HoldingHistory AS
SELECT
  try_cast(val[0] as INT) hh_h_t_id,
  try_cast(val[1] as INT) hh_t_id,
  try_cast(val[2] as INT) hh_before_qty,
  try_cast(val[3] as INT) hh_after_qty,
  1 batchid
FROM
  (
    SELECT
      split(value, "[|]") val
    FROM
      text.`${tpcdi_directory}sf=${scale_factor}/Batch1/HoldingHistory.txt`
  );

-- COMMAND ----------

CREATE OR REPLACE VIEW ${catalog}.${wh_db}_${scale_factor}_stage.v_HoldingIncremental AS
SELECT
  try_cast(val[2] as INT) hh_h_t_id,
  try_cast(val[3] as INT) hh_t_id,
  try_cast(val[4] as INT) hh_before_qty,
  try_cast(val[5] as INT) hh_after_qty,
  INT(batchid) batchid
FROM
  (
    SELECT
      split(value, "[|]") val,
      substring(_metadata.file_path FROM (position('/Batch', _metadata.file_path) + 6) FOR 1) batchid 
    FROM
      text.`${tpcdi_directory}sf=${scale_factor}/Batch[23]/HoldingHistory.txt`
  );

-- COMMAND ----------

CREATE OR REPLACE VIEW ${catalog}.${wh_db}_${scale_factor}_stage.v_DailyMarketIncremental AS
WITH dailymarkethistorical as (
  SELECT
    try_cast(val[0] as DATE) dm_date,
    val[1] dm_s_symb,
    try_cast(val[2] as DOUBLE) dm_close,
    try_cast(val[3] as DOUBLE) dm_high,
    try_cast(val[4] as DOUBLE) dm_low,
    try_cast(val[5] as INT) dm_vol,
    1 batchid
  FROM
    (
      SELECT
        split(value, "[|]") val
      FROM
        text.`${tpcdi_directory}sf=${scale_factor}/Batch1/DailyMarket.txt`
    )
),
dailymarketincremental as (
  SELECT
    try_cast(val[2] as DATE) dm_date,
    val[3] dm_s_symb,
    try_cast(val[4] as DOUBLE) dm_close,
    try_cast(val[5] as DOUBLE) dm_high,
    try_cast(val[6] as DOUBLE) dm_low,
    try_cast(val[7] as INT) dm_vol,
    INT(batchid) batchid
  FROM
    (
      SELECT
        split(value, "[|]") val,
        substring(_metadata.file_path FROM (position('/Batch', _metadata.file_path) + 6) FOR 1) batchid 
      FROM
        text.`${tpcdi_directory}sf=${scale_factor}/Batch[23]/DailyMarket.txt`
    )
),
DailyMarket as (
  SELECT
    dm.*,
    min_by(struct(dm_low, dm_date), dm_low) OVER (
      PARTITION BY dm_s_symb
      ORDER BY dm_date ASC ROWS BETWEEN 364 PRECEDING AND CURRENT ROW
    ) fiftytwoweeklow,
    max_by(struct(dm_high, dm_date), dm_high) OVER (
      PARTITION by dm_s_symb
      ORDER BY dm_date ASC ROWS BETWEEN 364 PRECEDING AND CURRENT ROW
    ) fiftytwoweekhigh
  FROM
    (
      SELECT * FROM dailymarkethistorical
      UNION ALL
      SELECT * FROM dailymarketincremental
    ) dm
)
select
  dm.* except(fiftytwoweeklow, fiftytwoweekhigh),
  fiftytwoweekhigh.dm_high fiftytwoweekhigh,
  bigint(date_format(fiftytwoweekhigh.dm_date, 'yyyyMMdd')) sk_fiftytwoweekhighdate,
  fiftytwoweeklow.dm_low fiftytwoweeklow,
  bigint(date_format(fiftytwoweeklow.dm_date, 'yyyyMMdd')) sk_fiftytwoweeklowdate
from DailyMarket dm

-- COMMAND ----------

CREATE OR REPLACE VIEW ${catalog}.${wh_db}_${scale_factor}_stage.v_WatchHistory AS
SELECT
  try_cast(val[0] as BIGINT) w_c_id,
  val[1] w_s_symb,
  try_cast(val[2] as TIMESTAMP) w_dts,
  val[3] w_action,
  1 batchid
FROM
  (
    SELECT
      split(value, "[|]") val
    FROM
      text.`${tpcdi_directory}sf=${scale_factor}/Batch1/WatchHistory.txt`
  );

-- COMMAND ----------

CREATE OR REPLACE VIEW ${catalog}.${wh_db}_${scale_factor}_stage.v_WatchIncremental AS
SELECT
  try_cast(val[2] as BIGINT) w_c_id,
  val[3] w_s_symb,
  try_cast(val[4] as TIMESTAMP) w_dts,
  val[5] w_action,
  INT(batchid) batchid
FROM
  (
    SELECT
      split(value, "[|]") val,
      substring(_metadata.file_path FROM (position('/Batch', _metadata.file_path) + 6) FOR 1) batchid 
    FROM
      text.`${tpcdi_directory}sf=${scale_factor}/Batch[23]/WatchHistory.txt`
  );

-- COMMAND ----------

CREATE OR REPLACE VIEW ${catalog}.${wh_db}_${scale_factor}_stage.v_Prospect AS
with p as (
  select 
    max_by(array_append(val, batchid), batchid) val,
    min(batchid) batchid
  FROM ( 
    SELECT
      split(value, "[,]") val,
      substring(_metadata.file_path FROM (position('/Batch', _metadata.file_path) + 6) FOR 1) batchid 
    FROM read_files(
      "${tpcdi_directory}sf=${scale_factor}/Batch*",
      format => "text",
      inferSchema => False, 
      header => False,
      fileNamePattern => "Prospect.csv"
    )
  )
  group by val[0]
)
SELECT
  val[0] agencyid,
  val[1] lastname,
  val[2] firstname,
  val[3] middleinitial,
  val[4] gender,
  val[5] addressline1,
  val[6] addressline2,
  val[7] postalcode,
  val[8] city,
  val[9] state,
  val[10] country,
  val[11] phone,
  try_cast(val[12] as BIGINT) income,
  try_cast(val[13] as int) numbercars,
  try_cast(val[14] as int) numberchildren,
  val[15] maritalstatus,
  try_cast(val[16] as int) age,
  try_cast(val[17] as int) creditrating,
  val[18] ownorrentflag,
  val[19] employer,
  try_cast(val[20] as int) numbercreditcards,
  try_cast(val[21] as int) networth,
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
    NULL) marketingnameplate,
  val[22] recordbatchid,
  batchid
FROM p
where val[22] = 3

-- COMMAND ----------

CREATE OR REPLACE VIEW ${catalog}.${wh_db}_${scale_factor}_stage.v_FinWire AS
SELECT
  value,
  substring(value, 16, 3) rectype
FROM read_files(
  "${tpcdi_directory}sf=${scale_factor}/Batch1",
  format => "text",
  inferSchema => False, 
  header => False,
  fileNamePattern => "FINWIRE[0-9][0-9][0-9][0-9]Q[1-4]"
)

-- COMMAND ----------

CREATE OR REPLACE VIEW ${catalog}.${wh_db}_${scale_factor}_stage.v_BatchDate AS
SELECT
  DATE(val [0]) batchdate,
  INT(batchid) batchid
FROM
  (
    SELECT
      split(value, "[|]") val,
      substring(_metadata.file_path FROM (position('/Batch', _metadata.file_path) + 6) FOR 1) batchid 
    FROM read_files(
      "${tpcdi_directory}sf=${scale_factor}/Batch*",
      format => "text",
      inferSchema => False, 
      header => False,
      fileNamePattern => "BatchDate.txt"
    )
  );
