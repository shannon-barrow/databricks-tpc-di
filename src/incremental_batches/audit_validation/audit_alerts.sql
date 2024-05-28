-- Databricks notebook source
-- CREATE WIDGET DROPDOWN scale_factor DEFAULT "10" CHOICES SELECT * FROM (VALUES ("10"), ("100"), ("1000"), ("5000"), ("10000"));
-- CREATE WIDGET TEXT wh_db DEFAULT '';
-- CREATE WIDGET TEXT catalog DEFAULT 'tpcdi';

-- COMMAND ----------

INSERT INTO ${catalog}.${wh_db}_${scale_factor}.DIMessages
SELECT 
  CURRENT_TIMESTAMP() AS MessageDateAndTime,
  batchid,
  MessageSource,
  MessageText,
  'Alert' AS MessageType,
  MessageData
FROM (
  SELECT 
    batchid,
    'DimCustomer' MessageSource,
    'Invalid customer tier' MessageText,
    concat('C_ID = ', customerid, ', C_TIER = ', nvl(cast(tier AS string), 'null')) MessageData
  FROM (
    SELECT 
      customerid, 
      tier,
      batchid
    FROM ${catalog}.${wh_db}_${scale_factor}.DimCustomer
    WHERE 
      tier NOT IN (1,2,3)
      OR tier IS NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customerid, batchid ORDER BY enddate desc) = 1)
  UNION ALL
  SELECT DISTINCT
    batchid,
    'DimCustomer' MessageSource,
    'DOB out of range' MessageText,
    concat('C_ID = ', customerid, ', C_DOB = ', dob) MessageData
  FROM ${catalog}.${wh_db}_${scale_factor}.DimCustomer dc
  JOIN ${catalog}.${wh_db}_${scale_factor}.batchdate bd USING (batchid)
  WHERE
    datediff(YEAR, dob, batchdate) >= 100
    OR dob > batchdate
  UNION ALL
  SELECT DISTINCT
    batchid,
    'DimTrade' MessageSource,
    'Invalid trade commission' MessageText,
    concat('T_ID = ', tradeid, ', T_COMM = ', commission) MessageData
  FROM ${catalog}.${wh_db}_${scale_factor}.DimTrade
  WHERE
    commission IS NOT NULL
    AND commission > tradeprice * quantity
  UNION ALL
  SELECT DISTINCT
    batchid,
    'DimTrade' MessageSource,
    'Invalid trade fee' MessageText,
    concat('T_ID = ', tradeid, ', T_CHRG = ', fee) MessageData
  FROM ${catalog}.${wh_db}_${scale_factor}.DimTrade
  WHERE
    fee IS NOT NULL
    AND fee > tradeprice * quantity
  UNION ALL
  SELECT DISTINCT
    fmh.batchid,
    'FactMarketHistory' MessageSource,
    'No earnings for company' MessageText,
    concat('DM_S_SYMB = ', symbol) MessageData
  FROM ${catalog}.${wh_db}_${scale_factor}.FactMarketHistory fmh
  JOIN ${catalog}.${wh_db}_${scale_factor}.DimSecurity ds 
    ON 
      ds.sk_securityid = fmh.sk_securityid
  WHERE
    PERatio IS NULL
  UNION ALL
  SELECT DISTINCT
    1 batchid,
    'DimCompany' MessageSource,
    'Invalid SPRating' MessageText,
    concat('CO_ID = ', cik, ', CO_SP_RATE = ', sprating) MessageData
  FROM (
    SELECT 
      trim(substring(value, 79, 10)) CIK, 
      trim(substring(value, 95, 4)) sprating 
    FROM ${catalog}.${wh_db}_${scale_factor}_stage.finwire
    WHERE rectype = 'CMP')
  WHERE sprating NOT IN ('AAA','AA','A','BBB','BB','B','CCC','CC','C','D','AA+','A+','BBB+','BB+','B+','CCC+','AA-','A-','BBB-','BB-','B-','CCC-')
)
