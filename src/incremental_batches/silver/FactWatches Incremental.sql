-- Databricks notebook source
-- CREATE WIDGET DROPDOWN scale_factor DEFAULT "10" CHOICES SELECT * FROM (VALUES ("10"), ("100"), ("1000"), ("5000"), ("10000"));
-- CREATE WIDGET TEXT tpcdi_directory DEFAULT "/Volumes/tpcdi/tpcdi_raw_data/tpcdi_volume/";
-- CREATE WIDGET TEXT wh_db DEFAULT '';
-- CREATE WIDGET TEXT catalog DEFAULT 'tpcdi';
-- CREATE WIDGET DROPDOWN batch_id DEFAULT '2' CHOICES SELECT * FROM (VALUES ("2"), ("3"));

-- COMMAND ----------

-- MAGIC %md
-- MAGIC The merge is made a bit more complicated than one would expect since the FactWatches table does NOT have the customerid or symbol in the table.  It only has the surrogate key of those natural keys, meaning we have to join to the Dim tables to get the original SK of the Watch, then we can merge.

-- COMMAND ----------

with Watches as (
  SELECT 
    wh.w_c_id customerid,
    wh.w_s_symb symbol,        
    date(min(if(w_action != 'CNCL', w_dts, null))) dateplaced,
    date(max(if(w_action = 'CNCL', w_dts, null))) dateremoved
  FROM ${catalog}.${wh_db}_${scale_factor}_stage.WatchIncremental wh
  where batchid = cast(${batch_id} as int)
  GROUP BY 
    w_c_id,
    w_s_symb
),
Watch_actv AS (
  SELECT
    c.sk_customerid sk_customerid,
    s.sk_securityid sk_securityid,
    bigint(date_format(dateplaced, 'yyyyMMdd')) sk_dateid_dateplaced,
    bigint(date_format(dateremoved, 'yyyyMMdd')) sk_dateid_dateremoved,
    cast(${batch_id} as int) batchid
  from Watches wh
  JOIN ${catalog}.${wh_db}_${scale_factor}.DimSecurity s 
    ON 
      s.symbol = wh.symbol
      AND s.iscurrent
  JOIN ${catalog}.${wh_db}_${scale_factor}.DimCustomer c 
    ON
      wh.customerid = c.customerid
      AND c.iscurrent
  WHERE dateplaced IS NOT NULL
),
Watch_cncl AS (
  SELECT
    fw.sk_customerid, 
    fw.sk_securityid, 
    cast(null as bigint) sk_dateid_dateplaced, 
    bigint(date_format(dateremoved, 'yyyyMMdd')) sk_dateid_dateremoved,
    cast(${batch_id} as int) batchid
  FROM (
    SELECT
      sk_customerid,
      sk_securityid
    FROM ${catalog}.${wh_db}_${scale_factor}.FactWatches
    WHERE sk_dateid_dateremoved is null
  ) fw -- existing Active watches
  JOIN ${catalog}.${wh_db}_${scale_factor}.DimCustomer c 
    ON fw.sk_customerid = c.sk_customerid
  JOIN ${catalog}.${wh_db}_${scale_factor}.DimSecurity s 
    ON fw.sk_securityid = s.sk_securityid
  JOIN (
    SELECT 
      customerid,
      symbol,
      dateremoved
    FROM Watches
    WHERE isnull(dateplaced) -- Canceled Watches
  ) w
  ON
    w.customerid = c.customerid
    AND w.symbol = s.symbol
)
MERGE INTO ${catalog}.${wh_db}_${scale_factor}.FactWatches t
USING (
  SELECT
    sk_customerid AS merge_sk_customerid,
    sk_securityid AS merge_sk_securityid,
    wc.*
  FROM Watch_cncl wc
  WHERE 
    sk_securityid IS NOT NULL 
    AND sk_customerid IS NOT NULL
  UNION ALL
  SELECT 
    CAST(NULL AS BIGINT) AS merge_sk_customerid,
    CAST(NULL AS BIGINT) AS merge_sk_securityid,
    wa.*
  FROM Watch_actv wa
  WHERE 
    sk_securityid IS NOT NULL 
    AND sk_customerid IS NOT NULL) s
ON 
  t.sk_dateid_dateremoved is null
  AND t.sk_securityid = s.merge_sk_securityid
  AND t.sk_customerid = s.merge_sk_customerid
WHEN MATCHED THEN UPDATE SET
  t.sk_dateid_dateremoved = s.sk_dateid_dateremoved
  ,t.batchid = s.batchid -- not sure if we should be updating batchid or keep as the original
WHEN NOT MATCHED THEN 
INSERT (sk_customerid, sk_securityid, sk_dateid_dateplaced, sk_dateid_dateremoved, batchid)
VALUES (sk_customerid, sk_securityid, sk_dateid_dateplaced, sk_dateid_dateremoved, batchid)
