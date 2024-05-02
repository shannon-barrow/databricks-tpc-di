-- Databricks notebook source
-- CREATE WIDGET DROPDOWN scale_factor DEFAULT "10" CHOICES SELECT * FROM (VALUES ("10"), ("100"), ("1000"), ("5000"), ("10000"));
-- CREATE WIDGET TEXT tpcdi_directory DEFAULT "/Volumes/tpcdi/tpcdi_raw_data/tpcdi_volume/";
-- CREATE WIDGET TEXT wh_db DEFAULT '';
-- CREATE WIDGET TEXT catalog DEFAULT 'tpcdi';
-- CREATE WIDGET DROPDOWN batch_id DEFAULT '2' CHOICES SELECT * FROM (VALUES ("2"), ("3"));

-- COMMAND ----------

WITH account AS (
  SELECT
    accountid,
    b.sk_brokerid,
    dc.sk_customerid,
    accountdesc,
    taxstatus,
    decode(a.status, 
      'ACTV',	'Active',
      'CMPT','Completed',
      'CNCL','Canceled',
      'PNDG','Pending',
      'SBMT','Submitted',
      'INAC','Inactive') status,
    bd.batchdate effectivedate,
    date('9999-12-31') enddate
  FROM
    ${catalog}.${wh_db}_${scale_factor}_stage.AccountIncremental a
  JOIN 
    ${catalog}.${wh_db}_${scale_factor}.BatchDate bd 
    ON 
      a.batchid = bd.batchid
  JOIN 
    ${catalog}.${wh_db}_${scale_factor}.DimCustomer dc
    ON 
      dc.iscurrent
      and dc.customerid = a.customerid
  JOIN 
    ${catalog}.${wh_db}_${scale_factor}.DimBroker b 
    ON 
      a.brokerid = b.brokerid
  WHERE
    a.batchid = cast(${batch_id} as int)
),
cust_updates as (
  SELECT 
    a.accountid,
    a.sk_brokerid,
    ci.sk_customerid,
    a.status,    
    a.accountdesc,
    a.taxstatus,
    ci.effectivedate,
    ci.enddate
  FROM (
    SELECT 
      sk_customerid, 
      customerid,
      effectivedate,
      enddate
    FROM ${catalog}.${wh_db}_${scale_factor}.DimCustomer
    WHERE 
      iscurrent 
      AND batchid = cast(${batch_id} as int)) ci
  JOIN (
    SELECT 
      sk_customerid, 
      customerid,
      enddate
    FROM ${catalog}.${wh_db}_${scale_factor}.DimCustomer
    WHERE 
      !iscurrent
      AND batchid < cast(${batch_id} as int)) ch
    ON 
      ci.customerid = ch.customerid
      AND ch.enddate = ci.effectivedate  
  JOIN ${catalog}.${wh_db}_${scale_factor}.DimAccount a
    ON 
      ch.sk_customerid = a.sk_customerid
      AND a.iscurrent
),
all_incr_updates as (
  SELECT
    nvl(a.accountid, b.accountid) accountid,
    nvl(a.sk_brokerid, b.sk_brokerid) sk_brokerid,
    nvl(a.sk_customerid, b.sk_customerid) sk_customerid,
    nvl(a.accountdesc, b.accountdesc) accountdesc,
    nvl(a.TaxStatus, b.TaxStatus) TaxStatus,
    nvl(a.status, b.status) status,
    true iscurrent,
    cast(${batch_id} as int) batchid,
    nvl(a.effectivedate, b.effectivedate) effectivedate,
    nvl(a.enddate, b.enddate) enddate
  FROM account a
  FULL OUTER JOIN cust_updates b
    ON a.accountid = b.accountid
),
matched_accts as (
  SELECT
    s.accountid mergeKey,
    bigint(concat(date_format(s.effectivedate, 'yyyyMMdd'), s.accountid)) sk_accountid,
    s.accountid,
    nvl(s.sk_brokerid, t.sk_brokerid) sk_brokerid,
    s.sk_customerid,
    nvl(s.accountdesc, t.accountdesc) accountdesc,
    nvl(s.taxstatus, t.taxstatus) taxstatus,
    s.status,
    true iscurrent,
    s.batchid,
    s.effectivedate,
    s.enddate    
  FROM all_incr_updates s
  JOIN ${catalog}.${wh_db}_${scale_factor}.DimAccount t
    ON s.accountid = t.accountid
  WHERE t.iscurrent
)
MERGE INTO ${catalog}.${wh_db}_${scale_factor}.DimAccount t USING (
  SELECT
    CAST(NULL AS BIGINT) AS mergeKey,
    bigint(concat(date_format(effectivedate, 'yyyyMMdd'), accountid)) sk_accountid,
    dav.*
  FROM all_incr_updates dav
  UNION ALL
  SELECT *
  FROM matched_accts) s 
  ON t.accountid = s.mergeKey
WHEN MATCHED AND t.iscurrent THEN UPDATE SET
  t.iscurrent = false,
  t.enddate = s.effectivedate
WHEN NOT MATCHED THEN INSERT (sk_accountid, accountid, sk_brokerid, sk_customerid, accountdesc, TaxStatus, status, iscurrent, batchid, effectivedate, enddate)
VALUES (sk_accountid, accountid, sk_brokerid, sk_customerid, accountdesc, TaxStatus, status, iscurrent, batchid, effectivedate, enddate);
