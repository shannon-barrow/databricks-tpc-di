-- Databricks notebook source
-- CREATE WIDGET DROPDOWN scale_factor DEFAULT "10" CHOICES SELECT * FROM (VALUES ("10"), ("100"), ("1000"), ("5000"), ("10000"), ("20000"));
-- CREATE WIDGET TEXT tpcdi_directory DEFAULT "/Volumes/tpcdi/tpcdi_raw_data/tpcdi_volume/";
-- CREATE WIDGET TEXT wh_db DEFAULT "";
-- CREATE WIDGET TEXT catalog DEFAULT 'tpcdi';

-- COMMAND ----------

USE IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor);
CREATE OR REPLACE TABLE factwatches (
  sk_customerid BIGINT NOT NULL COMMENT 'Surrogate Key of the customer associated with watch list',
  sk_securityid BIGINT NOT NULL COMMENT 'Surrogate Key of the security listed on watch list',
  customerid BIGINT NOT NULL COMMENT 'Customer associated with watch list',
  symbol STRING NOT NULL COMMENT 'Security listed on watch list',
  sk_dateid_dateplaced BIGINT COMMENT 'Date the watch list item was added',
  sk_dateid_dateremoved BIGINT COMMENT 'Date the watch list item was removed',
  removed BOOLEAN COMMENT 'True if this watch has been removed',
  CONSTRAINT factwatches_pk PRIMARY KEY(sk_customerid, sk_securityid),
  CONSTRAINT factwatches_customer_fk FOREIGN KEY (sk_customerid) REFERENCES DimCustomer(sk_customerid),
  CONSTRAINT factwatches_security_fk FOREIGN KEY (sk_securityid) REFERENCES DimSecurity(sk_securityid),
  CONSTRAINT factwatches_dateplaced_fk FOREIGN KEY (sk_dateid_dateplaced) REFERENCES DimDate(sk_dateid),
  CONSTRAINT factwatches_dateremoved_fk FOREIGN KEY (sk_dateid_dateremoved) REFERENCES DimDate(sk_dateid)
) 
PARTITIONED BY (removed)
TBLPROPERTIES (
  'delta.autoOptimize.autoCompact' = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true'
);

-- COMMAND ----------

INSERT OVERWRITE IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.factwatches')
with watches as (
  SELECT 
    w_c_id customerid,
    w_s_symb symbol,        
    min(case when w_action = 'ACTV' then date(w_dts) end) dateplaced,
    max(case when w_action = 'CNCL' then date(w_dts) end) dateremoved
  -- Source: spark-gen temp Delta watchhistory{sf}; filter stg_target='tables' (= w_dts < 2015-07-06).
  FROM IDENTIFIER(:catalog || '.tpcdi_raw_data.watchhistory' || :scale_factor) a
  WHERE stg_target = 'tables'
  GROUP BY ALL
)
select
  c.sk_customerid sk_customerid,
  s.sk_securityid sk_securityid,
  wh.customerid,
  wh.symbol,
  bigint(date_format(wh.dateplaced, 'yyyyMMdd')) sk_dateid_dateplaced,
  bigint(date_format(wh.dateremoved, 'yyyyMMdd')) sk_dateid_dateremoved,
  nvl2(wh.dateremoved, True, False) removed
from Watches wh
JOIN IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimSecurity') s 
  ON 
    s.symbol = wh.symbol
    AND wh.dateplaced >= s.effectivedate 
    AND wh.dateplaced < s.enddate
JOIN IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimCustomer') c 
  ON
    wh.customerid = c.customerid
    AND wh.dateplaced >= c.effectivedate 
    AND wh.dateplaced < c.enddate