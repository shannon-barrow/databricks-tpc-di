-- Databricks notebook source
-- Drops the 7 temp Delta tables in tpcdi_raw_data created by stage 0 of the
-- augmented_incremental data-gen workflow. Runs unconditionally at the end
-- of the workflow (ALL_DONE) once stage_tables + stage_files have consumed
-- the data. The per-day files at augmented_incremental/_staging/sf={sf}/
-- and the tpcdi_incremental_staging_{sf} schema are the kept deliverables;
-- these temp tables are not.

-- COMMAND ----------

-- CREATE WIDGET DROPDOWN scale_factor DEFAULT "10" CHOICES SELECT * FROM (VALUES ("10"), ("100"), ("1000"), ("5000"), ("10000"), ("20000"));
-- CREATE WIDGET TEXT catalog DEFAULT 'main';

-- COMMAND ----------

DROP TABLE IF EXISTS IDENTIFIER(:catalog || '.tpcdi_raw_data.customermgmt'    || :scale_factor);
DROP TABLE IF EXISTS IDENTIFIER(:catalog || '.tpcdi_raw_data.trade'           || :scale_factor);
DROP TABLE IF EXISTS IDENTIFIER(:catalog || '.tpcdi_raw_data.tradehistory'    || :scale_factor);
DROP TABLE IF EXISTS IDENTIFIER(:catalog || '.tpcdi_raw_data.cashtransaction' || :scale_factor);
DROP TABLE IF EXISTS IDENTIFIER(:catalog || '.tpcdi_raw_data.holdinghistory'  || :scale_factor);
DROP TABLE IF EXISTS IDENTIFIER(:catalog || '.tpcdi_raw_data.watchhistory'    || :scale_factor);
DROP TABLE IF EXISTS IDENTIFIER(:catalog || '.tpcdi_raw_data.dailymarket'     || :scale_factor);
