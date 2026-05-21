-- Databricks notebook source
-- CREATE WIDGET DROPDOWN scale_factor DEFAULT "10" CHOICES SELECT * FROM (VALUES ("10"), ("100"), ("1000"), ("5000"), ("10000"), ("20000"));
-- CREATE WIDGET TEXT tpcdi_directory DEFAULT "/Volumes/tpcdi/tpcdi_raw_data/tpcdi_volume/";
-- CREATE WIDGET TEXT wh_db DEFAULT "";
-- CREATE WIDGET TEXT catalog DEFAULT 'tpcdi';

-- COMMAND ----------

USE IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor);

-- Pre-window DailyMarket events ([2015-07-06, 2016-07-05] under the 365-day
-- AUG_FILES window). Persisted as its own staging table so it survives
-- cleanup_stage0 dropping the temp tpcdi_raw_data.dailymarket{sf} Delta.
--
-- Consumers:
--   - Classic / dbt: setup.py / setup_dbt.py clones (or re-INSERTs) into
--     `{wh_db}_{sf}.bronzedailymarket` so FactMarketHistory's 365-day
--     lookback is fully populated from batch 1 (2016-07-06).
--   - SDP: dlt_ingest_bronze's `bronzedailymarket_backfill` append_flow
--     reads from this once, before any per-batch update fires. The
--     downstream `factmarkethistorystg` MV's 380-day window over
--     bronzedailymarket then computes the correct rolling-year aggregate
--     from batch 1.
--
-- Schema mirrors the bronze ingestion shape (cdc_flag + cdc_dsn synthesized
-- here since the spark-gen DailyMarket Delta is the raw shape).
CREATE OR REPLACE TABLE bronzedailymarket (
  cdc_flag STRING,
  cdc_dsn BIGINT,
  dm_date DATE,
  dm_s_symb STRING,
  dm_close DOUBLE,
  dm_high DOUBLE,
  dm_low DOUBLE,
  dm_vol INT
)
CLUSTER BY (dm_date)  -- liquid: matches setup / dbt-Liquid (FMH 365-day rolling lookback filters on dm_date)
TBLPROPERTIES (
  -- Match the per-variant bronzedailymarket props -- Classic deep-clones from
  -- here; deep clone preserves source TBLPROPERTIES. autoCompact=false to
  -- avoid contention during the daily Auto Loader appends post-clone.
  'delta.autoOptimize.autoCompact' = 'false',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.columnMapping.mode' = 'name',
  'delta.enableDeletionVectors' = 'false',
  'delta.enableIcebergCompatV2' = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg'
);

INSERT OVERWRITE bronzedailymarket
SELECT
  'I'      AS cdc_flag,
  0        AS cdc_dsn,
  dm_date,
  dm_s_symb,
  dm_close,
  dm_high,
  dm_low,
  dm_vol
FROM IDENTIFIER(:catalog || '.tpcdi_raw_data.dailymarket' || :scale_factor)
WHERE stg_target = 'tables';
