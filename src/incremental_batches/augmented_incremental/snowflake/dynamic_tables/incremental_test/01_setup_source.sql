-- ============================================================================
-- Step 1: Build the test source table.
--
-- Goal: a private copy of bronzedailymarket with the same DDL, seeded with
-- pre-2016-01-01 data only. We'll insert one more day later (step 04) to
-- simulate a daily-batch arrival and observe how the test DTs refresh.
--
-- Why a copy and not bronzedailymarket directly: we don't want to disturb
-- the real DT graph or its CHANGE_TRACKING stream, and we want a fully
-- self-contained test the user can drop at the end.
--
-- Run: snow sql -c tpcdi_kp -f 01_setup_source.sql
-- ============================================================================

USE SCHEMA TPCDI_TEST.SHANNON_AUG_SF_DT_10;

CREATE OR REPLACE TABLE bronzedailymarket_fmhtest (
    cdc_flag  STRING,
    cdc_dsn   NUMBER(38,0),
    dm_date   DATE,
    dm_s_symb STRING,
    dm_close  FLOAT,
    dm_high   FLOAT,
    dm_low    FLOAT,
    dm_vol    NUMBER(10,0)
)
CHANGE_TRACKING = TRUE
COMMENT = 'Test source for DT incremental-refresh experiments. See incremental_test/README.md';

INSERT INTO bronzedailymarket_fmhtest
SELECT * FROM bronzedailymarket WHERE dm_date < '2016-01-01';

SELECT
    COUNT(*)            AS n_rows,
    MIN(dm_date)        AS min_dt,
    MAX(dm_date)        AS max_dt,
    COUNT(DISTINCT dm_s_symb) AS n_symbols
FROM bronzedailymarket_fmhtest;
