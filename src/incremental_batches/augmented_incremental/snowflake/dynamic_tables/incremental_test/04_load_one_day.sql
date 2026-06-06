-- ============================================================================
-- Step 4: Load one more day of bronzedailymarket into the test source.
--
-- Pre-existing range in bronzedailymarket_fmhtest: < 2016-01-01.
-- This INSERT adds dm_date = 2016-01-01 — a single new batch.
--
-- With TARGET_LAG = '1 minute' set on the test DTs, the Snowflake scheduler
-- should detect the source change within ~30s and trigger refreshes. Wait
-- ~1-2 minutes, then run step 05 to see which DTs ran INCREMENTAL vs FULL
-- and how long each took.
--
-- Run: snow sql -c tpcdi_kp -f 04_load_one_day.sql
-- ============================================================================

USE SCHEMA TPCDI_TEST.SHANNON_AUG_SF_DT_10;

INSERT INTO bronzedailymarket_fmhtest
SELECT * FROM bronzedailymarket WHERE dm_date = '2016-01-01';

SELECT
    COUNT(*)                  AS n_rows_total,
    MIN(dm_date)              AS min_dt,
    MAX(dm_date)              AS max_dt,
    SUM(IFF(dm_date = '2016-01-01', 1, 0)) AS n_new_rows
FROM bronzedailymarket_fmhtest;
