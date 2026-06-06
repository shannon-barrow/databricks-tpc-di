-- ============================================================================
-- Step 6 (extension): Pattern G — FIRST_VALUE(OBJECT_CONSTRUCT(...)) with
-- the window ORDER BY value (dm_low ASC / dm_high DESC) instead of dm_date.
--
-- Idea: the OBJECT carries both the value and its date in one window pass;
-- ordering by the value puts the row we want at position 1 of the frame.
--
-- NOTE on semantics: with `ORDER BY dm_low ASC ROWS BETWEEN 364 PRECEDING
-- AND CURRENT ROW`, the "364 PRECEDING" rows are the 364 rows with lower
-- dm_low values (tie-broken by dm_date), NOT rows from the last 364 days.
-- So this is a different window than the TPC-DI 52-week-by-date semantics.
-- We're still testing it to see whether the pattern stays INCREMENTAL after
-- an upstream change — if yes, we know FIRST_VALUE(OBJECT_CONSTRUCT) on a
-- sliding frame is INCREMENTAL-eligible and we can shape the right ordering
-- to recover the date semantics we actually want.
--
-- Run after step 02 (source already populated).
-- Run: snow sql -c tpcdi_kp -f 06_pattern_g_firstvalue.sql
-- ============================================================================

USE SCHEMA TPCDI_TEST.SHANNON_AUG_SF_DT_10;
USE WAREHOUSE BARROW_XS_GEN2;


CREATE OR REPLACE DYNAMIC TABLE fmhtest_g_firstvalue_object
    TARGET_LAG   = '1 minute'
    WAREHOUSE    = BARROW_XS_GEN2
    REFRESH_MODE = AUTO
AS
SELECT
    dm.*,
    FIRST_VALUE(OBJECT_CONSTRUCT('dm_low', dm_low, 'dm_date', dm_date)) OVER (
        PARTITION BY dm_s_symb
        ORDER BY dm_low ASC, dm_date ASC
        ROWS BETWEEN 364 PRECEDING AND CURRENT ROW
    ) AS fiftytwoweeklow,
    FIRST_VALUE(OBJECT_CONSTRUCT('dm_high', dm_high, 'dm_date', dm_date)) OVER (
        PARTITION BY dm_s_symb
        ORDER BY dm_high DESC, dm_date ASC
        ROWS BETWEEN 364 PRECEDING AND CURRENT ROW
    ) AS fiftytwoweekhigh
FROM bronzedailymarket_fmhtest dm;


-- Inspect refresh_mode chosen for G
SELECT name, refresh_mode, refresh_mode_reason
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLES())
WHERE database_name = 'TPCDI_TEST'
  AND schema_name   = 'SHANNON_AUG_SF_DT_10'
  AND name = 'FMHTEST_G_FIRSTVALUE_OBJECT';
