-- ============================================================================
-- Step 2: Create 6 test Dynamic Tables — each with REFRESH_MODE = AUTO so
-- Snowflake picks INCREMENTAL or FULL.
--
-- TARGET_LAG = '1 minute' so an upstream INSERT (step 04) triggers a refresh
-- promptly. INITIALIZE = ON_CREATE blocks until the initial backfill
-- finishes, so step 03 sees stable refresh_mode values.
--
-- Pattern matrix:
--   A — no window           (control; always INCREMENTAL)
--   B — MIN/MAX OVER PARTITION BY only
--   C — MIN/MAX OVER PARTITION BY ... ORDER BY (default RANGE frame)
--   D — MIN/MAX OVER PARTITION BY ... ORDER BY ROWS BETWEEN 364 PRECEDING
--                                              AND CURRENT ROW (sliding 52w)
--   E — MIN_BY/MAX_BY OVER PARTITION BY only        (2026-03-19 GA — full partition)
--   F — MIN_BY/MAX_BY OVER PARTITION BY ORDER BY ROWS BETWEEN 364 PRECEDING
--                                                  AND CURRENT ROW (sliding 52w on MIN_BY/MAX_BY)
--
-- Run: snow sql -c tpcdi_kp -f 02_create_test_dts.sql
-- ============================================================================

USE SCHEMA TPCDI_TEST.SHANNON_AUG_SF_DT_10;
USE WAREHOUSE BARROW_XS_GEN2;


-- A: control — no window
CREATE OR REPLACE DYNAMIC TABLE fmhtest_a_no_window
    TARGET_LAG   = '1 minute'
    WAREHOUSE    = BARROW_XS_GEN2
    REFRESH_MODE = AUTO
AS
SELECT dm_s_symb, dm_date, dm_close, dm_high, dm_low, dm_vol
FROM bronzedailymarket_fmhtest;


-- B: MIN/MAX OVER PARTITION (full partition, no ORDER BY)
CREATE OR REPLACE DYNAMIC TABLE fmhtest_b_minmax_partition
    TARGET_LAG   = '1 minute'
    WAREHOUSE    = BARROW_XS_GEN2
    REFRESH_MODE = AUTO
AS
SELECT
    dm_s_symb, dm_date, dm_close,
    MIN(dm_low)  OVER (PARTITION BY dm_s_symb) AS min_low_all,
    MAX(dm_high) OVER (PARTITION BY dm_s_symb) AS max_high_all
FROM bronzedailymarket_fmhtest;


-- C: MIN/MAX OVER PARTITION ... ORDER BY (default frame: RANGE UNBOUNDED PRECEDING)
CREATE OR REPLACE DYNAMIC TABLE fmhtest_c_minmax_orderby_default_frame
    TARGET_LAG   = '1 minute'
    WAREHOUSE    = BARROW_XS_GEN2
    REFRESH_MODE = AUTO
AS
SELECT
    dm_s_symb, dm_date, dm_close,
    MIN(dm_low)  OVER (PARTITION BY dm_s_symb ORDER BY dm_date) AS min_low_so_far,
    MAX(dm_high) OVER (PARTITION BY dm_s_symb ORDER BY dm_date) AS max_high_so_far
FROM bronzedailymarket_fmhtest;


-- D: MIN/MAX OVER sliding 52-week window (THIS IS THE FMH PATTERN)
CREATE OR REPLACE DYNAMIC TABLE fmhtest_d_minmax_sliding_52w
    TARGET_LAG   = '1 minute'
    WAREHOUSE    = BARROW_XS_GEN2
    REFRESH_MODE = AUTO
AS
SELECT
    dm_s_symb, dm_date, dm_close,
    MIN(dm_low)  OVER (
        PARTITION BY dm_s_symb ORDER BY dm_date
        ROWS BETWEEN 364 PRECEDING AND CURRENT ROW
    ) AS fiftytwoweeklow,
    MAX(dm_high) OVER (
        PARTITION BY dm_s_symb ORDER BY dm_date
        ROWS BETWEEN 364 PRECEDING AND CURRENT ROW
    ) AS fiftytwoweekhigh
FROM bronzedailymarket_fmhtest;


-- E: MIN_BY/MAX_BY OVER PARTITION (full partition) — tests post-2026-03-19 GA
CREATE OR REPLACE DYNAMIC TABLE fmhtest_e_minby_partition
    TARGET_LAG   = '1 minute'
    WAREHOUSE    = BARROW_XS_GEN2
    REFRESH_MODE = AUTO
AS
SELECT
    dm_s_symb, dm_date,
    MIN_BY(dm_date, dm_low)  OVER (PARTITION BY dm_s_symb) AS date_at_min_low,
    MAX_BY(dm_date, dm_high) OVER (PARTITION BY dm_s_symb) AS date_at_max_high
FROM bronzedailymarket_fmhtest;


-- F: MIN_BY/MAX_BY OVER sliding 52w — the *natural* shape of the original
-- FMH (date associated with the 52-week low/high). Previously errored with
-- "Sliding window frame unsupported for function MIN_BY"; does the GA fix it?
CREATE OR REPLACE DYNAMIC TABLE fmhtest_f_minby_sliding_52w
    TARGET_LAG   = '1 minute'
    WAREHOUSE    = BARROW_XS_GEN2
    REFRESH_MODE = AUTO
AS
SELECT
    dm_s_symb, dm_date,
    MIN_BY(dm_date, dm_low)  OVER (
        PARTITION BY dm_s_symb ORDER BY dm_date
        ROWS BETWEEN 364 PRECEDING AND CURRENT ROW
    ) AS date_at_52w_low,
    MAX_BY(dm_date, dm_high) OVER (
        PARTITION BY dm_s_symb ORDER BY dm_date
        ROWS BETWEEN 364 PRECEDING AND CURRENT ROW
    ) AS date_at_52w_high
FROM bronzedailymarket_fmhtest;
