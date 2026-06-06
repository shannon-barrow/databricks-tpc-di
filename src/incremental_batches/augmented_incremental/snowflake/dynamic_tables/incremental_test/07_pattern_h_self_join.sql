-- ============================================================================
-- Step 7: Pattern H — Pattern D windowed MIN/MAX + the self-join that
-- recovers the date of each 52-week low/high. Exact shape of our real
-- FMH DT (minus the final dimsecurity/companyyeareps joins; testing the
-- self-join in isolation).
--
-- Question: does Snowflake's optimizer keep this INCREMENTAL once the
-- BETWEEN range join is added back? The Snowflake docs flag "range joins"
-- as a FULL-refresh trigger. We've now confirmed plain sliding-MIN/MAX is
-- INCREMENTAL (Pattern D); this tests whether the join undoes that.
--
-- Run AFTER step 02 (source already populated).
-- ============================================================================

USE SCHEMA TPCDI_TEST.SHANNON_AUG_SF_DT_10;
USE WAREHOUSE BARROW_XS_GEN2;


CREATE OR REPLACE DYNAMIC TABLE fmhtest_h_self_join
    TARGET_LAG   = '1 minute'
    WAREHOUSE    = BARROW_XS_GEN2
    REFRESH_MODE = AUTO
AS
WITH per_day AS (
    SELECT dm_s_symb, dm_date, dm_close, dm_high, dm_low, dm_vol
    FROM bronzedailymarket_fmhtest
),
windowed_vals AS (
    SELECT
        dm_s_symb, dm_date, dm_close, dm_high, dm_low, dm_vol,
        MIN(dm_low)  OVER (
            PARTITION BY dm_s_symb ORDER BY dm_date
            ROWS BETWEEN 364 PRECEDING AND CURRENT ROW
        ) AS fiftytwoweeklow,
        MAX(dm_high) OVER (
            PARTITION BY dm_s_symb ORDER BY dm_date
            ROWS BETWEEN 364 PRECEDING AND CURRENT ROW
        ) AS fiftytwoweekhigh
    FROM per_day
)
SELECT
    w.dm_s_symb, w.dm_date, w.dm_close, w.dm_high, w.dm_low, w.dm_vol,
    w.fiftytwoweeklow, w.fiftytwoweekhigh,
    MAX(plow.dm_date)  AS fiftytwoweeklowdate,
    MAX(phigh.dm_date) AS fiftytwoweekhighdate
FROM windowed_vals w
LEFT JOIN per_day plow
    ON  plow.dm_s_symb = w.dm_s_symb
    AND plow.dm_date  BETWEEN DATEADD('day', -364, w.dm_date) AND w.dm_date
    AND plow.dm_low    = w.fiftytwoweeklow
LEFT JOIN per_day phigh
    ON  phigh.dm_s_symb = w.dm_s_symb
    AND phigh.dm_date  BETWEEN DATEADD('day', -364, w.dm_date) AND w.dm_date
    AND phigh.dm_high   = w.fiftytwoweekhigh
GROUP BY
    w.dm_s_symb, w.dm_date, w.dm_close, w.dm_high, w.dm_low, w.dm_vol,
    w.fiftytwoweeklow, w.fiftytwoweekhigh;
