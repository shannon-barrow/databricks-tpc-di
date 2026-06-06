-- ============================================================================
-- Step 8: Pattern J — Pattern H but with INNER JOIN instead of LEFT JOIN.
--
-- Snowflake's H error mentioned "outer joins with non-equality predicates."
-- Since the join-by-value-and-window match is guaranteed (the MIN/MAX value
-- came from a row inside the same window), there is no NULL-preserving need
-- for an outer join. INNER JOIN is semantically correct here. Does that
-- alone unlock INCREMENTAL?
--
-- Run AFTER step 01 (source already populated).
-- ============================================================================

USE SCHEMA TPCDI_TEST.SHANNON_AUG_SF_DT_10;
USE WAREHOUSE BARROW_XS_GEN2;


CREATE OR REPLACE DYNAMIC TABLE fmhtest_j_inner_join
    TARGET_LAG   = DOWNSTREAM
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
JOIN per_day plow
    ON  plow.dm_s_symb = w.dm_s_symb
    AND plow.dm_date  BETWEEN DATEADD('day', -364, w.dm_date) AND w.dm_date
    AND plow.dm_low    = w.fiftytwoweeklow
JOIN per_day phigh
    ON  phigh.dm_s_symb = w.dm_s_symb
    AND phigh.dm_date  BETWEEN DATEADD('day', -364, w.dm_date) AND w.dm_date
    AND phigh.dm_high   = w.fiftytwoweekhigh
GROUP BY
    w.dm_s_symb, w.dm_date, w.dm_close, w.dm_high, w.dm_low, w.dm_vol,
    w.fiftytwoweeklow, w.fiftytwoweekhigh;
