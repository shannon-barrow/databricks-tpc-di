-- ============================================================================
-- Step 10: Pattern I + the downstream joins from the real FMH DT.
--
-- Adds (vs Pattern I):
--   - JOIN dimsecurity (SCD2 temporal — symbol + date ∈ [effectivedate, enddate))
--   - LEFT JOIN companyyeareps (functional predicates on QUARTER/YEAR)
--   - Final projection matching factmarkethistory schema (sk_securityid,
--     sk_companyid, sk_dateid, peratio, yield, fiftytwoweek*, sk_fiftytwoweek*,
--     closeprice/dayhigh/daylow/volume)
--
-- Question: does the AUTO planner still pick INCREMENTAL with these joins, or
-- do they force FULL (range-predicate on dimsecurity, functional preds on EPS)?
--
-- Run AFTER step 09 (source + truth in place).
-- ============================================================================

USE SCHEMA TPCDI_TEST.SHANNON_AUG_SF_DT_10;
USE WAREHOUSE BARROW_XS_GEN2;


CREATE OR REPLACE DYNAMIC TABLE fmhtest_i_with_joins
    TARGET_LAG   = DOWNSTREAM
    WAREHOUSE    = BARROW_XS_GEN2
    REFRESH_MODE = AUTO
AS
WITH per_day AS (
    SELECT
        dm_s_symb, dm_date, dm_close, dm_high, dm_low, dm_vol,
        ROUND(dm_low::number(38,2)  * 100)::number(38,0) * 100000
            +             DATEDIFF(day, DATE '1900-01-01', dm_date)    AS low_packed,
        ROUND(dm_high::number(38,2) * 100)::number(38,0) * 100000
            + (99999 -    DATEDIFF(day, DATE '1900-01-01', dm_date))   AS high_packed
    FROM bronzedailymarket_fmhtest
),
windowed AS (
    SELECT
        dm_s_symb, dm_date, dm_close, dm_high, dm_low, dm_vol,
        MIN(low_packed) OVER (
            PARTITION BY dm_s_symb ORDER BY dm_date
            ROWS BETWEEN 364 PRECEDING AND CURRENT ROW
        ) AS low_packed_52w,
        MAX(high_packed) OVER (
            PARTITION BY dm_s_symb ORDER BY dm_date
            ROWS BETWEEN 364 PRECEDING AND CURRENT ROW
        ) AS high_packed_52w
    FROM per_day
),
unpacked AS (
    SELECT
        dm_s_symb, dm_date, dm_close, dm_high, dm_low, dm_vol,
        (FLOOR(low_packed_52w  / 100000) / 100)::number(15,2)              AS fiftytwoweeklow,
        DATEADD(day,            MOD(low_packed_52w,  100000), DATE '1900-01-01') AS fiftytwoweeklowdate,
        (FLOOR(high_packed_52w / 100000) / 100)::number(15,2)              AS fiftytwoweekhigh,
        DATEADD(day, 99999 -    MOD(high_packed_52w, 100000), DATE '1900-01-01') AS fiftytwoweekhighdate
    FROM windowed
)
SELECT
    s.sk_securityid,
    s.sk_companyid,
    TO_CHAR(dm.dm_date, 'YYYYMMDD')::number                  AS sk_dateid,
    DIV0(dm.dm_close, f.prev_year_basic_eps)                 AS peratio,
    DIV0(s.dividend,  dm.dm_close) / 100                     AS yield,
    dm.fiftytwoweekhigh,
    TO_CHAR(dm.fiftytwoweekhighdate, 'YYYYMMDD')::number     AS sk_fiftytwoweekhighdate,
    dm.fiftytwoweeklow,
    TO_CHAR(dm.fiftytwoweeklowdate,  'YYYYMMDD')::number     AS sk_fiftytwoweeklowdate,
    dm.dm_close                                              AS closeprice,
    dm.dm_high                                               AS dayhigh,
    dm.dm_low                                                AS daylow,
    dm.dm_vol                                                AS volume
FROM unpacked dm
JOIN dimsecurity s
    ON  s.symbol    = dm.dm_s_symb
    AND dm.dm_date >= s.effectivedate
    AND dm.dm_date <  s.enddate
LEFT JOIN companyyeareps f
    ON  f.sk_companyid     = s.sk_companyid
    AND QUARTER(dm.dm_date) = QUARTER(f.qtr_start_date)
    AND YEAR(dm.dm_date)    = YEAR(f.qtr_start_date);
