-- ============================================================================
-- Step 9: Pattern I — packed-encoding approach. Replaces the BETWEEN self-join
-- that was forcing FULL refresh in our real FMH DT.
--
-- Idea: encode (value, date) as one sortable scalar inside the per-row
-- projection, so MIN/MAX over the sliding 52-week window returns ONE number
-- that we decode back into (value, date) downstream. Single windowed pass,
-- no joins — same shape as Pattern D which we already proved is INCREMENTAL
-- append-only.
--
-- Encoding (carries through "value tiebroken by EARLIEST date" per
-- TPC-DI spec: "Earliest date on which the 52 week low/high price was set"):
--   value_as_int * 100000 + date_slot
--   where:
--     value_as_int = ROUND(value * 100) — captures 2 decimals (cents) as integer
--     date_slot    = 5-digit DATEDIFF(day, '1900-01-01', dm_date) field
--                    (5 digits = 273-year span from epoch; plenty of headroom)
--     date_slot preserved as-is for LOW (MIN picks smallest value, then
--       smallest date_slot = earliest date for ties).
--     date_slot inverted for HIGH (MAX picks largest value, then largest
--       (99999 - date_slot) = smallest date_slot = earliest date for ties).
--
-- Decoding:
--   value = (FLOOR(packed / 100000) / 100)::number(15,2)
--   date  = '1900-01-01' + date_slot days            [for LOW]
--   date  = '1900-01-01' + (99999 - date_slot) days  [for HIGH]
--
-- Critical detail: cast the value to NUMBER(38,4) BEFORE multiplying by 10000
-- and then to NUMBER(38,0). If you multiply NUMBER(15,4) * 100000 directly,
-- the cents portion bleeds into the 5-digit date slot and corrupts both
-- fields (debugged 2026-05-25).
--
-- Final price precision is NUMBER(15,2) on output — TPC-DI prices are 2 dp.
--
-- Run AFTER step 01 (source already populated).
-- ============================================================================

USE SCHEMA TPCDI_TEST.SHANNON_AUG_SF_DT_10;
USE WAREHOUSE BARROW_XS_GEN2;


CREATE OR REPLACE DYNAMIC TABLE fmhtest_i_encoded
    TARGET_LAG   = DOWNSTREAM
    WAREHOUSE    = BARROW_XS_GEN2
    REFRESH_MODE = AUTO
AS
WITH per_day AS (
    SELECT
        dm_s_symb, dm_date, dm_close, dm_high, dm_low, dm_vol,
        -- LOW: MIN picks smallest value first; for ties, smallest offset = earliest date wins.
        ROUND(dm_low::number(38,2)  * 100)::number(38,0) * 100000
            +             DATEDIFF(day, DATE '1900-01-01', dm_date)    AS low_packed,
        -- HIGH: MAX picks largest value first; for ties, largest (99999-offset)
        -- = smallest offset = earliest date wins.
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
)
SELECT
    dm_s_symb, dm_date, dm_close, dm_high, dm_low, dm_vol,
    (FLOOR(low_packed_52w  / 100000) / 100)::number(15,2)              AS fiftytwoweeklow,
    DATEADD(day,            MOD(low_packed_52w,  100000), DATE '1900-01-01') AS fiftytwoweeklowdate,
    (FLOOR(high_packed_52w / 100000) / 100)::number(15,2)              AS fiftytwoweekhigh,
    DATEADD(day, 99999 -    MOD(high_packed_52w, 100000), DATE '1900-01-01') AS fiftytwoweekhighdate
FROM windowed;
