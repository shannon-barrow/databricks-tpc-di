-- ============================================================================
-- Step 3: Inspect what Snowflake CHOSE for each test DT.
--
-- INFORMATION_SCHEMA.DYNAMIC_TABLES exposes REFRESH_MODE and
-- REFRESH_MODE_REASON — the latter explains *why* Snowflake fell back to
-- FULL when REFRESH_MODE = AUTO was requested.
--
-- Run AFTER step 02 completes. INITIALIZE = ON_CREATE keeps each CREATE
-- statement blocking on the initial backfill, so by the time step 02 has
-- returned, the columns below are stable.
--
-- Run: snow sql -c tpcdi_kp -f 03_check_refresh_modes.sql
-- ============================================================================

USE SCHEMA TPCDI_TEST.SHANNON_AUG_SF_DT_10;

SELECT
    name,
    refresh_mode,
    refresh_mode_reason
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLES())
WHERE database_name = 'TPCDI_TEST'
  AND schema_name   = 'SHANNON_AUG_SF_DT_10'
  AND STARTSWITH(name, 'FMHTEST_')
ORDER BY name;
