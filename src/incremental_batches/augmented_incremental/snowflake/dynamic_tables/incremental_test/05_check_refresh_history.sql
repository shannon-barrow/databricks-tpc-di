-- ============================================================================
-- Step 5: Inspect refresh history for the test DTs.
--
-- For each refresh: wall time, INCREMENTAL vs FULL action, rows
-- inserted/deleted, query_id. The STATISTICS JSON has the most detail
-- (numInsertedRows, numDeletedRows, numScannedBytes, refresh_action).
--
-- INCREMENTAL refresh = REFRESH_ACTION 'INCREMENTAL' AND only the new rows
-- in the numbers. FULL refresh = REFRESH_ACTION 'REINITIALIZE' with the
-- full row count.
--
-- Run a couple of minutes after step 04 — give the scheduler time to fire.
--
-- Run: snow sql -c tpcdi_kp -f 05_check_refresh_history.sql
-- ============================================================================

USE SCHEMA TPCDI_TEST.SHANNON_AUG_SF_DT_10;

-- Per-DT recent refresh actions (after the step-04 insert)
SELECT
    name,
    state,
    refresh_trigger,
    refresh_action,
    DATEDIFF('millisecond', refresh_start_time, refresh_end_time) AS wall_ms,
    refresh_start_time,
    statistics:numInsertedRows::number AS rows_inserted,
    statistics:numDeletedRows::number  AS rows_deleted,
    statistics:executionTimeBreakdownInMillis AS exec_breakdown_ms,
    query_id
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(
    DATA_TIMESTAMP_START => DATEADD('hour', -1, CURRENT_TIMESTAMP())
))
WHERE database_name = 'TPCDI_TEST'
  AND schema_name   = 'SHANNON_AUG_SF_DT_10'
  AND STARTSWITH(name, 'FMHTEST_')
  AND state != 'SKIPPED'
ORDER BY name, refresh_start_time;
