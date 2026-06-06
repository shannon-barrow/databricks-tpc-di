-- ============================================================================
-- Cleanup: drop all test DTs + the test source. Idempotent.
-- ============================================================================

USE SCHEMA TPCDI_TEST.SHANNON_AUG_SF_DT_10;

DROP DYNAMIC TABLE IF EXISTS fmhtest_f_minby_sliding_52w;
DROP DYNAMIC TABLE IF EXISTS fmhtest_e_minby_partition;
DROP DYNAMIC TABLE IF EXISTS fmhtest_d_minmax_sliding_52w;
DROP DYNAMIC TABLE IF EXISTS fmhtest_c_minmax_orderby_default_frame;
DROP DYNAMIC TABLE IF EXISTS fmhtest_b_minmax_partition;
DROP DYNAMIC TABLE IF EXISTS fmhtest_a_no_window;
DROP TABLE         IF EXISTS bronzedailymarket_fmhtest;
