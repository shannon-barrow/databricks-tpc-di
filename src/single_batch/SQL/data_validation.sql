-- Databricks notebook source
-- Data Validation: Verify join integrity across the TPC-DI pipeline.
-- Checks that no rows are silently dropped by INNER JOINs or produce
-- NULL surrogate keys from FULL OUTER JOINs at each stage.
-- Run AFTER the pipeline completes to validate data quality.

-- COMMAND ----------

-- =============================================================================
-- 1. SOURCE DATA COUNTS (raw file row counts from staged data)
-- =============================================================================

SELECT 'Source Row Counts' as section;

-- COMMAND ----------

-- CustomerMgmt action counts by type
SELECT
  'CustomerMgmt' as source,
  ActionType,
  count(*) as row_count
FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '_stage.CustomerMgmt')
GROUP BY ActionType
ORDER BY ActionType;

-- COMMAND ----------

-- CustomerMgmt: accounts from historical batch (Batch 1 account-bearing actions)
SELECT
  'CustomerMgmt account actions' as check_name,
  count(*) as total_account_actions,
  count(distinct accountid) as distinct_accounts,
  count(distinct customerid) as distinct_customers,
  sum(case when ActionType = 'NEW' then 1 else 0 end) as new_count,
  sum(case when ActionType = 'ADDACCT' then 1 else 0 end) as addacct_count,
  sum(case when ActionType = 'UPDACCT' then 1 else 0 end) as updacct_count,
  sum(case when ActionType = 'CLOSEACCT' then 1 else 0 end) as closeacct_count
FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '_stage.CustomerMgmt')
WHERE ActionType NOT IN ('UPDCUST', 'INACT');

-- COMMAND ----------

-- Trade/CashTransaction/HoldingHistory source counts
SELECT 'Trade' as source, count(*) as row_count
FROM read_files(
  "${tpcdi_directory}sf=${scale_factor}/Batch1",
  format => "csv", inferSchema => False, header => False, sep => "|",
  fileNamePattern => "Trade_[0-9]*.txt",
  schema => "t_id BIGINT, t_dts STRING, t_st_id STRING, t_tt_id STRING, t_is_cash STRING, t_s_symb STRING, t_qty INT, t_bid_price DOUBLE, t_ca_id BIGINT, t_exec_name STRING, t_trade_price DOUBLE, t_chrg DOUBLE, t_comm DOUBLE, t_tax DOUBLE"
)
UNION ALL
SELECT 'CashTransaction' as source, count(*) as row_count
FROM read_files(
  "${tpcdi_directory}sf=${scale_factor}/Batch1",
  format => "csv", inferSchema => False, header => False, sep => "|",
  fileNamePattern => "CashTransaction_[0-9]*.txt",
  schema => "ct_ca_id BIGINT, ct_dts STRING, ct_amt DOUBLE, ct_name STRING"
)
UNION ALL
SELECT 'HoldingHistory' as source, count(*) as row_count
FROM read_files(
  "${tpcdi_directory}sf=${scale_factor}/Batch1",
  format => "csv", inferSchema => False, header => False, sep => "|",
  fileNamePattern => "HoldingHistory_[0-9]*.txt",
  schema => "hh_h_t_id BIGINT, hh_t_id BIGINT, hh_before_qty INT, hh_after_qty INT"
)
UNION ALL
SELECT 'WatchHistory' as source, count(*) as row_count
FROM read_files(
  "${tpcdi_directory}sf=${scale_factor}/Batch1",
  format => "csv", inferSchema => False, header => False, sep => "|",
  fileNamePattern => "WatchHistory_[0-9]*.txt",
  schema => "w_c_id BIGINT, w_s_symb STRING, w_dts STRING, w_action STRING"
);

-- COMMAND ----------

-- =============================================================================
-- 2. DIMCUSTOMER INTEGRITY
-- =============================================================================

SELECT 'DimCustomer Checks' as section;

-- COMMAND ----------

-- DimCustomer: row counts and NULL checks
SELECT
  'DimCustomer' as table_name,
  count(*) as total_rows,
  count(distinct customerid) as distinct_customers,
  sum(case when sk_customerid is null then 1 else 0 end) as null_sk_customerid,
  sum(case when customerid is null then 1 else 0 end) as null_customerid,
  sum(case when iscurrent then 1 else 0 end) as current_rows,
  sum(case when effectivedate >= enddate then 1 else 0 end) as invalid_date_ranges
FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimCustomer');

-- COMMAND ----------

-- =============================================================================
-- 3. DIMACCOUNT INTEGRITY (critical: FULL OUTER JOIN with DimCustomer)
-- =============================================================================

SELECT 'DimAccount Checks' as section;

-- COMMAND ----------

-- DimAccount: NULL sk_customerid means the FULL OUTER JOIN with DimCustomer failed
SELECT
  'DimAccount' as table_name,
  count(*) as total_rows,
  count(distinct accountid) as distinct_accounts,
  sum(case when sk_customerid is null then 1 else 0 end) as null_sk_customerid,
  sum(case when sk_brokerid is null then 1 else 0 end) as null_sk_brokerid,
  sum(case when sk_accountid is null then 1 else 0 end) as null_sk_accountid,
  sum(case when iscurrent then 1 else 0 end) as current_rows,
  sum(case when effectivedate >= enddate then 1 else 0 end) as invalid_date_ranges
FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimAccount');

-- COMMAND ----------

-- DimAccount: check which customerids have no matching DimCustomer record
-- (These are the ones causing null sk_customerid)
SELECT
  'DimAccount orphan customers' as check_name,
  a.customerid,
  a.accountid,
  a.effectivedate as acct_effectivedate,
  a.enddate as acct_enddate,
  a.batchid
FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimAccount') a
WHERE a.sk_customerid IS NULL
LIMIT 20;

-- COMMAND ----------

-- =============================================================================
-- 4. DIMTRADE INTEGRITY (INNER JOIN with DimAccount)
-- =============================================================================

SELECT 'DimTrade Checks' as section;

-- COMMAND ----------

-- DimTrade: row count and NULL surrogate key checks
SELECT
  'DimTrade' as table_name,
  count(*) as total_rows,
  sum(case when sk_customerid is null then 1 else 0 end) as null_sk_customerid,
  sum(case when sk_accountid is null then 1 else 0 end) as null_sk_accountid,
  sum(case when sk_brokerid is null then 1 else 0 end) as null_sk_brokerid,
  sum(case when sk_companyid is null then 1 else 0 end) as null_sk_companyid,
  sum(case when sk_securityid is null then 1 else 0 end) as null_sk_securityid
FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimTrade');

-- COMMAND ----------

-- DimTrade: check for trades that couldn't match DimAccount
-- (dropped by INNER JOIN - these would be silently lost)
WITH trade_source AS (
  SELECT t_id, t_ca_id, date(t_dts) as trade_date
  FROM read_files(
    "${tpcdi_directory}sf=${scale_factor}/Batch1",
    format => "csv", inferSchema => False, header => False, sep => "|",
    fileNamePattern => "Trade_[0-9]*.txt",
    schema => "t_id BIGINT, t_dts STRING, t_st_id STRING, t_tt_id STRING, t_is_cash STRING, t_s_symb STRING, t_qty INT, t_bid_price DOUBLE, t_ca_id BIGINT, t_exec_name STRING, t_trade_price DOUBLE, t_chrg DOUBLE, t_comm DOUBLE, t_tax DOUBLE"
  )
)
SELECT
  'Trade->DimAccount join loss' as check_name,
  count(*) as source_trades,
  count(da.accountid) as matched_trades,
  count(*) - count(da.accountid) as unmatched_trades,
  round((count(*) - count(da.accountid)) * 100.0 / count(*), 2) as pct_lost
FROM trade_source ts
LEFT JOIN IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimAccount') da
  ON ts.t_ca_id = da.accountid
  AND ts.trade_date >= da.effectivedate
  AND ts.trade_date < da.enddate;

-- COMMAND ----------

-- =============================================================================
-- 5. FACTCASHBALANCES INTEGRITY (INNER JOIN with DimAccount)
-- =============================================================================

SELECT 'FactCashBalances Checks' as section;

-- COMMAND ----------

-- FactCashBalances: row count and NULL checks
SELECT
  'FactCashBalances' as table_name,
  count(*) as total_rows,
  sum(case when sk_customerid is null then 1 else 0 end) as null_sk_customerid,
  sum(case when sk_accountid is null then 1 else 0 end) as null_sk_accountid
FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.FactCashBalances');

-- COMMAND ----------

-- CashTransaction->DimAccount join loss check
WITH cash_source AS (
  SELECT ct_ca_id, date(ct_dts) as datevalue
  FROM read_files(
    "${tpcdi_directory}sf=${scale_factor}/Batch1",
    format => "csv", inferSchema => False, header => False, sep => "|",
    fileNamePattern => "CashTransaction_[0-9]*.txt",
    schema => "ct_ca_id BIGINT, ct_dts STRING, ct_amt DOUBLE, ct_name STRING"
  )
)
SELECT
  'CashTxn->DimAccount join loss' as check_name,
  count(*) as source_rows,
  count(da.accountid) as matched_rows,
  count(*) - count(da.accountid) as unmatched_rows,
  round((count(*) - count(da.accountid)) * 100.0 / count(*), 2) as pct_lost
FROM cash_source cs
LEFT JOIN IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimAccount') da
  ON cs.ct_ca_id = da.accountid
  AND cs.datevalue >= da.effectivedate
  AND cs.datevalue < da.enddate;

-- COMMAND ----------

-- =============================================================================
-- 6. FACTHOLDINGS INTEGRITY (via DimTrade)
-- =============================================================================

SELECT 'FactHoldings Checks' as section;

-- COMMAND ----------

-- FactHoldings: row count and NULL checks
SELECT
  'FactHoldings' as table_name,
  count(*) as total_rows,
  sum(case when sk_customerid is null then 1 else 0 end) as null_sk_customerid,
  sum(case when sk_accountid is null then 1 else 0 end) as null_sk_accountid
FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.FactHoldings');

-- COMMAND ----------

-- HoldingHistory->DimTrade join loss check
WITH hh_source AS (
  SELECT hh_h_t_id, hh_t_id
  FROM read_files(
    "${tpcdi_directory}sf=${scale_factor}/Batch1",
    format => "csv", inferSchema => False, header => False, sep => "|",
    fileNamePattern => "HoldingHistory_[0-9]*.txt",
    schema => "hh_h_t_id BIGINT, hh_t_id BIGINT, hh_before_qty INT, hh_after_qty INT"
  )
)
SELECT
  'HoldingHistory->DimTrade join loss' as check_name,
  count(*) as source_rows,
  count(dt.tradeid) as matched_rows,
  count(*) - count(dt.tradeid) as unmatched_rows,
  round((count(*) - count(dt.tradeid)) * 100.0 / count(*), 2) as pct_lost
FROM hh_source hh
LEFT JOIN IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimTrade') dt
  ON hh.hh_t_id = dt.tradeid;

-- COMMAND ----------

-- =============================================================================
-- 7. FACTWATCHES INTEGRITY (INNER JOIN with DimCustomer)
-- =============================================================================

SELECT 'FactWatches Checks' as section;

-- COMMAND ----------

-- FactWatches: row count and NULL checks
SELECT
  'FactWatches' as table_name,
  count(*) as total_rows,
  sum(case when sk_customerid is null then 1 else 0 end) as null_sk_customerid,
  sum(case when sk_securityid is null then 1 else 0 end) as null_sk_securityid
FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.FactWatches');

-- COMMAND ----------

-- WatchHistory->DimCustomer join loss check
WITH wh_source AS (
  SELECT w_c_id as customerid, date(w_dts) as dateplaced
  FROM read_files(
    "${tpcdi_directory}sf=${scale_factor}/Batch1",
    format => "csv", inferSchema => False, header => False, sep => "|",
    fileNamePattern => "WatchHistory_[0-9]*.txt",
    schema => "w_c_id BIGINT, w_s_symb STRING, w_dts STRING, w_action STRING"
  )
  WHERE w_action = 'ACTV'
)
SELECT
  'WatchHistory->DimCustomer join loss' as check_name,
  count(*) as source_watches,
  count(dc.customerid) as matched_watches,
  count(*) - count(dc.customerid) as unmatched_watches,
  round((count(*) - count(dc.customerid)) * 100.0 / count(*), 2) as pct_lost
FROM wh_source wh
LEFT JOIN IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimCustomer') dc
  ON wh.customerid = dc.customerid
  AND wh.dateplaced >= dc.effectivedate
  AND wh.dateplaced < dc.enddate;

-- COMMAND ----------

-- =============================================================================
-- 8. CROSS-TABLE REFERENTIAL INTEGRITY SUMMARY
-- =============================================================================

SELECT 'Referential Integrity Summary' as section;

-- COMMAND ----------

-- Accounts in DimAccount that reference customers not in DimCustomer
SELECT
  'DimAccount->DimCustomer orphans' as check_name,
  count(distinct a.customerid) as orphan_customerids
FROM (SELECT distinct customerid FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimAccount')) a
LEFT JOIN (SELECT distinct customerid FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimCustomer')) c
  ON a.customerid = c.customerid
WHERE c.customerid IS NULL;

-- COMMAND ----------

-- Trades referencing accounts not in DimAccount (using current accounts)
SELECT
  'DimTrade->DimAccount orphans' as check_name,
  count(distinct t.sk_accountid) as orphan_sk_accountids
FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimTrade') t
LEFT JOIN IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimAccount') a
  ON t.sk_accountid = a.sk_accountid
WHERE a.sk_accountid IS NULL;

-- COMMAND ----------

-- =============================================================================
-- 9. FINAL VALIDATION SUMMARY
-- =============================================================================

SELECT
  test_name,
  CASE WHEN result = 0 THEN 'PASS' ELSE 'FAIL (' || result || ')' END as status
FROM (
  SELECT 'DimAccount null sk_customerid' as test_name,
    (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimAccount') WHERE sk_customerid IS NULL) as result
  UNION ALL
  SELECT 'DimAccount null sk_brokerid',
    (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimAccount') WHERE sk_brokerid IS NULL)
  UNION ALL
  SELECT 'DimTrade null sk_customerid',
    (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimTrade') WHERE sk_customerid IS NULL)
  UNION ALL
  SELECT 'DimTrade null sk_accountid',
    (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimTrade') WHERE sk_accountid IS NULL)
  UNION ALL
  SELECT 'FactCashBalances null sk_customerid',
    (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.FactCashBalances') WHERE sk_customerid IS NULL)
  UNION ALL
  SELECT 'FactCashBalances null sk_accountid',
    (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.FactCashBalances') WHERE sk_accountid IS NULL)
  UNION ALL
  SELECT 'FactHoldings null sk_customerid',
    (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.FactHoldings') WHERE sk_customerid IS NULL)
  UNION ALL
  SELECT 'FactHoldings null sk_accountid',
    (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.FactHoldings') WHERE sk_accountid IS NULL)
  UNION ALL
  SELECT 'FactWatches null sk_customerid',
    (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.FactWatches') WHERE sk_customerid IS NULL)
  UNION ALL
  SELECT 'DimAccount->DimCustomer orphan customerids',
    (SELECT count(*) FROM (
      SELECT distinct a.customerid FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimAccount') a
      LEFT JOIN (SELECT distinct customerid FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimCustomer')) c ON a.customerid = c.customerid
      WHERE c.customerid IS NULL
    ))
);
