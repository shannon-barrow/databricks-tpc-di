-- Databricks notebook source
-- Data Validation: End-to-end pipeline integrity checks.
-- For every source-to-target transition, validates that source records
-- are not silently dropped by INNER JOINs and that surrogate keys are populated.
-- Run AFTER the pipeline completes.

-- COMMAND ----------

-- =============================================================================
-- 1. DIMBROKER: HR.csv brokers -> DimBroker
-- =============================================================================

SELECT
  'DimBroker' as target_table,
  src.source_brokers,
  tgt.target_rows,
  CASE WHEN src.source_brokers = tgt.target_rows THEN 'PASS' ELSE 'FAIL' END as status,
  'Every broker in HR.csv (jobcode=314) should appear in DimBroker' as description
FROM
  (SELECT count(*) as source_brokers
   FROM read_files(
     "${tpcdi_directory}sf=${scale_factor}/Batch1",
     format => "csv", inferSchema => False, header => False, sep => ",",
     fileNamePattern => "HR_[0-9]*.csv",
     schema => "employeeid STRING, managerid STRING, employeefirstname STRING, employeelastname STRING, employeemi STRING, employeejobcode STRING, employeebranch STRING, employeeoffice STRING, employeephone STRING"
   ) WHERE employeejobcode = '314') src,
  (SELECT count(*) as target_rows FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimBroker')) tgt;

-- COMMAND ----------

-- =============================================================================
-- 2. DIMCUSTOMER: CustomerMgmt NEW/UPDCUST/INACT -> DimCustomer
-- =============================================================================

SELECT
  'DimCustomer distinct customers' as target_table,
  src.source_customers,
  tgt.target_customers,
  CASE WHEN src.source_customers = tgt.target_customers THEN 'PASS' ELSE 'FAIL' END as status,
  'Every customer with a NEW action should appear in DimCustomer' as description
FROM
  (SELECT count(distinct customerid) as source_customers
   FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '_stage.CustomerMgmt')
   WHERE ActionType = 'NEW') src,
  (SELECT count(distinct customerid) as target_customers
   FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimCustomer')) tgt;

-- COMMAND ----------

-- =============================================================================
-- 3. DIMACCOUNT: CustomerMgmt account actions -> DimAccount
-- =============================================================================

-- 3a. Every distinct accountid from account-bearing actions should be in DimAccount
SELECT
  'DimAccount distinct accounts' as target_table,
  src.source_accounts,
  tgt.target_accounts,
  CASE WHEN src.source_accounts = tgt.target_accounts THEN 'PASS' ELSE 'FAIL' END as status,
  'Every accountid from NEW/ADDACCT should appear in DimAccount' as description
FROM
  (SELECT count(distinct accountid) as source_accounts
   FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '_stage.CustomerMgmt')
   WHERE ActionType IN ('NEW', 'ADDACCT')) src,
  (SELECT count(distinct accountid) as target_accounts
   FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimAccount')) tgt;

-- COMMAND ----------

-- 3b. No NULL surrogate keys in DimAccount
SELECT
  'DimAccount NULL checks' as target_table,
  sum(case when sk_customerid is null then 1 else 0 end) as null_sk_customerid,
  sum(case when sk_brokerid is null then 1 else 0 end) as null_sk_brokerid,
  CASE WHEN sum(case when sk_customerid is null then 1 else 0 end) = 0
        AND sum(case when sk_brokerid is null then 1 else 0 end) = 0
       THEN 'PASS' ELSE 'FAIL' END as status,
  'No NULL surrogate keys allowed (FULL OUTER JOIN with DimCustomer must always match)' as description
FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimAccount');

-- COMMAND ----------

-- =============================================================================
-- 4. DIMCOMPANY: FinWire CMP records -> DimCompany
-- =============================================================================

SELECT
  'DimCompany distinct companies' as target_table,
  src.source_companies,
  tgt.target_companies,
  CASE WHEN src.source_companies = tgt.target_companies THEN 'PASS' ELSE 'FAIL' END as status,
  'Every distinct CIK from FinWire CMP should appear in DimCompany' as description
FROM
  (SELECT count(distinct try_cast(substring(value, 19, 10) as BIGINT)) as source_companies
   FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '_stage.FinWire')
   WHERE rectype = 'CMP') src,
  (SELECT count(distinct companyid) as target_companies
   FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimCompany')) tgt;

-- COMMAND ----------

-- =============================================================================
-- 5. DIMSECURITY: FinWire SEC records -> DimSecurity
-- =============================================================================

SELECT
  'DimSecurity distinct securities' as target_table,
  src.source_securities,
  tgt.target_securities,
  CASE WHEN src.source_securities = tgt.target_securities THEN 'PASS' ELSE 'FAIL' END as status,
  'Every distinct symbol from FinWire SEC should appear in DimSecurity' as description
FROM
  (SELECT count(distinct trim(substring(value, 19, 15))) as source_securities
   FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '_stage.FinWire')
   WHERE rectype = 'SEC') src,
  (SELECT count(distinct symbol) as target_securities
   FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimSecurity')) tgt;

-- COMMAND ----------

-- =============================================================================
-- 6. FINANCIAL: FinWire FIN records -> Financial
-- =============================================================================

SELECT
  'Financial' as target_table,
  src.source_financials,
  tgt.target_financials,
  CASE WHEN src.source_financials = tgt.target_financials THEN 'PASS' ELSE 'FAIL' END as status,
  'Every FinWire FIN record should produce a Financial row (via DimCompany join)' as description
FROM
  (SELECT count(*) as source_financials
   FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '_stage.FinWire')
   WHERE rectype = 'FIN') src,
  (SELECT count(*) as target_financials
   FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.Financial')) tgt;

-- COMMAND ----------

-- =============================================================================
-- 7. DIMTRADE: Trade.txt + TradeHistory.txt -> DimTrade
--    Every distinct tradeid from Batch1 Trade + incremental Trade inserts
--    should appear in DimTrade (joined through DimAccount and DimSecurity)
-- =============================================================================

WITH trade_source AS (
  SELECT count(distinct t_id) as cnt
  FROM read_files(
    "${tpcdi_directory}sf=${scale_factor}/Batch1",
    format => "csv", inferSchema => False, header => False, sep => "|",
    fileNamePattern => "Trade_[0-9]*.txt",
    schema => "t_id BIGINT, t_dts STRING, t_st_id STRING, t_tt_id STRING, t_is_cash STRING, t_s_symb STRING, t_qty INT, t_bid_price DOUBLE, t_ca_id BIGINT, t_exec_name STRING, t_trade_price DOUBLE, t_chrg DOUBLE, t_comm DOUBLE, t_tax DOUBLE"
  )
),
trade_inc AS (
  SELECT count(distinct tradeid) as cnt
  FROM read_files(
    "${tpcdi_directory}sf=${scale_factor}/Batch{2,3}",
    format => "csv", inferSchema => False, header => False, sep => "|",
    fileNamePattern => "Trade_[0-9]*.txt",
    schema => "cdc_flag STRING, cdc_dsn BIGINT, tradeid BIGINT, t_dts STRING, status STRING, t_tt_id STRING, cashflag TINYINT, t_s_symb STRING, quantity INT, bidprice DOUBLE, t_ca_id BIGINT, executedby STRING, tradeprice DOUBLE, fee DOUBLE, commission DOUBLE, tax DOUBLE"
  ) WHERE cdc_flag = 'I'
)
SELECT
  'DimTrade' as target_table,
  (SELECT cnt FROM trade_source) + (SELECT cnt FROM trade_inc) as source_trades,
  (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimTrade')) as target_rows,
  CASE WHEN (SELECT cnt FROM trade_source) + (SELECT cnt FROM trade_inc) =
            (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimTrade'))
       THEN 'PASS' ELSE 'FAIL' END as status,
  'Every trade (Batch1 + incremental inserts) should appear in DimTrade via DimAccount+DimSecurity joins' as description;

-- COMMAND ----------

-- DimTrade: identify which source trades were lost in the join
WITH batch1_trades AS (
  SELECT t_id as tradeid, t_ca_id, t_s_symb, date(t_dts) as trade_date
  FROM read_files(
    "${tpcdi_directory}sf=${scale_factor}/Batch1",
    format => "csv", inferSchema => False, header => False, sep => "|",
    fileNamePattern => "Trade_[0-9]*.txt",
    schema => "t_id BIGINT, t_dts STRING, t_st_id STRING, t_tt_id STRING, t_is_cash STRING, t_s_symb STRING, t_qty INT, t_bid_price DOUBLE, t_ca_id BIGINT, t_exec_name STRING, t_trade_price DOUBLE, t_chrg DOUBLE, t_comm DOUBLE, t_tax DOUBLE"
  )
),
missing_acct AS (
  SELECT count(*) cnt FROM batch1_trades t
  LEFT JOIN IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimAccount') da
    ON t.t_ca_id = da.accountid AND t.trade_date >= da.effectivedate AND t.trade_date < da.enddate
  WHERE da.accountid IS NULL
),
missing_sec AS (
  SELECT count(*) cnt FROM batch1_trades t
  LEFT JOIN IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimSecurity') ds
    ON t.t_s_symb = ds.symbol AND t.trade_date >= ds.effectivedate AND t.trade_date < ds.enddate
  WHERE ds.symbol IS NULL
)
SELECT
  'DimTrade join loss detail' as check_name,
  (SELECT cnt FROM missing_acct) as trades_missing_account,
  (SELECT cnt FROM missing_sec) as trades_missing_security,
  'Trades that cannot match DimAccount or DimSecurity (dropped by INNER JOIN)' as description;

-- COMMAND ----------

-- =============================================================================
-- 8. FACTCASHBALANCES: CashTransaction -> FactCashBalances (via DimAccount)
--    Source grain: (accountid, date) after daily aggregation
-- =============================================================================

WITH cash_source AS (
  SELECT count(*) as cnt FROM (
    SELECT ct_ca_id, to_date(ct_dts) as datevalue
    FROM read_files(
      "${tpcdi_directory}sf=${scale_factor}/Batch1",
      format => "csv", inferSchema => False, header => False, sep => "|",
      fileNamePattern => "CashTransaction_[0-9]*.txt",
      schema => "ct_ca_id BIGINT, ct_dts STRING, ct_amt DOUBLE, ct_name STRING"
    )
    GROUP BY ct_ca_id, to_date(ct_dts)
  )
),
cash_inc AS (
  SELECT count(*) as cnt FROM (
    SELECT accountid, to_date(ct_dts) as datevalue
    FROM read_files(
      "${tpcdi_directory}sf=${scale_factor}/Batch{2,3}",
      format => "csv", inferSchema => False, header => False, sep => "|",
      fileNamePattern => "CashTransaction_[0-9]*.txt",
      schema => "cdc_flag STRING, cdc_dsn BIGINT, accountid BIGINT, ct_dts STRING, ct_amt DOUBLE, ct_name STRING"
    )
    GROUP BY accountid, to_date(ct_dts)
  )
)
SELECT
  'FactCashBalances' as target_table,
  (SELECT cnt FROM cash_source) + (SELECT cnt FROM cash_inc) as source_daily_acct_rows,
  (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.FactCashBalances')) as target_rows,
  CASE WHEN (SELECT cnt FROM cash_source) + (SELECT cnt FROM cash_inc) =
            (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.FactCashBalances'))
       THEN 'PASS' ELSE 'FAIL' END as status,
  'Every (accountid, date) from CashTransaction should appear in FactCashBalances via DimAccount join' as description;

-- COMMAND ----------

-- =============================================================================
-- 9. FACTHOLDINGS: HoldingHistory -> FactHoldings (via DimTrade)
-- =============================================================================

WITH hh_source AS (
  SELECT count(*) as cnt
  FROM read_files(
    "${tpcdi_directory}sf=${scale_factor}/Batch1",
    format => "csv", inferSchema => False, header => False, sep => "|",
    fileNamePattern => "HoldingHistory_[0-9]*.txt",
    schema => "hh_h_t_id BIGINT, hh_t_id BIGINT, hh_before_qty INT, hh_after_qty INT"
  )
),
hh_inc AS (
  SELECT count(*) as cnt
  FROM read_files(
    "${tpcdi_directory}sf=${scale_factor}/Batch{2,3}",
    format => "csv", inferSchema => False, header => False, sep => "|",
    fileNamePattern => "HoldingHistory_[0-9]*.txt",
    schema => "cdc_flag STRING, cdc_dsn BIGINT, hh_h_t_id BIGINT, hh_t_id BIGINT, hh_before_qty INT, hh_after_qty INT"
  )
)
SELECT
  'FactHoldings' as target_table,
  (SELECT cnt FROM hh_source) + (SELECT cnt FROM hh_inc) as source_rows,
  (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.FactHoldings')) as target_rows,
  CASE WHEN (SELECT cnt FROM hh_source) + (SELECT cnt FROM hh_inc) =
            (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.FactHoldings'))
       THEN 'PASS' ELSE 'FAIL' END as status,
  'Every HoldingHistory row should appear in FactHoldings via DimTrade join (tradeid = hh_t_id)' as description;

-- COMMAND ----------

-- =============================================================================
-- 10. FACTWATCHES: WatchHistory -> FactWatches (via DimCustomer + DimSecurity)
--     PK is (sk_customerid, sk_securityid) which maps to distinct (w_c_id, w_s_symb)
-- =============================================================================

WITH watch_source AS (
  SELECT count(*) as cnt FROM (
    SELECT DISTINCT w_c_id, w_s_symb
    FROM read_files(
      "${tpcdi_directory}sf=${scale_factor}/Batch1",
      format => "csv", inferSchema => False, header => False, sep => "|",
      fileNamePattern => "WatchHistory_[0-9]*.txt",
      schema => "w_c_id BIGINT, w_s_symb STRING, w_dts STRING, w_action STRING"
    )
  )
),
watch_inc AS (
  SELECT count(*) as cnt FROM (
    SELECT DISTINCT w_c_id, w_s_symb
    FROM read_files(
      "${tpcdi_directory}sf=${scale_factor}/Batch{2,3}",
      format => "csv", inferSchema => False, header => False, sep => "|",
      fileNamePattern => "WatchHistory_[0-9]*.txt",
      schema => "cdc_flag STRING, cdc_dsn BIGINT, w_c_id BIGINT, w_s_symb STRING, w_dts STRING, w_action STRING"
    )
  )
),
-- Combined distinct pairs across all batches (some Batch2/3 pairs may overlap with Batch1)
watch_all AS (
  SELECT count(*) as cnt FROM (
    SELECT DISTINCT w_c_id, w_s_symb
    FROM (
      SELECT w_c_id, w_s_symb
      FROM read_files(
        "${tpcdi_directory}sf=${scale_factor}/Batch1",
        format => "csv", inferSchema => False, header => False, sep => "|",
        fileNamePattern => "WatchHistory_[0-9]*.txt",
        schema => "w_c_id BIGINT, w_s_symb STRING, w_dts STRING, w_action STRING"
      )
      UNION ALL
      SELECT w_c_id, w_s_symb
      FROM read_files(
        "${tpcdi_directory}sf=${scale_factor}/Batch{2,3}",
        format => "csv", inferSchema => False, header => False, sep => "|",
        fileNamePattern => "WatchHistory_[0-9]*.txt",
        schema => "cdc_flag STRING, cdc_dsn BIGINT, w_c_id BIGINT, w_s_symb STRING, w_dts STRING, w_action STRING"
      )
    )
  )
)
SELECT
  'FactWatches' as target_table,
  (SELECT cnt FROM watch_all) as source_distinct_pairs,
  (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.FactWatches')) as target_rows,
  CASE WHEN (SELECT cnt FROM watch_all) =
            (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.FactWatches'))
       THEN 'PASS' ELSE 'FAIL' END as status,
  'Every distinct (w_c_id, w_s_symb) pair should appear in FactWatches via DimCustomer+DimSecurity joins' as description;

-- COMMAND ----------

-- FactWatches join loss detail
WITH watches_agg AS (
  SELECT
    w_c_id customerid,
    w_s_symb symbol,
    date(min(w_dts)) dateplaced
  FROM (
    SELECT w_c_id, w_s_symb, w_dts
    FROM read_files(
      "${tpcdi_directory}sf=${scale_factor}/Batch1",
      format => "csv", inferSchema => False, header => False, sep => "|",
      fileNamePattern => "WatchHistory_[0-9]*.txt",
      schema => "w_c_id BIGINT, w_s_symb STRING, w_dts STRING, w_action STRING"
    )
    UNION ALL
    SELECT w_c_id, w_s_symb, w_dts
    FROM read_files(
      "${tpcdi_directory}sf=${scale_factor}/Batch{2,3}",
      format => "csv", inferSchema => False, header => False, sep => "|",
      fileNamePattern => "WatchHistory_[0-9]*.txt",
      schema => "cdc_flag STRING, cdc_dsn BIGINT, w_c_id BIGINT, w_s_symb STRING, w_dts STRING, w_action STRING"
    )
  )
  GROUP BY w_c_id, w_s_symb
),
missing_cust AS (
  SELECT count(*) cnt FROM watches_agg w
  LEFT JOIN IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimCustomer') dc
    ON w.customerid = dc.customerid AND w.dateplaced >= dc.effectivedate AND w.dateplaced < dc.enddate
  WHERE dc.customerid IS NULL
),
missing_sec AS (
  SELECT count(*) cnt FROM watches_agg w
  LEFT JOIN IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimSecurity') ds
    ON w.symbol = ds.symbol AND w.dateplaced >= ds.effectivedate AND w.dateplaced < ds.enddate
  WHERE ds.symbol IS NULL
)
SELECT
  'FactWatches join loss detail' as check_name,
  (SELECT cnt FROM missing_cust) as watches_missing_customer,
  (SELECT cnt FROM missing_sec) as watches_missing_security,
  'Watch pairs that cannot match DimCustomer or DimSecurity (dropped by INNER JOIN)' as description;

-- COMMAND ----------

-- =============================================================================
-- 11. FACTMARKETHISTORY: DailyMarket -> FactMarketHistory (via DimSecurity)
-- =============================================================================

WITH dm_source AS (
  SELECT count(*) as cnt FROM (
    SELECT dm_date, dm_s_symb
    FROM read_files(
      "${tpcdi_directory}sf=${scale_factor}/Batch1",
      format => "csv", inferSchema => False, header => False, sep => "|",
      fileNamePattern => "DailyMarket_[0-9]*.txt",
      schema => "dm_date DATE, dm_s_symb STRING, dm_close DOUBLE, dm_high DOUBLE, dm_low DOUBLE, dm_vol INT"
    )
    UNION ALL
    SELECT dm_date, dm_s_symb
    FROM read_files(
      "${tpcdi_directory}sf=${scale_factor}/Batch{2,3}",
      format => "csv", inferSchema => False, header => False, sep => "|",
      fileNamePattern => "DailyMarket_[0-9]*.txt",
      schema => "cdc_flag STRING, cdc_dsn BIGINT, dm_date DATE, dm_s_symb STRING, dm_close DOUBLE, dm_high DOUBLE, dm_low DOUBLE, dm_vol INT"
    )
  )
)
SELECT
  'FactMarketHistory' as target_table,
  (SELECT cnt FROM dm_source) as source_rows,
  (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.FactMarketHistory')) as target_rows,
  CASE WHEN (SELECT cnt FROM dm_source) =
            (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.FactMarketHistory'))
       THEN 'PASS' ELSE 'FAIL' END as status,
  'Every DailyMarket row should appear in FactMarketHistory via DimSecurity join' as description;

-- COMMAND ----------

-- =============================================================================
-- 12. PROSPECT: Prospect.csv -> Prospect
-- =============================================================================

SELECT
  'Prospect' as target_table,
  src.source_rows,
  tgt.target_rows,
  CASE WHEN src.source_rows = tgt.target_rows THEN 'PASS' ELSE 'FAIL' END as status,
  'Every distinct agencyid from latest batch Prospect file should appear in Prospect' as description
FROM
  (SELECT count(distinct agencyid) as source_rows
   FROM read_files(
     "${tpcdi_directory}sf=${scale_factor}/Batch3",
     format => "csv", inferSchema => False, header => False, sep => ",",
     fileNamePattern => "Prospect_[0-9]*.csv",
     schema => "agencyid STRING, lastname STRING, firstname STRING, middleinitial STRING, gender STRING, addressline1 STRING, addressline2 STRING, postalcode STRING, city STRING, state STRING, country STRING, phone STRING, income STRING, numbercars INT, numberchildren INT, maritalstatus STRING, age INT, creditrating INT, ownorrentflag STRING, employer STRING, numbercreditcards INT, networth INT"
   )) src,
  (SELECT count(*) as target_rows
   FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.Prospect')) tgt;

-- COMMAND ----------

-- =============================================================================
-- 13. SUMMARY: All surrogate key NULL checks across fact/dimension tables
-- =============================================================================

SELECT
  test_name,
  fail_count,
  CASE WHEN fail_count = 0 THEN 'PASS' ELSE 'FAIL (' || fail_count || ')' END as status
FROM (
  SELECT 'DimAccount.sk_customerid' as test_name,
    (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimAccount') WHERE sk_customerid IS NULL) as fail_count
  UNION ALL
  SELECT 'DimAccount.sk_brokerid',
    (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimAccount') WHERE sk_brokerid IS NULL)
  UNION ALL
  SELECT 'DimTrade.sk_customerid',
    (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimTrade') WHERE sk_customerid IS NULL)
  UNION ALL
  SELECT 'DimTrade.sk_accountid',
    (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimTrade') WHERE sk_accountid IS NULL)
  UNION ALL
  SELECT 'DimTrade.sk_securityid',
    (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimTrade') WHERE sk_securityid IS NULL)
  UNION ALL
  SELECT 'DimTrade.sk_companyid',
    (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimTrade') WHERE sk_companyid IS NULL)
  UNION ALL
  SELECT 'FactCashBalances.sk_customerid',
    (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.FactCashBalances') WHERE sk_customerid IS NULL)
  UNION ALL
  SELECT 'FactCashBalances.sk_accountid',
    (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.FactCashBalances') WHERE sk_accountid IS NULL)
  UNION ALL
  SELECT 'FactHoldings.sk_customerid',
    (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.FactHoldings') WHERE sk_customerid IS NULL)
  UNION ALL
  SELECT 'FactHoldings.sk_accountid',
    (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.FactHoldings') WHERE sk_accountid IS NULL)
  UNION ALL
  SELECT 'FactWatches.sk_customerid',
    (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.FactWatches') WHERE sk_customerid IS NULL)
  UNION ALL
  SELECT 'FactWatches.sk_securityid',
    (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.FactWatches') WHERE sk_securityid IS NULL)
  UNION ALL
  SELECT 'FactMarketHistory.sk_securityid',
    (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.FactMarketHistory') WHERE sk_securityid IS NULL)
);
