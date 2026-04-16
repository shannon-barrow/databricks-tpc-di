-- Databricks notebook source
-- Data Validation: End-to-end pipeline integrity checks.
-- For every source-to-target transition, validates that source records
-- are not silently dropped by INNER JOINs and that surrogate keys are populated.
-- Accounts for Batch1 + Batch2/3 incremental sources where applicable.
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
-- 2. DIMCUSTOMER: CustomerMgmt NEW + Batch2/3 Customer inserts -> DimCustomer
-- =============================================================================

WITH hist_custs AS (
  SELECT count(distinct customerid) as cnt
  FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '_stage.CustomerMgmt')
  WHERE ActionType = 'NEW'
),
inc_custs AS (
  SELECT count(distinct customerid) as cnt
  FROM (
    SELECT try_cast(c_id as BIGINT) as customerid
    FROM read_files(
      "${tpcdi_directory}sf=${scale_factor}/Batch{2,3}",
      format => "csv", inferSchema => False, header => False, sep => "|",
      fileNamePattern => "Customer_[0-9]*.txt",
      schemaEvolutionMode => 'none',
      schema => "cdc_flag STRING, cdc_dsn BIGINT, c_id BIGINT, c_tax_id STRING, c_st_id STRING, c_l_name STRING, c_f_name STRING, c_m_name STRING, c_gndr STRING, c_tier STRING, c_dob STRING, c_adline1 STRING, c_adline2 STRING, c_zipcode STRING, c_city STRING, c_state_prov STRING, c_ctry STRING, c_ctry_1 STRING, c_area_1 STRING, c_local_1 STRING, c_ext_1 STRING, c_ctry_2 STRING, c_area_2 STRING, c_local_2 STRING, c_ext_2 STRING, c_ctry_3 STRING, c_area_3 STRING, c_local_3 STRING, c_ext_3 STRING, c_email_1 STRING, c_email_2 STRING, c_lcl_tx_id STRING, c_nat_tx_id STRING"
    )
    WHERE cdc_flag = 'I'
  )
),
all_custs AS (
  -- Combine historical + incremental distinct customer IDs
  SELECT count(distinct customerid) as cnt FROM (
    SELECT customerid FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '_stage.CustomerMgmt') WHERE ActionType = 'NEW'
    UNION
    SELECT try_cast(c_id as BIGINT) as customerid
    FROM read_files(
      "${tpcdi_directory}sf=${scale_factor}/Batch{2,3}",
      format => "csv", inferSchema => False, header => False, sep => "|",
      fileNamePattern => "Customer_[0-9]*.txt",
      schemaEvolutionMode => 'none',
      schema => "cdc_flag STRING, cdc_dsn BIGINT, c_id BIGINT, c_tax_id STRING, c_st_id STRING, c_l_name STRING, c_f_name STRING, c_m_name STRING, c_gndr STRING, c_tier STRING, c_dob STRING, c_adline1 STRING, c_adline2 STRING, c_zipcode STRING, c_city STRING, c_state_prov STRING, c_ctry STRING, c_ctry_1 STRING, c_area_1 STRING, c_local_1 STRING, c_ext_1 STRING, c_ctry_2 STRING, c_area_2 STRING, c_local_2 STRING, c_ext_2 STRING, c_ctry_3 STRING, c_area_3 STRING, c_local_3 STRING, c_ext_3 STRING, c_email_1 STRING, c_email_2 STRING, c_lcl_tx_id STRING, c_nat_tx_id STRING"
    )
    WHERE cdc_flag = 'I'
  )
)
SELECT
  'DimCustomer distinct customers' as target_table,
  (SELECT cnt FROM all_custs) as source_customers,
  tgt.target_customers,
  CASE WHEN (SELECT cnt FROM all_custs) = tgt.target_customers THEN 'PASS'
       WHEN (SELECT cnt FROM all_custs) <= tgt.target_customers THEN 'WARN (target has ' || (tgt.target_customers - (SELECT cnt FROM all_custs)) || ' extra)'
       ELSE 'FAIL' END as status,
  'Every distinct customer should appear in DimCustomer (exact match expected)' as description
FROM
  (SELECT count(distinct customerid) as target_customers
   FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimCustomer')) tgt;

-- COMMAND ----------

-- =============================================================================
-- 3. DIMACCOUNT: CustomerMgmt + Batch2/3 Account inserts -> DimAccount
-- =============================================================================

-- 3a. Distinct account count (target >= source since SCD2 + incremental)
WITH hist_accts AS (
  SELECT count(distinct accountid) as cnt
  FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '_stage.CustomerMgmt')
  WHERE ActionType IN ('NEW', 'ADDACCT')
),
inc_accts AS (
  SELECT count(distinct accountid) as cnt
  FROM read_files(
    "${tpcdi_directory}sf=${scale_factor}/Batch{2,3}",
    format => "csv", inferSchema => False, header => False, sep => "|",
    fileNamePattern => "Account_[0-9]*.txt",
    schemaEvolutionMode => 'none',
    schema => "cdc_flag STRING, cdc_dsn BIGINT, accountid BIGINT, brokerid BIGINT, customerid BIGINT, accountdesc STRING, taxstatus TINYINT, status STRING"
  )
  WHERE cdc_flag = 'I'
),
all_accts AS (
  SELECT count(distinct accountid) as cnt FROM (
    SELECT accountid FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '_stage.CustomerMgmt') WHERE ActionType IN ('NEW', 'ADDACCT')
    UNION
    SELECT accountid FROM read_files(
      "${tpcdi_directory}sf=${scale_factor}/Batch{2,3}",
      format => "csv", inferSchema => False, header => False, sep => "|",
      fileNamePattern => "Account_[0-9]*.txt",
      schemaEvolutionMode => 'none',
      schema => "cdc_flag STRING, cdc_dsn BIGINT, accountid BIGINT, brokerid BIGINT, customerid BIGINT, accountdesc STRING, taxstatus TINYINT, status STRING"
    )
    WHERE cdc_flag = 'I'
  )
)
SELECT
  'DimAccount distinct accounts' as target_table,
  (SELECT cnt FROM all_accts) as source_accounts,
  tgt.target_accounts,
  CASE WHEN (SELECT cnt FROM all_accts) = tgt.target_accounts THEN 'PASS'
       WHEN (SELECT cnt FROM all_accts) <= tgt.target_accounts THEN 'WARN (target has ' || (tgt.target_accounts - (SELECT cnt FROM all_accts)) || ' extra)'
       ELSE 'FAIL' END as status,
  'Every distinct account should appear in DimAccount (exact match expected)' as description
FROM
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
--    value column has PTS+rectype stripped (starts at position 1 = CompanyName)
--    CIK is at positions 61-70 in the stripped value
-- =============================================================================

SELECT
  'DimCompany distinct companies' as target_table,
  src.source_companies,
  tgt.target_companies,
  CASE WHEN src.source_companies = tgt.target_companies THEN 'PASS' ELSE 'FAIL' END as status,
  'Every distinct CIK from FinWire CMP should appear in DimCompany' as description
FROM
  (SELECT count(distinct try_cast(trim(substring(value, 61, 10)) as BIGINT)) as source_companies
   FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '_stage.FinWire')
   WHERE rectype = 'CMP') src,
  (SELECT count(distinct companyid) as target_companies
   FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimCompany')) tgt;

-- COMMAND ----------

-- =============================================================================
-- 5. DIMSECURITY: FinWire SEC records -> DimSecurity
--    Symbol is at positions 1-15 in the stripped value
-- =============================================================================

SELECT
  'DimSecurity distinct securities' as target_table,
  src.source_securities,
  tgt.target_securities,
  CASE WHEN src.source_securities = tgt.target_securities THEN 'PASS' ELSE 'FAIL' END as status,
  'Every distinct symbol from FinWire SEC should appear in DimSecurity' as description
FROM
  (SELECT count(distinct trim(substring(value, 1, 15))) as source_securities
   FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '_stage.FinWire')
   WHERE rectype = 'SEC') src,
  (SELECT count(distinct symbol) as target_securities
   FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimSecurity')) tgt;

-- COMMAND ----------

-- =============================================================================
-- 6. FINANCIAL: FinWire FIN records -> Financial
--    FIN records are stored as rectype 'FIN_COMPANYID' or 'FIN_NAME'
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
   WHERE rectype LIKE 'FIN%') src,
  (SELECT count(*) as target_financials
   FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.Financial')) tgt;

-- COMMAND ----------

-- =============================================================================
-- 7. DIMTRADE: Trade.txt (all batches) -> DimTrade
--    Counts all distinct tradeids from Batch1 + Batch2/3 inserts
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
    schemaEvolutionMode => 'none',
    schema => "cdc_flag STRING, cdc_dsn BIGINT, tradeid BIGINT, t_dts STRING, status STRING, t_tt_id STRING, cashflag TINYINT, t_s_symb STRING, quantity INT, bidprice DOUBLE, t_ca_id BIGINT, executedby STRING, tradeprice DOUBLE, fee DOUBLE, commission DOUBLE, tax DOUBLE"
  ) WHERE cdc_flag = 'I'
)
SELECT
  'DimTrade' as target_table,
  (SELECT cnt FROM trade_source) + (SELECT cnt FROM trade_inc) as source_trades,
  (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimTrade')) as target_rows,
  CASE WHEN abs((SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimTrade'))
            - ((SELECT cnt FROM trade_source) + (SELECT cnt FROM trade_inc)))
            <= 0.01 * ((SELECT cnt FROM trade_source) + (SELECT cnt FROM trade_inc))
       THEN 'PASS'
       ELSE 'FAIL (delta=' || ((SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimTrade'))
            - ((SELECT cnt FROM trade_source) + (SELECT cnt FROM trade_inc))) || ')'
       END as status,
  'DimTrade should be within 1% of source trades' as description;

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
-- 8. FACTCASHBALANCES: CashTransaction (all batches) -> FactCashBalances
--    Source grain: (accountid, date) after daily aggregation across all batches
-- =============================================================================

WITH all_cash AS (
  SELECT count(*) as cnt FROM (
    SELECT accountid, datevalue FROM (
      SELECT ct_ca_id as accountid, to_date(ct_dts) as datevalue
      FROM read_files(
        "${tpcdi_directory}sf=${scale_factor}/Batch1",
        format => "csv", inferSchema => False, header => False, sep => "|",
        fileNamePattern => "CashTransaction_[0-9]*.txt",
        schemaEvolutionMode => 'none',
        schema => "ct_ca_id BIGINT, ct_dts TIMESTAMP, ct_amt DOUBLE, ct_name STRING"
      )
      UNION ALL
      SELECT accountid, to_date(ct_dts) as datevalue
      FROM read_files(
        "${tpcdi_directory}sf=${scale_factor}/Batch{2,3}",
        format => "csv", inferSchema => False, header => False, sep => "|",
        fileNamePattern => "CashTransaction_[0-9]*.txt",
        schemaEvolutionMode => 'none',
        schema => "cdc_flag STRING, cdc_dsn BIGINT, accountid BIGINT, ct_dts TIMESTAMP, ct_amt DOUBLE, ct_name STRING"
      )
    )
    GROUP BY accountid, datevalue
  )
)
SELECT
  'FactCashBalances' as target_table,
  (SELECT cnt FROM all_cash) as source_daily_acct_rows,
  (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.FactCashBalances')) as target_rows,
  CASE WHEN abs((SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.FactCashBalances'))
            - (SELECT cnt FROM all_cash))
            <= 0.001 * (SELECT cnt FROM all_cash)
       THEN 'PASS'
       ELSE 'FAIL (delta=' || ((SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.FactCashBalances'))
            - (SELECT cnt FROM all_cash)) || ')'
       END as status,
  'FactCashBalances should be within 0.1% of source (accountid,date) pairs' as description;

-- COMMAND ----------

-- =============================================================================
-- 9. FACTHOLDINGS: HoldingHistory (all batches) -> FactHoldings
-- =============================================================================

WITH all_hh AS (
  SELECT count(*) as cnt FROM (
    SELECT *
    FROM read_files(
      "${tpcdi_directory}sf=${scale_factor}/Batch1",
      format => "csv", inferSchema => False, header => False, sep => "|",
      fileNamePattern => "HoldingHistory_[0-9]*.txt",
      schema => "hh_h_t_id BIGINT, hh_t_id BIGINT, hh_before_qty INT, hh_after_qty INT"
    )
    UNION ALL
    SELECT * except(cdc_flag, cdc_dsn)
    FROM read_files(
      "${tpcdi_directory}sf=${scale_factor}/Batch{2,3}",
      format => "csv", inferSchema => False, header => False, sep => "|",
      fileNamePattern => "HoldingHistory_[0-9]*.txt",
      schemaEvolutionMode => 'none',
      schema => "cdc_flag STRING, cdc_dsn BIGINT, hh_h_t_id BIGINT, hh_t_id BIGINT, hh_before_qty INT, hh_after_qty INT"
    )
  )
)
SELECT
  'FactHoldings' as target_table,
  (SELECT cnt FROM all_hh) as source_rows,
  (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.FactHoldings')) as target_rows,
  CASE WHEN abs((SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.FactHoldings'))
            - (SELECT cnt FROM all_hh))
            <= 0.001 * (SELECT cnt FROM all_hh)
       THEN 'PASS'
       ELSE 'FAIL (delta=' || ((SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.FactHoldings'))
            - (SELECT cnt FROM all_hh)) || ')'
       END as status,
  'FactHoldings should be within 0.1% of source rows' as description;

-- COMMAND ----------

-- =============================================================================
-- 10. FACTWATCHES: WatchHistory (all batches) -> FactWatches
--     PK is (sk_customerid, sk_securityid) = distinct (w_c_id, w_s_symb)
-- =============================================================================

WITH watch_all AS (
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
        schemaEvolutionMode => 'none',
        schema => "cdc_flag STRING, cdc_dsn BIGINT, w_c_id BIGINT, w_s_symb STRING, w_dts STRING, w_action STRING"
      )
    )
  )
)
SELECT
  'FactWatches' as target_table,
  (SELECT cnt FROM watch_all) as source_distinct_pairs,
  (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.FactWatches')) as target_rows,
  CASE WHEN (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.FactWatches'))
            BETWEEN (SELECT cnt FROM watch_all) * 0.5 AND (SELECT cnt FROM watch_all)
       THEN 'PASS'
       ELSE 'FAIL (target=' || (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.FactWatches'))
            || ', source_pairs=' || (SELECT cnt FROM watch_all) || ')'
       END as status,
  'FactWatches should be 50-100% of source distinct pairs (some drop from DimCustomer/DimSecurity join)' as description;

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
      schemaEvolutionMode => 'none',
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
--     Target can be > source due to DimSecurity SCD2 splitting rows.
--     Check target >= source (no rows lost).
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
      schemaEvolutionMode => 'none',
      schema => "cdc_flag STRING, cdc_dsn BIGINT, dm_date DATE, dm_s_symb STRING, dm_close DOUBLE, dm_high DOUBLE, dm_low DOUBLE, dm_vol INT"
    )
  )
)
SELECT
  'FactMarketHistory' as target_table,
  (SELECT cnt FROM dm_source) as source_rows,
  (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.FactMarketHistory')) as target_rows,
  CASE WHEN (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.FactMarketHistory'))
            BETWEEN (SELECT cnt FROM dm_source) AND (SELECT cnt FROM dm_source) * 1.05
       THEN 'PASS'
       ELSE 'FAIL (target=' || (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.FactMarketHistory'))
            || ', source=' || (SELECT cnt FROM dm_source)
            || ', delta=' || ((SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.FactMarketHistory')) - (SELECT cnt FROM dm_source)) || ')'
       END as status,
  'FactMarketHistory should be within 0-5% above source (DimSecurity SCD2 can expand rows)' as description;

-- COMMAND ----------

-- =============================================================================
-- 12. PROSPECT: Prospect.csv (all batches) -> Prospect
--     Prospect table retains all distinct agencyids across all batches.
-- =============================================================================

SELECT
  'Prospect' as target_table,
  src.source_rows,
  tgt.target_rows,
  CASE WHEN src.source_rows = tgt.target_rows THEN 'PASS'
       ELSE 'FAIL (source=' || src.source_rows || ', target=' || tgt.target_rows || ')' END as status,
  'Prospect table should exactly match distinct agencyids from latest batch' as description
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

-- COMMAND ----------

-- =============================================================================
-- 14. SCD2 INTEGRITY: EndDate alignment, no overlaps, end-of-time, consolidation
--     Applied to DimCustomer, DimAccount, DimSecurity, DimCompany
-- =============================================================================

SELECT test_name, fail_count,
  CASE WHEN fail_count = 0 THEN 'PASS' ELSE 'FAIL (' || fail_count || ')' END as status
FROM (
  -- EndDate alignment: every record's EndDate matches the next record's EffectiveDate, or is 9999-12-31
  SELECT 'DimCustomer EndDate alignment' as test_name,
    (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimCustomer'))
    - (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimCustomer') a
       JOIN IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimCustomer') b
       ON a.CustomerID = b.CustomerID AND a.EndDate = b.EffectiveDate)
    - (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimCustomer') WHERE EndDate = '9999-12-31')
    as fail_count
  UNION ALL
  SELECT 'DimAccount EndDate alignment',
    (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimAccount'))
    - (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimAccount') a
       JOIN IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimAccount') b
       ON a.AccountID = b.AccountID AND a.EndDate = b.EffectiveDate)
    - (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimAccount') WHERE EndDate = '9999-12-31')
  UNION ALL
  SELECT 'DimSecurity EndDate alignment',
    (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimSecurity'))
    - (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimSecurity') a
       JOIN IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimSecurity') b
       ON a.Symbol = b.Symbol AND a.EndDate = b.EffectiveDate)
    - (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimSecurity') WHERE EndDate = '9999-12-31')
  UNION ALL
  SELECT 'DimCompany EndDate alignment',
    (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimCompany'))
    - (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimCompany') a
       JOIN IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimCompany') b
       ON a.CompanyID = b.CompanyID AND a.EndDate = b.EffectiveDate)
    - (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimCompany') WHERE EndDate = '9999-12-31')

  -- No overlapping date ranges
  UNION ALL
  SELECT 'DimCustomer no overlap',
    (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimCustomer') a
     JOIN IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimCustomer') b
     ON a.CustomerID = b.CustomerID AND a.SK_CustomerID <> b.SK_CustomerID
     AND a.EffectiveDate >= b.EffectiveDate AND a.EffectiveDate < b.EndDate)
  UNION ALL
  SELECT 'DimAccount no overlap',
    (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimAccount') a
     JOIN IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimAccount') b
     ON a.AccountID = b.AccountID AND a.SK_AccountID <> b.SK_AccountID
     AND a.EffectiveDate >= b.EffectiveDate AND a.EffectiveDate < b.EndDate)
  UNION ALL
  SELECT 'DimSecurity no overlap',
    (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimSecurity') a
     JOIN IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimSecurity') b
     ON a.Symbol = b.Symbol AND a.SK_SecurityID <> b.SK_SecurityID
     AND a.EffectiveDate >= b.EffectiveDate AND a.EffectiveDate < b.EndDate)
  UNION ALL
  SELECT 'DimCompany no overlap',
    (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimCompany') a
     JOIN IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimCompany') b
     ON a.CompanyID = b.CompanyID AND a.SK_CompanyID <> b.SK_CompanyID
     AND a.EffectiveDate >= b.EffectiveDate AND a.EffectiveDate < b.EndDate)

  -- End of time: every entity has exactly one record with EndDate = 9999-12-31
  UNION ALL
  SELECT 'DimCustomer end of time',
    (SELECT count(distinct CustomerID) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimCustomer'))
    - (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimCustomer') WHERE EndDate = '9999-12-31')
  UNION ALL
  SELECT 'DimAccount end of time',
    (SELECT count(distinct AccountID) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimAccount'))
    - (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimAccount') WHERE EndDate = '9999-12-31')
  UNION ALL
  SELECT 'DimSecurity end of time',
    (SELECT count(distinct Symbol) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimSecurity'))
    - (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimSecurity') WHERE EndDate = '9999-12-31')
  UNION ALL
  SELECT 'DimCompany end of time',
    (SELECT count(distinct CompanyID) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimCompany'))
    - (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimCompany') WHERE EndDate = '9999-12-31')

  -- Consolidation: no zero-length records (EffectiveDate = EndDate)
  UNION ALL
  SELECT 'DimCustomer no zero-length',
    (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimCustomer') WHERE EffectiveDate = EndDate)
  UNION ALL
  SELECT 'DimAccount no zero-length',
    (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimAccount') WHERE EffectiveDate = EndDate)
  UNION ALL
  SELECT 'DimSecurity no zero-length',
    (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimSecurity') WHERE EffectiveDate = EndDate)
  UNION ALL
  SELECT 'DimCompany no zero-length',
    (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimCompany') WHERE EffectiveDate = EndDate)

  -- IsCurrent consistency: IsCurrent=true iff EndDate = 9999-12-31
  UNION ALL
  SELECT 'DimCustomer IsCurrent',
    (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimCustomer'))
    - (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimCustomer') WHERE EndDate = '9999-12-31' AND IsCurrent)
    - (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimCustomer') WHERE EndDate < '9999-12-31' AND NOT IsCurrent)
  UNION ALL
  SELECT 'DimAccount IsCurrent',
    (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimAccount'))
    - (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimAccount') WHERE EndDate = '9999-12-31' AND IsCurrent)
    - (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimAccount') WHERE EndDate < '9999-12-31' AND NOT IsCurrent)
  UNION ALL
  SELECT 'DimSecurity IsCurrent',
    (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimSecurity'))
    - (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimSecurity') WHERE EndDate = '9999-12-31' AND IsCurrent)
    - (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimSecurity') WHERE EndDate < '9999-12-31' AND NOT IsCurrent)

  -- Distinct surrogate keys
  UNION ALL
  SELECT 'DimCustomer distinct SKs',
    (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimCustomer'))
    - (SELECT count(distinct SK_CustomerID) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimCustomer'))
  UNION ALL
  SELECT 'DimAccount distinct SKs',
    (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimAccount'))
    - (SELECT count(distinct SK_AccountID) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimAccount'))
  UNION ALL
  SELECT 'DimSecurity distinct SKs',
    (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimSecurity'))
    - (SELECT count(distinct SK_SecurityID) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimSecurity'))
  UNION ALL
  SELECT 'DimCompany distinct SKs',
    (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimCompany'))
    - (SELECT count(distinct SK_CompanyID) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimCompany'))
);

-- COMMAND ----------

-- =============================================================================
-- 15. SK DATE RANGE CONTAINMENT: FK surrogate keys map to records with valid date ranges
-- =============================================================================

SELECT test_name, fail_count,
  CASE WHEN fail_count = 0 THEN 'PASS' ELSE 'FAIL (' || fail_count || ')' END as status
FROM (
  -- DimAccount.SK_CustomerID must map to a DimCustomer record whose dates contain the account's dates
  SELECT 'DimAccount SK_CustomerID containment' as test_name,
    (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimAccount'))
    - (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimAccount') a
       JOIN IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimCustomer') c
       ON a.SK_CustomerID = c.SK_CustomerID AND c.EffectiveDate <= a.EffectiveDate AND a.EndDate <= c.EndDate)
    as fail_count
  UNION ALL
  -- DimAccount.SK_BrokerID must map to a valid DimBroker record
  SELECT 'DimAccount SK_BrokerID containment',
    (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimAccount'))
    - (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimAccount') a
       JOIN IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimBroker') b
       ON a.SK_BrokerID = b.SK_BrokerID AND b.EffectiveDate <= a.EffectiveDate AND a.EndDate <= b.EndDate)
  UNION ALL
  -- DimSecurity.SK_CompanyID must map to a DimCompany record whose dates contain the security's dates
  SELECT 'DimSecurity SK_CompanyID containment',
    (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimSecurity'))
    - (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimSecurity') s
       JOIN IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimCompany') c
       ON s.SK_CompanyID = c.SK_CompanyID AND c.EffectiveDate <= s.EffectiveDate AND s.EndDate <= c.EndDate)
);

-- COMMAND ----------

-- =============================================================================
-- 16. DATA QUALITY: Valid values for enumerated fields
-- =============================================================================

SELECT test_name, fail_count,
  CASE WHEN fail_count = 0 THEN 'PASS' ELSE 'FAIL (' || fail_count || ')' END as status
FROM (
  SELECT 'DimCustomer valid Status' as test_name,
    (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimCustomer') WHERE Status NOT IN ('Active', 'Inactive')) as fail_count
  UNION ALL
  SELECT 'DimCustomer valid Gender',
    (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimCustomer') WHERE Gender NOT IN ('M', 'F', 'U'))
  UNION ALL
  SELECT 'DimCustomer TaxID format',
    (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimCustomer') WHERE TaxID NOT LIKE '___-__-____')
  UNION ALL
  SELECT 'DimCustomer Phone1 format',
    (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimCustomer')
     WHERE Phone1 NOT LIKE '+1 (___) ___-____%' AND Phone1 NOT LIKE '(___) ___-____%'
       AND Phone1 NOT LIKE '___-____%' AND Phone1 <> '' AND Phone1 IS NOT NULL)
  UNION ALL
  SELECT 'DimCustomer Email1 format',
    (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimCustomer')
     WHERE Email1 NOT LIKE '_%.%@%.%' AND Email1 IS NOT NULL)
  UNION ALL
  SELECT 'DimAccount valid Status',
    (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimAccount') WHERE Status NOT IN ('Active', 'Inactive'))
  UNION ALL
  SELECT 'DimAccount valid TaxStatus',
    (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimAccount') WHERE BatchID = 1 AND TaxStatus NOT IN (0, 1, 2))
  UNION ALL
  SELECT 'DimSecurity valid Status',
    (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimSecurity') WHERE Status NOT IN ('Active', 'Inactive'))
  UNION ALL
  SELECT 'DimSecurity valid ExchangeID',
    (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimSecurity') WHERE ExchangeID NOT IN ('NYSE', 'NASDAQ', 'AMEX', 'PCX'))
  UNION ALL
  SELECT 'DimSecurity valid Issue',
    (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimSecurity') WHERE Issue NOT IN ('COMMON', 'PREF_A', 'PREF_B', 'PREF_C', 'PREF_D'))
  UNION ALL
  SELECT 'DimCustomer LocalTaxRate valid',
    (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimCustomer'))
    - (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimCustomer') c
       JOIN IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.TaxRate') t
       ON c.LocalTaxRateDesc = t.TX_NAME AND c.LocalTaxRate = t.TX_RATE)
  UNION ALL
  SELECT 'DimCustomer NationalTaxRate valid',
    (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimCustomer'))
    - (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimCustomer') c
       JOIN IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.TaxRate') t
       ON c.NationalTaxRateDesc = t.TX_NAME AND c.NationalTaxRate = t.TX_RATE)
);

-- COMMAND ----------

-- =============================================================================
-- 17. CROSS-TABLE BUSINESS RULES
-- =============================================================================

SELECT test_name, fail_count,
  CASE WHEN fail_count = 0 THEN 'PASS' ELSE 'FAIL (' || fail_count || ')' END as status
FROM (
  -- If a customer is inactive, their accounts should also be inactive
  SELECT 'Inactive customer -> inactive accounts' as test_name,
    (SELECT count(*) FROM (
      SELECT c.SK_CustomerID
      FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimCustomer') c
      LEFT JOIN IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimAccount') a
        ON a.SK_CustomerID = c.SK_CustomerID AND a.Status = 'Inactive'
      WHERE c.Status = 'Inactive'
      GROUP BY c.SK_CustomerID
      HAVING count(a.SK_AccountID) < 1
    )) as fail_count
  UNION ALL
  -- Prospect demographic fields match for current customers with AgencyID
  SELECT 'Prospect demographic match',
    (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimCustomer')
     WHERE AgencyID IS NOT NULL AND IsCurrent)
    - (SELECT count(*) FROM IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.DimCustomer') c
       JOIN IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.Prospect') p
       ON upper(c.FirstName || c.LastName || c.AddressLine1 || coalesce(c.AddressLine2,'') || c.PostalCode)
        = upper(p.FirstName || p.LastName || p.AddressLine1 || coalesce(p.AddressLine2,'') || p.PostalCode)
       AND coalesce(c.CreditRating, 0) = coalesce(p.CreditRating, 0)
       AND coalesce(c.NetWorth, 0) = coalesce(p.NetWorth, 0)
       AND coalesce(c.MarketingNameplate, '') = coalesce(p.MarketingNameplate, '')
       WHERE c.IsCurrent)
);
