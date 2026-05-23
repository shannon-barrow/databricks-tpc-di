-- ============================================================================
-- TPC-DI Augmented Incremental — Snowflake Dynamic Tables variant
--
-- ONE-SHOT DDL: 16 dynamic-table definitions covering the same model surface
-- as the dbt-Snowflake variant under ../dbt/snowflake_models/, expressed as
-- pure CREATE OR REPLACE DYNAMIC TABLE statements with NO dbt in the loop.
--
-- Placeholder substitution (Python str.format) handled by setup_sf_dt.py:
--   {catalog}         Snowflake database (e.g. TPCDI_TEST)
--   {schema}          {wh_db}_{sf}                 — run schema (CLONE + DTs land here)
--   {staging_schema}  STAGING_SF{sf}                — clone source (read-only)
--   {warehouse}       DT refresh warehouse          — e.g. BARROW_DT_MED
--   {target_lag}      DT leaf TARGET_LAG            — e.g. '1 minute'
--
-- DAG (refresh order — Snowflake infers from query references):
--
--   bronze_raw tables  (regular tables, populated per-batch via COPY INTO by seed_raw.py)
--      │
--      ▼
--   bronze* DTs        (1:1 SELECT, REFRESH_MODE=INCREMENTAL)
--      │
--      ▼
--   account_updates_from_customer DT
--      │
--      ▼
--   dimcustomer / dimaccount  (SCD2 via QUALIFY ROW_NUMBER)
--      │
--      ▼
--   dimtrade         (SCD2 with close_ts derivation)
--      │
--      ▼
--   factwatches / factcashbalances / factholdings / factmarkethistory /
--   currentaccountbalances  (leaf golds — concrete TARGET_LAG)
--
-- Intermediate DTs use TARGET_LAG=DOWNSTREAM so Snowflake only refreshes them
-- when the leaf golds need fresh data. Leaves carry the concrete lag.
--
-- All clone-target tables (taxrate, dimdate, industry, tradetype, dimbroker,
-- dimsecurity, statustype, dimcompany, dimtime, financial, companyyeareps,
-- batchdate, cashtransactionhistorical) are CLONEd as REGULAR tables by
-- setup_sf_dt.py before this DDL runs — they don't change per batch so we
-- don't need DT semantics on them.
-- ============================================================================


-- ============================================================================
-- BRONZE — 1:1 mirror of bronze_raw tables. Append-only path; QUALIFY dedups
-- defensively on cdc_dsn so a batch-replay (same {batch_date} re-COPY-INTO)
-- doesn't double-count. cdc_dsn is the TPC-DI per-event change sequence — it's
-- globally unique across the lifetime of the dataset.
-- ============================================================================

CREATE OR REPLACE DYNAMIC TABLE {catalog}.{schema}.bronzeaccount
  TARGET_LAG   = DOWNSTREAM
  WAREHOUSE    = {warehouse}
  REFRESH_MODE = INCREMENTAL
AS
SELECT * FROM {catalog}.{schema}.bronzeaccount_raw
QUALIFY ROW_NUMBER() OVER (PARTITION BY cdc_dsn ORDER BY update_dt DESC) = 1;

CREATE OR REPLACE DYNAMIC TABLE {catalog}.{schema}.bronzecustomer
  TARGET_LAG   = DOWNSTREAM
  WAREHOUSE    = {warehouse}
  REFRESH_MODE = INCREMENTAL
AS
SELECT * FROM {catalog}.{schema}.bronzecustomer_raw
QUALIFY ROW_NUMBER() OVER (PARTITION BY cdc_dsn ORDER BY update_dt DESC) = 1;

CREATE OR REPLACE DYNAMIC TABLE {catalog}.{schema}.bronzecashtransaction
  TARGET_LAG   = DOWNSTREAM
  WAREHOUSE    = {warehouse}
  REFRESH_MODE = INCREMENTAL
AS
SELECT * FROM {catalog}.{schema}.bronzecashtransaction_raw
QUALIFY ROW_NUMBER() OVER (PARTITION BY cdc_dsn ORDER BY event_dt DESC) = 1;

CREATE OR REPLACE DYNAMIC TABLE {catalog}.{schema}.bronzeholdings
  TARGET_LAG   = DOWNSTREAM
  WAREHOUSE    = {warehouse}
  REFRESH_MODE = INCREMENTAL
AS
SELECT * FROM {catalog}.{schema}.bronzeholdings_raw
QUALIFY ROW_NUMBER() OVER (PARTITION BY cdc_dsn ORDER BY event_dt DESC) = 1;

CREATE OR REPLACE DYNAMIC TABLE {catalog}.{schema}.bronzetrade
  TARGET_LAG   = DOWNSTREAM
  WAREHOUSE    = {warehouse}
  REFRESH_MODE = INCREMENTAL
AS
SELECT * FROM {catalog}.{schema}.bronzetrade_raw
QUALIFY ROW_NUMBER() OVER (PARTITION BY cdc_dsn ORDER BY event_dt DESC) = 1;

CREATE OR REPLACE DYNAMIC TABLE {catalog}.{schema}.bronzewatches
  TARGET_LAG   = DOWNSTREAM
  WAREHOUSE    = {warehouse}
  REFRESH_MODE = INCREMENTAL
AS
SELECT * FROM {catalog}.{schema}.bronzewatches_raw
QUALIFY ROW_NUMBER() OVER (PARTITION BY cdc_dsn ORDER BY event_dt DESC) = 1;

-- bronzedailymarket: in the dbt variant this is read from the stage too, but
-- the staging clone already contains the historical bronze. For DT variant,
-- bronzedailymarket is a *regular* CLONE'd table (see setup_sf_dt.py — it's
-- in CLONE_TABLES, not BRONZE_RAW_TABLES) so we don't need a DT for it. The
-- per-batch DailyMarket.txt rows land via append into the cloned table by
-- seed_raw.py.


-- ============================================================================
-- BRONZE-tier derived — account_updates_from_customer
--
-- dbt model: account_updates_from_customer.sql
-- Source: per-batch derived from bronzecustomer.update_dt=batch_date rows.
--
-- DT formulation — REWRITTEN to break the circular dimaccount dep that
-- the dbt version has. The original dbt model joins dimaccount as a
-- regular MERGE-target table; here dimaccount is itself a DT that
-- UNIONs this very table, so a direct join would be cyclic and Snowflake
-- rejects the DAG at CREATE time.
--
-- Replacement: join bronzeaccount directly, picking the latest known
-- state of each account (by accountid, ordered by update_dt + cdc_dsn).
-- Tradeoff: doesn't honor "what was the account state AT the time of
-- the customer update" — uses the all-time-latest state. For SF=10
-- smoke testing that's acceptable; auditing may show drift vs the dbt
-- variant for cases where account state changed AFTER a customer event.
-- A more rigorous version would use LATERAL with a correlated subquery
-- (not incremental-refresh eligible) or a self-join with row-number on
-- (a.update_dt <= c.update_dt) (sliding-frame window — also likely
-- forces FULL refresh).
-- ============================================================================

CREATE OR REPLACE DYNAMIC TABLE {catalog}.{schema}.account_updates_from_customer
  TARGET_LAG   = DOWNSTREAM
  WAREHOUSE    = {warehouse}
  REFRESH_MODE = INCREMENTAL
AS
WITH latest_account AS (
  SELECT
    accountid, brokerid, customerid, accountdesc, taxstatus, status, update_dt
  FROM {catalog}.{schema}.bronzeaccount
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY accountid
    ORDER BY update_dt DESC, cdc_dsn DESC
  ) = 1
)
SELECT
  'cust_update'  AS cdc_flag,
  -1::bigint     AS cdc_dsn,
  la.accountid,
  la.brokerid,
  c.customerid,
  la.accountdesc,
  la.taxstatus,
  la.status,
  c.update_dt
FROM {catalog}.{schema}.bronzecustomer c
JOIN latest_account la ON la.customerid = c.customerid
WHERE c.cdc_flag = 'U';


-- ============================================================================
-- SILVER — dimcustomer (SCD2)
--
-- dbt approach: per-batch new_rows (iscurrent=T) + close_rows (iscurrent=F)
-- UNION ALL, MERGE keyed on sk_customerid with update-cols {iscurrent, enddate}.
--
-- DT approach: every bronzecustomer.I or .U event becomes a dim row. iscurrent
-- and enddate derived from the next event per customerid via window functions.
-- LAST_VALUE with offset frame replaces dbt's MERGE close-row logic.
--
-- INCREMENTAL refresh eligibility: ROW_NUMBER and LAST_VALUE with bounded
-- frames are documented as supported. Custom frame (ROWS BETWEEN 1 FOLLOWING
-- AND 1 FOLLOWING) may force fall-back — verify at deploy.
-- ============================================================================

CREATE OR REPLACE DYNAMIC TABLE {catalog}.{schema}.dimcustomer
  TARGET_LAG   = DOWNSTREAM
  WAREHOUSE    = {warehouse}
  REFRESH_MODE = INCREMENTAL
AS
WITH ranked AS (
  SELECT
    c.*,
    LAST_VALUE(c.update_dt) OVER (
      PARTITION BY c.customerid
      ORDER BY c.update_dt
      ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING
    ) AS next_update_dt,
    ROW_NUMBER() OVER (
      PARTITION BY c.customerid
      ORDER BY c.update_dt DESC
    ) AS rn_desc
  FROM {catalog}.{schema}.bronzecustomer c
)
SELECT
  (TO_CHAR(c.update_dt, 'YYYYMMDD') || c.customerid::string)::number(38,0) AS sk_customerid,
  c.customerid,
  c.taxid,
  DECODE(c.status,
    'ACTV', 'Active',
    'CMPT', 'Completed',
    'CNCL', 'Canceled',
    'PNDG', 'Pending',
    'SBMT', 'Submitted',
    'INAC', 'Inactive') AS status,
  c.lastname,
  c.firstname,
  c.middleinitial,
  IFF(UPPER(c.gender) IN ('M', 'F'), UPPER(c.gender), 'U') AS gender,
  c.tier,
  c.dob,
  c.addressline1,
  c.addressline2,
  c.postalcode,
  c.city,
  c.stateprov,
  c.country,
  NVL2(c.c_local_1,
    CONCAT(
      NVL2(c.c_ctry_1, '+' || c.c_ctry_1 || ' ', ''),
      NVL2(c.c_area_1, '(' || c.c_area_1 || ') ', ''),
      c.c_local_1,
      NVL(c.c_ext_1, '')),
    c.c_local_1) AS phone1,
  NVL2(c.c_local_2,
    CONCAT(
      NVL2(c.c_ctry_2, '+' || c.c_ctry_2 || ' ', ''),
      NVL2(c.c_area_2, '(' || c.c_area_2 || ') ', ''),
      c.c_local_2,
      NVL(c.c_ext_2, '')),
    c.c_local_2) AS phone2,
  NVL2(c.c_local_3,
    CONCAT(
      NVL2(c.c_ctry_3, '+' || c.c_ctry_3 || ' ', ''),
      NVL2(c.c_area_3, '(' || c.c_area_3 || ') ', ''),
      c.c_local_3,
      NVL(c.c_ext_3, '')),
    c.c_local_3) AS phone3,
  c.email1,
  c.email2,
  r_nat.tx_name AS nationaltaxratedesc,
  r_nat.tx_rate AS nationaltaxrate,
  r_lcl.tx_name AS localtaxratedesc,
  r_lcl.tx_rate AS localtaxrate,
  c.update_dt   AS effectivedate,
  COALESCE(c.next_update_dt, TO_DATE('9999-12-31')) AS enddate,
  (c.rn_desc = 1) AS iscurrent
FROM ranked c
JOIN {catalog}.{schema}.taxrate r_lcl ON c.lcl_tx_id = r_lcl.tx_id
JOIN {catalog}.{schema}.taxrate r_nat ON c.nat_tx_id = r_nat.tx_id;


-- ============================================================================
-- SILVER — dimaccount (SCD2)
--
-- Same pattern as dimcustomer. Source is the UNION ALL of bronzeaccount and
-- account_updates_from_customer dedup'd by (update_dt, accountid).
-- ============================================================================

CREATE OR REPLACE DYNAMIC TABLE {catalog}.{schema}.dimaccount
  TARGET_LAG   = DOWNSTREAM
  WAREHOUSE    = {warehouse}
  REFRESH_MODE = INCREMENTAL
AS
WITH unioned AS (
  SELECT cdc_flag, accountid, brokerid, customerid, accountdesc, taxstatus,
         status, update_dt
  FROM {catalog}.{schema}.bronzeaccount
  UNION ALL
  SELECT cdc_flag, accountid, brokerid, customerid, accountdesc, taxstatus,
         status, update_dt
  FROM {catalog}.{schema}.account_updates_from_customer
),
deduped AS (
  SELECT * FROM unioned
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY update_dt, accountid ORDER BY cdc_flag DESC
  ) = 1
),
ranked AS (
  SELECT
    a.*,
    LAST_VALUE(a.update_dt) OVER (
      PARTITION BY a.accountid
      ORDER BY a.update_dt
      ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING
    ) AS next_update_dt,
    ROW_NUMBER() OVER (
      PARTITION BY a.accountid
      ORDER BY a.update_dt DESC
    ) AS rn_desc
  FROM deduped a
)
SELECT
  (TO_CHAR(a.update_dt, 'YYYYMMDD') || a.accountid::varchar)::number(38,0) AS sk_accountid,
  a.accountid,
  a.brokerid AS sk_brokerid,
  dc.sk_customerid,
  a.accountdesc,
  a.taxstatus,
  DECODE(a.status,
    'ACTV', 'Active',
    'CMPT', 'Completed',
    'CNCL', 'Canceled',
    'PNDG', 'Pending',
    'SBMT', 'Submitted',
    'INAC', 'Inactive',
    a.status) AS status,
  (a.rn_desc = 1) AS iscurrent,
  a.update_dt    AS effectivedate,
  COALESCE(a.next_update_dt, TO_DATE('9999-12-31')) AS enddate
FROM ranked a
JOIN {catalog}.{schema}.dimcustomer dc
  ON dc.iscurrent
 AND dc.customerid = a.customerid;


-- ============================================================================
-- SILVER — dimtrade (SCD2 with close_ts derivation)
--
-- dbt approach: max_by(object_construct(...), t_dts) over today's bronze rows,
-- then derive close_ts where status IN ('CMPT','CNCL'); MERGE keyed by tradeid
-- with merge_update_columns and incremental_predicate sk_closedateid IS NULL.
--
-- DT approach: per tradeid, take the LATEST bronzetrade row (max t_dts) — no
-- need to "find the last record" because the LATEST IS the dim row. status
-- derivation, sk_closedateid logic same as the dbt model.
--
-- Note: each tradeid's row in dimtrade reflects the most-recent bronzetrade
-- event for that tradeid. Closed trades stay frozen forever (no more events).
-- Open trades update when their next event arrives. Same semantics as dbt
-- MERGE with merge_update_columns restricting the update scope.
-- ============================================================================

CREATE OR REPLACE DYNAMIC TABLE {catalog}.{schema}.dimtrade
  TARGET_LAG   = DOWNSTREAM
  WAREHOUSE    = {warehouse}
  REFRESH_MODE = INCREMENTAL
AS
WITH latest_per_trade AS (
  SELECT
    tradeid,
    MIN(IFF(cdc_flag = 'I', t_dts, NULL)) AS create_ts,
    MAX_BY(t_dts,      t_dts) AS max_t_dts,
    MAX_BY(status,     t_dts) AS status_code,
    MAX_BY(t_tt_id,    t_dts) AS t_tt_id,
    MAX_BY(cashflag,   t_dts) AS cashflag,
    MAX_BY(t_s_symb,   t_dts) AS t_s_symb,
    MAX_BY(quantity,   t_dts) AS quantity,
    MAX_BY(bidprice,   t_dts) AS bidprice,
    MAX_BY(t_ca_id,    t_dts) AS t_ca_id,
    MAX_BY(executedby, t_dts) AS executedby,
    MAX_BY(tradeprice, t_dts) AS tradeprice,
    MAX_BY(fee,        t_dts) AS fee,
    MAX_BY(commission, t_dts) AS commission,
    MAX_BY(tax,        t_dts) AS tax
  FROM {catalog}.{schema}.bronzetrade
  GROUP BY tradeid
),
derived AS (
  SELECT
    tradeid,
    create_ts,
    max_t_dts,
    CASE WHEN status_code IN ('CMPT', 'CNCL') THEN max_t_dts END AS close_ts,
    DECODE(status_code,
      'ACTV', 'Active',
      'CMPT', 'Completed',
      'CNCL', 'Canceled',
      'PNDG', 'Pending',
      'SBMT', 'Submitted',
      'INAC', 'Inactive') AS status,
    DECODE(t_tt_id,
      'TMB', 'Market Buy',
      'TMS', 'Market Sell',
      'TSL', 'Stop Loss',
      'TLS', 'Limit Sell',
      'TLB', 'Limit Buy') AS type,
    IFF(cashflag = 1, TRUE, FALSE) AS cashflag,
    t_s_symb, quantity, bidprice, t_ca_id, executedby,
    tradeprice, fee, commission, tax
  FROM latest_per_trade
)
SELECT
  t.tradeid,
  da.sk_brokerid,
  TO_CHAR(t.create_ts, 'YYYYMMDD')::number AS sk_createdateid,
  TO_CHAR(t.create_ts, 'HH24MISS')::number AS sk_createtimeid,
  TO_CHAR(t.close_ts,  'YYYYMMDD')::number AS sk_closedateid,
  TO_CHAR(t.close_ts,  'HH24MISS')::number AS sk_closetimeid,
  t.status, t.type, t.cashflag,
  ds.sk_securityid, ds.sk_companyid,
  t.quantity, t.bidprice,
  da.sk_customerid, da.sk_accountid,
  t.executedby, t.tradeprice, t.fee, t.commission, t.tax
FROM derived t
JOIN {catalog}.{schema}.dimsecurity ds
  ON ds.symbol = t.t_s_symb
 AND TO_DATE(t.max_t_dts) >= ds.effectivedate
 AND TO_DATE(t.max_t_dts) <  ds.enddate
JOIN {catalog}.{schema}.dimaccount da
  ON t.t_ca_id = da.accountid
 AND da.iscurrent;


-- ============================================================================
-- SILVER — factwatches
--
-- dbt approach: per (customerid, symbol) compute dateplaced (min of non-CNCL)
-- and dateremoved (max of CNCL); MERGE with merge_update_columns
-- {sk_dateid_dateremoved, removed} and incremental_predicate removed=FALSE.
--
-- DT approach: same aggregate over ALL bronzewatches rows (across all time).
-- ============================================================================

CREATE OR REPLACE DYNAMIC TABLE {catalog}.{schema}.factwatches
  TARGET_LAG   = {target_lag}
  WAREHOUSE    = {warehouse}
  REFRESH_MODE = INCREMENTAL
AS
WITH w AS (
  SELECT
    w_c_id   AS customerid,
    w_s_symb AS symbol,
    TO_DATE(MIN(IFF(w_action != 'CNCL', w_dts, CAST(NULL AS TIMESTAMP)))) AS dateplaced,
    TO_DATE(MAX(IFF(w_action  = 'CNCL', w_dts, CAST(NULL AS TIMESTAMP)))) AS dateremoved
  FROM {catalog}.{schema}.bronzewatches
  GROUP BY w_c_id, w_s_symb
)
SELECT
  c.sk_customerid,
  s.sk_securityid,
  w.customerid,
  w.symbol,
  TO_CHAR(w.dateplaced,  'YYYYMMDD')::number AS sk_dateid_dateplaced,
  TO_CHAR(w.dateremoved, 'YYYYMMDD')::number AS sk_dateid_dateremoved,
  NVL2(w.dateremoved, TRUE, FALSE) AS removed
FROM w
JOIN {catalog}.{schema}.dimsecurity s
  ON s.symbol = w.symbol
 AND s.iscurrent
JOIN {catalog}.{schema}.dimcustomer c
  ON w.customerid = c.customerid
 AND c.iscurrent;


-- ============================================================================
-- GOLD — currentaccountbalances
--
-- dbt approach: delete+insert keyed on accountid; SELECT result is fresh
-- per-account cumulative balance (UNION ALL of new transactions + prior
-- balances, then GROUP BY).
--
-- DT approach: simpler — aggregate ALL bronzecashtransaction rows ever,
-- grouped by accountid. Each refresh re-evaluates per accountid that has new
-- transactions. The "latest_batch" flag from the dbt model is dropped (it
-- was used by factcashbalances to filter today's accounts only — see below).
-- ============================================================================

CREATE OR REPLACE DYNAMIC TABLE {catalog}.{schema}.currentaccountbalances
  TARGET_LAG   = DOWNSTREAM
  WAREHOUSE    = {warehouse}
  REFRESH_MODE = INCREMENTAL
AS
SELECT
  MAX(TO_DATE(ct_dts))               AS ct_date,
  accountid,
  SUM(ct_amt)::number(15,2)          AS current_account_cash
FROM {catalog}.{schema}.bronzecashtransaction
GROUP BY accountid;


-- ============================================================================
-- GOLD — factcashbalances
--
-- dbt approach: per-batch, one row per (account_touched, today's sk_dateid)
-- with the day's snapshot cash. MERGE keyed (sk_accountid, sk_dateid).
--
-- DT approach: re-aggregate from bronzecashtransaction directly. For each
-- (accountid, ct_date) where any transaction landed, the cumulative balance
-- AS OF that date. Multi-day-grain output (one row per account per
-- transaction-day) matches the dbt MERGE output over time.
-- ============================================================================

CREATE OR REPLACE DYNAMIC TABLE {catalog}.{schema}.factcashbalances
  TARGET_LAG   = {target_lag}
  WAREHOUSE    = {warehouse}
  REFRESH_MODE = INCREMENTAL
AS
WITH per_day AS (
  SELECT
    accountid,
    TO_DATE(ct_dts) AS ct_date,
    SUM(ct_amt) AS day_amt
  FROM {catalog}.{schema}.bronzecashtransaction
  GROUP BY accountid, TO_DATE(ct_dts)
),
running AS (
  SELECT
    accountid,
    ct_date,
    SUM(day_amt) OVER (
      PARTITION BY accountid ORDER BY ct_date
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cash
  FROM per_day
)
SELECT
  a.sk_customerid,
  a.sk_accountid,
  TO_CHAR(r.ct_date, 'YYYYMMDD')::number AS sk_dateid,
  r.cash::number(15,2) AS cash
FROM running r
JOIN {catalog}.{schema}.dimaccount a
  ON r.accountid = a.accountid
 AND a.iscurrent;


-- ============================================================================
-- GOLD — factholdings
--
-- dbt approach: append-only fact, one row per holding-event whose trade is
-- closed-on-event-day. Same here.
-- ============================================================================

CREATE OR REPLACE DYNAMIC TABLE {catalog}.{schema}.factholdings
  TARGET_LAG   = {target_lag}
  WAREHOUSE    = {warehouse}
  REFRESH_MODE = INCREMENTAL
AS
WITH events AS (
  SELECT
    hh_h_t_id    AS tradeid,
    hh_t_id      AS currenttradeid,
    hh_after_qty AS currentholding,
    event_dt
  FROM {catalog}.{schema}.bronzeholdings
)
SELECT
  h.tradeid,
  h.currenttradeid,
  t.sk_customerid,
  t.sk_accountid,
  t.sk_securityid,
  t.sk_companyid,
  t.sk_closedateid AS sk_dateid,
  t.sk_closetimeid AS sk_timeid,
  t.tradeprice     AS currentprice,
  h.currentholding
FROM events h
JOIN {catalog}.{schema}.dimtrade t
  ON t.tradeid = h.tradeid
 AND t.sk_closedateid = TO_CHAR(h.event_dt, 'YYYYMMDD')::number;


-- ============================================================================
-- GOLD — factmarkethistory
--
-- dbt approach: per-batch new-day rows with 52-week low/high computed via
-- MIN_BY/MAX_BY(object_construct) over the prior-365-day window.
--
-- DT approach: this is the model most likely to fall back to FULL refresh
-- due to the sliding 365-day window aggregate. Three formulations tried in
-- order of preference (commented out alternatives):
--
--   A) Sliding 365-day MIN_BY/MAX_BY scoped via WHERE — may force FULL
--      because the lower bound moves daily (rows age out).
--
--   B) Per-day aggregate joined to itself with windowed MIN/MAX over 364
--      preceding rows — uses bounded window frame; should incrementalize.
--
--   C) All-time MIN/MAX per symbol — drops the 52-week semantics but is
--      strictly incremental. Use only if A and B both fall back to FULL.
--
-- Implementing formulation B below; if Snowflake refuses to incrementalize,
-- swap in A's WHERE-filter variant (still semantically correct, just heavier
-- on refresh cost).
-- ============================================================================

CREATE OR REPLACE DYNAMIC TABLE {catalog}.{schema}.factmarkethistory
  TARGET_LAG   = {target_lag}
  WAREHOUSE    = {warehouse}
  REFRESH_MODE = INCREMENTAL
AS
WITH per_day AS (
  -- One row per (symbol, day) — the bronze daily market grain.
  SELECT
    dm_s_symb,
    dm_date,
    dm_close,
    dm_high,
    dm_low,
    dm_vol
  FROM {catalog}.{schema}.bronzedailymarket
),
windowed AS (
  -- 52-week rolling MIN_BY(low, low) and MAX_BY(high, high) ending at each
  -- (symbol, day). MIN_BY/MAX_BY with a bounded preceding window: each new
  -- day's row is appended, and Snowflake's incremental engine only needs to
  -- recompute the 365-day suffix per affected symbol.
  SELECT
    dm_s_symb,
    dm_date,
    dm_close,
    dm_high,
    dm_low,
    dm_vol,
    MIN_BY(OBJECT_CONSTRUCT('dm_low',  dm_low,  'dm_date', dm_date), dm_low) OVER (
      PARTITION BY dm_s_symb ORDER BY dm_date
      ROWS BETWEEN 364 PRECEDING AND CURRENT ROW
    ) AS fiftytwoweeklow,
    MAX_BY(OBJECT_CONSTRUCT('dm_high', dm_high, 'dm_date', dm_date), dm_high) OVER (
      PARTITION BY dm_s_symb ORDER BY dm_date
      ROWS BETWEEN 364 PRECEDING AND CURRENT ROW
    ) AS fiftytwoweekhigh
  FROM per_day
)
SELECT
  s.sk_securityid,
  s.sk_companyid,
  TO_CHAR(dm.dm_date, 'YYYYMMDD')::number             AS sk_dateid,
  DIV0(dm.dm_close,  f.prev_year_basic_eps)            AS peratio,
  DIV0(s.dividend,   dm.dm_close) / 100                AS yield,
  dm.fiftytwoweekhigh:dm_high::float                                 AS fiftytwoweekhigh,
  TO_CHAR(dm.fiftytwoweekhigh:dm_date::date, 'YYYYMMDD')::number     AS sk_fiftytwoweekhighdate,
  dm.fiftytwoweeklow:dm_low::float                                   AS fiftytwoweeklow,
  TO_CHAR(dm.fiftytwoweeklow:dm_date::date, 'YYYYMMDD')::number      AS sk_fiftytwoweeklowdate,
  dm.dm_close AS closeprice,
  dm.dm_high  AS dayhigh,
  dm.dm_low   AS daylow,
  dm.dm_vol   AS volume
FROM windowed dm
JOIN {catalog}.{schema}.dimsecurity s
  ON s.symbol = dm.dm_s_symb
 AND dm.dm_date >= s.effectivedate
 AND dm.dm_date <  s.enddate
LEFT JOIN {catalog}.{schema}.companyyeareps f
  ON f.sk_companyid = s.sk_companyid
 AND QUARTER(dm.dm_date) = QUARTER(f.qtr_start_date)
 AND YEAR(dm.dm_date)    = YEAR(f.qtr_start_date);


-- ============================================================================
-- End of dt_create.sql. After this runs, the DAG is live and any new rows
-- landed into bronze_raw tables propagate to the leaves at TARGET_LAG.
-- ============================================================================
