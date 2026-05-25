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
--
-- TARGET_LAG = DOWNSTREAM on every DT. Leaves (factwatches, factcashbalances,
-- factholdings, factmarkethistory) have no downstream consumers, so Snowflake
-- never schedules an automatic refresh — manual ALTER DYNAMIC TABLE ... REFRESH
-- (issued by dt_wait_refresh.py per batch) is the only trigger, and it cascades
-- through the DOWNSTREAM chain to refresh intermediates exactly once per batch.
--
-- DAG (refresh order — Snowflake infers from query references):
--
--   bronze* tables     (regular Snowflake tables with CHANGE_TRACKING = TRUE;
--                       seeded from federated Iceberg via CTAS at setup time,
--                       then appended per batch via COPY INTO by seed_raw.py)
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
-- No bronze* DT pass-through layer. Downstream DTs read directly from the
-- regular bronze tables — Snowflake's incremental refresh tracks the
-- CHANGE_TRACKING stream on those tables, so each per-batch COPY INTO
-- propagates through the silver/gold DAG without an intermediate DT.
--
-- All clone-target tables (taxrate, dimdate, industry, tradetype, dimbroker,
-- dimsecurity, statustype, dimcompany, dimtime, financial, companyyeareps,
-- batchdate, cashtransactionhistorical) are CLONEd as REGULAR tables by
-- setup_sf_dt.py before this DDL runs — they don't change per batch so we
-- don't need DT semantics on them.
-- ============================================================================



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
-- Pattern ported from historical/DimCustomerHistorical.sql: LEAD(update_dt)
-- OVER (PARTITION BY customerid ORDER BY update_dt) gives the next event's
-- date, which becomes this row's enddate. iscurrent = (next_update_dt IS NULL)
-- — the row with no successor is the live one.
-- ============================================================================

CREATE OR REPLACE DYNAMIC TABLE {catalog}.{schema}.dimcustomer
  TARGET_LAG   = DOWNSTREAM
  WAREHOUSE    = {warehouse}
  REFRESH_MODE = INCREMENTAL
AS
WITH ranked AS (
  SELECT
    c.*,
    LEAD(c.update_dt) OVER (
      PARTITION BY c.customerid
      ORDER BY c.update_dt
    ) AS next_update_dt
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
  (c.next_update_dt IS NULL) AS iscurrent
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
    LEAD(a.update_dt) OVER (
      PARTITION BY a.accountid
      ORDER BY a.update_dt
    ) AS next_update_dt
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
  (a.next_update_dt IS NULL) AS iscurrent,
  a.update_dt    AS effectivedate,
  COALESCE(a.next_update_dt, TO_DATE('9999-12-31')) AS enddate
FROM ranked a
JOIN {catalog}.{schema}.dimcustomer dc
  ON dc.iscurrent
 AND dc.customerid = a.customerid;


-- ============================================================================
-- SILVER — dimtrade
--
-- Pattern ported from src/incremental_batches/augmented_incremental/historical/
-- DimTradeHistorical.sql. Single CTE: QUALIFY ROW_NUMBER picks the latest
-- t_dts row per tradeid (whole row, no per-column MAX_BY); MIN OVER PARTITION
-- gives create_ts (first t_dts per tradeid); CMPT-only columns wrapped in
-- CASE WHEN; final SELECT joins to dimsecurity/dimaccount on point-in-time
-- effectivedate ranges keyed off create_ts.
-- ============================================================================

CREATE OR REPLACE DYNAMIC TABLE {catalog}.{schema}.dimtrade
  TARGET_LAG   = DOWNSTREAM
  WAREHOUSE    = {warehouse}
  REFRESH_MODE = INCREMENTAL
AS
WITH rawtrade AS (
  SELECT
    tradeid,
    t_dts,
    MIN(t_dts) OVER (PARTITION BY tradeid) AS create_ts,
    CASE WHEN status IN ('CMPT', 'CNCL') THEN t_dts END AS close_ts,
    DECODE(status,
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
    CASE WHEN status = 'CMPT' THEN tradeprice END AS tradeprice,
    CASE WHEN status = 'CMPT' THEN fee        END AS fee,
    CASE WHEN status = 'CMPT' THEN commission END AS commission,
    CASE WHEN status = 'CMPT' THEN tax        END AS tax
  FROM {catalog}.{schema}.bronzetrade
  QUALIFY ROW_NUMBER() OVER (PARTITION BY tradeid ORDER BY t_dts DESC) = 1
)
SELECT
  trade.tradeid,
  da.sk_brokerid,
  TO_CHAR(trade.create_ts, 'YYYYMMDD')::number AS sk_createdateid,
  TO_CHAR(trade.create_ts, 'HH24MISS')::number AS sk_createtimeid,
  TO_CHAR(trade.close_ts,  'YYYYMMDD')::number AS sk_closedateid,
  TO_CHAR(trade.close_ts,  'HH24MISS')::number AS sk_closetimeid,
  trade.status, trade.type, trade.cashflag,
  ds.sk_securityid, ds.sk_companyid,
  trade.quantity, trade.bidprice,
  da.sk_customerid, da.sk_accountid,
  trade.executedby, trade.tradeprice, trade.fee, trade.commission, trade.tax
FROM rawtrade trade
JOIN {catalog}.{schema}.dimsecurity ds
  ON ds.symbol = trade.t_s_symb
 AND TO_DATE(trade.create_ts) >= ds.effectivedate
 AND TO_DATE(trade.create_ts) <  ds.enddate
JOIN {catalog}.{schema}.dimaccount da
  ON trade.t_ca_id = da.accountid
 AND TO_DATE(trade.create_ts) >= da.effectivedate
 AND TO_DATE(trade.create_ts) <  da.enddate;


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
  TARGET_LAG   = DOWNSTREAM
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
-- Cast FLOAT → NUMBER inside SUM (outer cast doesn't satisfy the INCREMENTAL
-- change-tracking planner — it inspects the aggregate's input type).
SELECT
  MAX(TO_DATE(ct_dts))               AS ct_date,
  accountid,
  SUM(ct_amt::number(15,2))          AS current_account_cash
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
  TARGET_LAG   = DOWNSTREAM
  WAREHOUSE    = {warehouse}
  REFRESH_MODE = INCREMENTAL
AS
WITH per_day AS (
  -- Cast FLOAT → NUMBER inside SUM so the windowed running balance below
  -- runs over a fixed-point type (INCREMENTAL refresh requirement).
  SELECT
    accountid,
    TO_DATE(ct_dts) AS ct_date,
    SUM(ct_amt::number(15,2)) AS day_amt
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
  TARGET_LAG   = DOWNSTREAM
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
-- Pattern: pack (value, date) into one sortable integer in the per-row
-- projection, then plain MIN/MAX over a date-ordered sliding 52w window
-- recovers BOTH the value and the tiebreak date in a single windowed pass.
-- Decoded downstream.
--
-- Why the encoding: the natural form is MIN_BY/MAX_BY(struct(date,value), value)
-- over a sliding window — which Snowflake's DT compiler rejects ("Sliding
-- window frame unsupported for function MIN_BY"). Plain MIN/MAX over a
-- sliding frame IS supported AND incrementalizable. The encoding maps the
-- MIN_BY semantics onto plain MIN by making the sort order (value asc,
-- date asc) representable as a single integer.
--
-- Encoding (value tiebroken by EARLIEST date per TPC-DI spec):
--   value_as_int = ROUND(value * 100)  -- captures cents as int
--   date_slot    = 5-digit DATEDIFF(day, '1900-01-01', dm_date)
--                  (5 digits = 273-year span from epoch; future-proof)
--   LOW:  pack = value_int * 100000 + date_slot
--           MIN picks smallest value; ties → smallest date_slot = earliest.
--   HIGH: pack = value_int * 100000 + (99999 - date_slot)
--           MAX picks largest value; ties → largest (99999 - date_slot)
--                                          = smallest date_slot = earliest.
--
-- REFRESH_MODE = INCREMENTAL forced. Snowflake AUTO conservatively chose
-- FULL for the joined query, but empirical refresh on a 1-day insert at
-- SF=10 shows 7,352 inserted / 0 deleted / 0 copied — true append-only.
-- (See incremental_test/10_pattern_i_with_joins.sql for the test harness.)
-- ============================================================================

CREATE OR REPLACE DYNAMIC TABLE {catalog}.{schema}.factmarkethistory
  TARGET_LAG   = DOWNSTREAM
  WAREHOUSE    = {warehouse}
  REFRESH_MODE = INCREMENTAL
AS
WITH per_day AS (
  SELECT
    dm_s_symb, dm_date, dm_close, dm_high, dm_low, dm_vol,
    -- LOW: MIN picks smallest value; ties broken by smallest date offset (earliest).
    ROUND(dm_low::number(38,2)  * 100)::number(38,0) * 100000
      +             DATEDIFF(day, DATE '1900-01-01', dm_date)        AS low_packed,
    -- HIGH: MAX picks largest value; ties broken by largest (99999 - offset)
    -- = smallest offset = earliest.
    ROUND(dm_high::number(38,2) * 100)::number(38,0) * 100000
      + (99999 -    DATEDIFF(day, DATE '1900-01-01', dm_date))       AS high_packed
  FROM {catalog}.{schema}.bronzedailymarket
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
JOIN {catalog}.{schema}.dimsecurity s
  ON  s.symbol    = dm.dm_s_symb
  AND dm.dm_date >= s.effectivedate
  AND dm.dm_date <  s.enddate
LEFT JOIN {catalog}.{schema}.companyyeareps f
  ON  f.sk_companyid     = s.sk_companyid
  AND QUARTER(dm.dm_date) = QUARTER(f.qtr_start_date)
  AND YEAR(dm.dm_date)    = YEAR(f.qtr_start_date);


-- ============================================================================
-- End of dt_create.sql. After this runs, the DAG is live and any new rows
-- landed into bronze tables propagate to the leaves at TARGET_LAG via
-- their CHANGE_TRACKING streams.
-- ============================================================================
