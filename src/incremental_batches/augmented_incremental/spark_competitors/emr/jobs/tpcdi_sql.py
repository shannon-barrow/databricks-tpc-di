"""SQL for the EMR variant of the Augmented Incremental TPC-DI benchmark.

Pure functions that return Spark SQL strings, so they can be unit-tested
without a Spark session. The transforms are ported from the Fabric Spark
notebooks (spark_competitors/fabric/notebooks/), which mirror the dbt models.
Only the dialect differences forced by open-source Spark 3.5 + delta-spark
(the EMR 7.x runtime) are changed:
  - no `SELECT * EXCEPT(...)`: DimAccount lists its columns explicitly;
  - no `CLUSTER BY` on the bronze tables: OSS Delta only clusters on
    OPTIMIZE, so per-batch reads rely on file-level min/max skipping instead
    (each batch appends its own files, all with one event date).
Everything else (CTEs, MERGE shapes, the DELETE-then-INSERT selective
overwrite, decode, max_by, GROUP BY ALL) runs unchanged on Spark 3.5.
"""
from __future__ import annotations

# 12 reference tables + 8 dims/facts the run schema starts from: the same 20
# the Databricks and Fabric setups clone from staging.
STAGING_TABLES = [
    "taxrate", "dimdate", "industry", "tradetype", "dimbroker", "financial",
    "companyyeareps", "dimsecurity", "statustype", "dimcompany", "dimtime",
    "currentaccountbalances",
    "dimcustomer", "dimaccount", "dimtrade", "factwatches", "factholdings",
    "factmarkethistory", "bronzedailymarket", "factcashbalances",
]

# Source-file schemas, identical to the Databricks / Fabric bronze schemas.
# The 6 empty bronze tables use the same column lists.
BRONZE_SCHEMAS = {
    "account":         "cdc_flag STRING, cdc_dsn BIGINT, accountid BIGINT, brokerid BIGINT, customerid BIGINT, accountdesc STRING, taxstatus TINYINT, status STRING, update_dt DATE",
    "cashtransaction": "cdc_flag STRING, cdc_dsn BIGINT, accountid BIGINT, ct_dts TIMESTAMP, ct_amt DOUBLE, ct_name STRING, event_dt DATE",
    "customer":        "cdc_flag STRING, cdc_dsn BIGINT, customerid BIGINT, taxid STRING, status STRING, lastname STRING, firstname STRING, middleinitial STRING, gender STRING, tier TINYINT, dob DATE, addressline1 STRING, addressline2 STRING, postalcode STRING, city STRING, stateprov STRING, country STRING, c_ctry_1 STRING, c_area_1 STRING, c_local_1 STRING, c_ext_1 STRING, c_ctry_2 STRING, c_area_2 STRING, c_local_2 STRING, c_ext_2 STRING, c_ctry_3 STRING, c_area_3 STRING, c_local_3 STRING, c_ext_3 STRING, email1 STRING, email2 STRING, lcl_tx_id STRING, nat_tx_id STRING, update_dt DATE",
    "dailymarket":     "cdc_flag STRING, cdc_dsn BIGINT, dm_date DATE, dm_s_symb STRING, dm_close DOUBLE, dm_high DOUBLE, dm_low DOUBLE, dm_vol INT",
    "holdings":        "cdc_flag STRING, cdc_dsn BIGINT, hh_h_t_id BIGINT, hh_t_id BIGINT, hh_before_qty INT, hh_after_qty INT, event_dt DATE",
    "trade":           "cdc_flag STRING, cdc_dsn BIGINT, tradeid BIGINT, t_dts TIMESTAMP, status STRING, t_tt_id STRING, cashflag TINYINT, t_s_symb STRING, quantity INT, bidprice DOUBLE, t_ca_id BIGINT, executedby STRING, tradeprice DOUBLE, fee DOUBLE, commission DOUBLE, tax DOUBLE, event_dt DATE",
    "watches":         "cdc_flag STRING, cdc_dsn BIGINT, w_c_id BIGINT, w_s_symb STRING, w_dts TIMESTAMP, w_action STRING, event_dt DATE",
}

# The day's file per bronze source, as simulate_filedrops names it.
BRONZE_FILES = {
    "account": "Account.txt", "cashtransaction": "CashTransaction.txt",
    "customer": "Customer.txt", "dailymarket": "DailyMarket.txt",
    "holdings": "HoldingHistory.txt", "trade": "Trade.txt",
    "watches": "WatchHistory.txt",
}

# bronzedailymarket is cloned from staging (the prior-year seed FactMarketHistory's
# 52-week window reads); the other 6 start empty.
EMPTY_BRONZE = ["account", "cashtransaction", "customer", "holdings", "trade", "watches"]

# Past Delta's default 32-column stats window: bronzecustomer's update_dt is
# its 34th column, and the per-batch date filter needs stats on it to skip
# the earlier batches' files.
BRONZE_TBLPROPERTIES = "TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = '34')"


def _date(batch_date: str) -> str:
    return f"cast('{batch_date}' as date)"


# --- setup ------------------------------------------------------------------

def create_database(db: str, location: str) -> str:
    return f"CREATE DATABASE {db} LOCATION '{location}'"


def drop_database(db: str) -> str:
    return f"DROP DATABASE IF EXISTS {db} CASCADE"


def clone_from_parquet(db: str, table: str, parquet_root: str) -> str:
    """Per-run copy of one staging table. OSS Delta has no clone from the
    Databricks staging tables, so the staging lives as plain parquet and each
    run rebuilds its Delta tables from it."""
    return (f"CREATE TABLE {db}.{table} USING DELTA AS "
            f"SELECT * FROM parquet.`{parquet_root.rstrip('/')}/{table}`")


def create_bronze(db: str, source: str) -> str:
    return (f"CREATE TABLE {db}.bronze{source} ({BRONZE_SCHEMAS[source]}) "
            f"USING DELTA {BRONZE_TBLPROPERTIES}")


# --- bronze -----------------------------------------------------------------

def ingest_bronze(db: str, source: str, view: str) -> str:
    """Every bronze table accumulates (dbt's append strategy); each transform
    scopes to the batch with its own date filter."""
    return f"INSERT INTO {db}.bronze{source} SELECT * FROM {view}"


def account_updates_from_customer(db: str, batch_date: str) -> str:
    return f"""
  INSERT INTO {db}.bronzeaccount
  with new_events as (
    select * from {db}.bronzecustomer
    where update_dt = {_date(batch_date)}
      and cdc_flag = 'U'
  )
  SELECT
    "cust_update" cdc_flag,
    -1 cdc_dsn,
    a.accountid,
    a.sk_brokerid brokerid,
    c.customerid,
    a.accountdesc,
    a.taxstatus,
    a.status,
    c.update_dt
  FROM new_events c
  JOIN {db}.dimaccount a
    ON
      c.customerid = substring(cast(a.sk_customerid as string), 9)
      and a.iscurrent
      and c.update_dt > a.effectivedate
"""


# --- silver -----------------------------------------------------------------

def _phone(n: int) -> str:
    return f"""nvl2(
        c_local_{n},
        concat(
          nvl2(c_ctry_{n}, '+' || c_ctry_{n} || ' ', ''),
          nvl2(c_area_{n}, '(' || c_area_{n} || ') ', ''),
          c_local_{n},
          nvl(c_ext_{n}, '')),
        c_local_{n}) phone{n}"""


_STATUS_DECODE = """'ACTV', 'Active',
        'CMPT', 'Completed',
        'CNCL', 'Canceled',
        'PNDG', 'Pending',
        'SBMT', 'Submitted',
        'INAC', 'Inactive'"""

_DIMCUSTOMER_COLS = (
    "sk_customerid, customerid, taxid, status, lastname, firstname, middleinitial, "
    "gender, tier, dob, addressline1, addressline2, postalcode, city, stateprov, "
    "country, phone1, phone2, phone3, email1, email2, nationaltaxratedesc, "
    "nationaltaxrate, localtaxratedesc, localtaxrate, effectivedate, enddate, iscurrent")


def dimcustomer(db: str, batch_date: str) -> str:
    return f"""
  with new_events as (
    select * from {db}.bronzecustomer
    where update_dt = {_date(batch_date)}
  ),
  incr_cust as (
    SELECT
      bigint(concat(date_format(c.update_dt, 'yyyyMMdd'), customerid)) sk_customerid,
      customerid,
      taxid,
      decode(status,
        {_STATUS_DECODE}) status,
      lastname,
      firstname,
      middleinitial,
      if(upper(c.gender) IN ('M', 'F'), upper(c.gender), 'U') gender,
      tier,
      dob,
      addressline1,
      addressline2,
      postalcode,
      city,
      stateprov,
      country,
      {_phone(1)},
      {_phone(2)},
      {_phone(3)},
      email1,
      email2,
      r_nat.tx_name as nationaltaxratedesc,
      r_nat.tx_rate as nationaltaxrate,
      r_lcl.tx_name as localtaxratedesc,
      r_lcl.tx_rate as localtaxrate,
      update_dt effectivedate,
      date('9999-12-31') enddate,
      True iscurrent
    FROM new_events c
    JOIN {db}.taxrate r_lcl
      ON c.lcl_tx_id = r_lcl.tx_id
    JOIN {db}.taxrate r_nat
      ON c.nat_tx_id = r_nat.tx_id
  )
  MERGE INTO {db}.dimcustomer t USING (
    SELECT
      s.customerid AS mergeKey,
      s.*
    FROM incr_cust s
    JOIN {db}.dimcustomer t
      ON s.customerid = t.customerid
    WHERE t.iscurrent
    UNION ALL
    SELECT
      cast(null as bigint) AS mergeKey,
      *
    FROM incr_cust
  ) s
    ON
      t.customerid = s.mergeKey
      AND t.iscurrent
  WHEN MATCHED AND s.sk_customerid is not null THEN UPDATE SET
    t.iscurrent = false,
    t.enddate = s.effectivedate
  WHEN NOT MATCHED THEN INSERT ({_DIMCUSTOMER_COLS})
  VALUES ({_DIMCUSTOMER_COLS})
"""


_DIMACCOUNT_COLS = ("sk_accountid, accountid, sk_brokerid, sk_customerid, accountdesc, "
                    "taxstatus, status, iscurrent, effectivedate, enddate")


def dimaccount(db: str, batch_date: str) -> str:
    # Fabric's `SELECT * except(cdc_dsn, _rn)` becomes an explicit column list:
    # OSS Spark 3.5 has no star-except.
    return f"""
  with new_events as (
    select * from {db}.bronzeaccount
    where update_dt = {_date(batch_date)}
  ),
  accounts as (
    SELECT cdc_flag, accountid, brokerid, customerid, accountdesc, taxstatus, status, update_dt
    FROM (
      SELECT *,
        row_number() over (partition by update_dt, accountid order by cdc_flag desc) as _rn
      FROM new_events a
    ) WHERE _rn = 1
  ),
  all_incr_updates as (
    SELECT
      bigint(concat(date_format(a.update_dt, 'yyyyMMdd'), a.accountid)) sk_accountid,
      accountid,
      brokerid sk_brokerid,
      dc.sk_customerid,
      accountdesc,
      taxstatus,
      decode(a.status,
        {_STATUS_DECODE},
        a.status) status,
      true iscurrent,
      update_dt effectivedate,
      date('9999-12-31') enddate
    FROM accounts a
    JOIN {db}.dimcustomer dc
      ON
        dc.iscurrent
        and dc.customerid = a.customerid
  ),
  matched_accts as (
    SELECT
      s.*
    FROM all_incr_updates s
    JOIN {db}.dimaccount t
      ON s.accountid = t.accountid
    WHERE t.iscurrent
  )
  MERGE INTO {db}.dimaccount t USING (
    SELECT
      CAST(NULL AS BIGINT) AS mergeKey,
      *
    FROM all_incr_updates
    UNION ALL
    SELECT
      accountid mergeKey,
      *
    FROM matched_accts
  ) s
  ON t.accountid = s.mergeKey AND t.iscurrent
  WHEN MATCHED AND t.iscurrent THEN UPDATE SET
    t.iscurrent = false,
    t.enddate = s.effectivedate
  WHEN NOT MATCHED THEN INSERT ({_DIMACCOUNT_COLS})
  VALUES ({_DIMACCOUNT_COLS})
"""


_DIMTRADE_COLS = (
    "tradeid, sk_brokerid, sk_createdateid, sk_createtimeid, sk_closedateid, "
    "sk_closetimeid, status, type, cashflag, sk_securityid, sk_companyid, quantity, "
    "bidprice, sk_customerid, sk_accountid, executedby, tradeprice, fee, commission, tax")


def dimtrade(db: str, batch_date: str) -> str:
    return f"""
  with new_events as (
    select * from {db}.bronzetrade
    where event_dt = {_date(batch_date)}
  ),
  trades as (
    SELECT
      tradeid,
      min(case when cdc_flag = "I" then t_dts end) create_ts,
      max_by(
        struct(
          t_dts, status, t_tt_id, cashflag, t_s_symb, quantity, bidprice,
          t_ca_id, executedby, tradeprice, fee, commission, tax
        ),
        t_dts
      ) current_record
    FROM new_events
    group by tradeid
  ),
  current_trades as (
    SELECT
      tradeid,
      create_ts,
      CASE
        WHEN current_record.status IN ("CMPT", "CNCL") THEN current_record.t_dts
        END close_ts,
      decode(current_record.status,
        {_STATUS_DECODE}) status,
      decode(current_record.t_tt_id,
        'TMB', 'Market Buy',
        'TMS', 'Market Sell',
        'TSL', 'Stop Loss',
        'TLS', 'Limit Sell',
        'TLB', 'Limit Buy'
      ) type,
      if(current_record.cashflag = 1, TRUE, FALSE) cashflag,
      current_record.t_s_symb,
      current_record.quantity,
      current_record.bidprice,
      current_record.t_ca_id,
      current_record.executedby,
      current_record.tradeprice,
      current_record.fee,
      current_record.commission,
      current_record.tax,
      current_record.t_dts
    FROM trades t
  ),
  final as (
    SELECT
      t.tradeid,
      sk_brokerid,
      bigint(date_format(create_ts, 'yyyyMMdd')) sk_createdateid,
      bigint(date_format(create_ts, 'HHmmss')) sk_createtimeid,
      bigint(date_format(close_ts, 'yyyyMMdd')) sk_closedateid,
      bigint(date_format(close_ts, 'HHmmss')) sk_closetimeid,
      t.status,
      t.type,
      cashflag,
      sk_securityid,
      sk_companyid,
      t.quantity,
      t.bidprice,
      sk_customerid,
      sk_accountid,
      t.executedby,
      t.tradeprice,
      t.fee,
      t.commission,
      t.tax
    FROM current_trades t
    JOIN {db}.dimsecurity ds
      ON
        ds.symbol = t.t_s_symb
        AND date(t_dts) >= ds.effectivedate
        AND date(t_dts) < ds.enddate
    JOIN {db}.dimaccount da
      ON
        t.t_ca_id = da.accountid
        AND da.iscurrent
  )
  MERGE INTO {db}.dimtrade t
  USING final s
    ON
      t.tradeid = s.tradeid
      AND t.sk_closedateid is null
  WHEN MATCHED THEN UPDATE SET
    sk_closedateid = s.sk_closedateid,
    sk_closetimeid = s.sk_closetimeid,
    status = s.status,
    type = s.type,
    cashflag = s.cashflag,
    quantity = s.quantity,
    bidprice = s.bidprice,
    executedby = s.executedby,
    tradeprice = s.tradeprice,
    fee = s.fee,
    commission = s.commission,
    tax = s.tax
  WHEN NOT MATCHED THEN INSERT ({_DIMTRADE_COLS})
  VALUES ({_DIMTRADE_COLS})
"""


_FACTWATCHES_COLS = ("sk_customerid, sk_securityid, customerid, symbol, "
                     "sk_dateid_dateplaced, sk_dateid_dateremoved, removed")


def factwatches(db: str, batch_date: str) -> str:
    return f"""
  with new_events as (
    select * from {db}.bronzewatches
    where event_dt = {_date(batch_date)}
  ),
  w as (
    SELECT
      w_c_id customerid,
      w_s_symb symbol,
      date(min(if(w_action != 'CNCL', w_dts, cast(null as timestamp)))) dateplaced,
      date(max(if(w_action = 'CNCL', w_dts, cast(null as timestamp)))) dateremoved
    FROM new_events
    group by all
  ),
  stage as (
    SELECT
      c.sk_customerid sk_customerid,
      s.sk_securityid sk_securityid,
      w.customerid,
      w.symbol,
      bigint(date_format(w.dateplaced, 'yyyyMMdd')) sk_dateid_dateplaced,
      bigint(date_format(w.dateremoved, 'yyyyMMdd')) sk_dateid_dateremoved,
      nvl2(w.dateremoved, True, False) removed
    from w
    JOIN {db}.dimsecurity s
      ON
        s.symbol = w.symbol
        AND s.iscurrent
    JOIN {db}.dimcustomer c
      ON
        w.customerid = c.customerid
        AND c.iscurrent
  )
  MERGE INTO {db}.factwatches t
  USING stage s
  ON
    !t.removed
    AND t.symbol = s.symbol
    AND t.customerid = s.customerid
    AND t.sk_dateid_dateremoved IS NULL
  WHEN MATCHED THEN UPDATE SET
    t.sk_dateid_dateremoved = s.sk_dateid_dateremoved,
    t.removed = True
  WHEN NOT MATCHED THEN
  INSERT ({_FACTWATCHES_COLS})
  VALUES ({_FACTWATCHES_COLS})
"""


# --- gold -------------------------------------------------------------------

def currentaccountbalances(db: str, batch_date: str) -> str:
    return f"""
  INSERT OVERWRITE {db}.currentaccountbalances
  with new_txns as (
    SELECT
      to_date(ct_dts) ct_date,
      accountid,
      ct_amt,
      True latest_batch
    FROM {db}.bronzecashtransaction
    where event_dt = {_date(batch_date)}
  ),
  c as (
    SELECT ct_date, accountid, ct_amt, latest_batch FROM new_txns
    UNION ALL
    SELECT
      ct_date,
      accountid,
      current_account_cash,
      False latest_batch
    FROM {db}.currentaccountbalances
  )
  SELECT
    max(ct_date) ct_date,
    accountid,
    cast(sum(ct_amt) as DECIMAL(15,2)) current_account_cash,
    max(latest_batch) latest_batch
  FROM c
  GROUP BY ALL
"""


def factcashbalances_src(db: str) -> str:
    """This batch's rows. The caller replaces their sk_dateids in the target
    (OSS Delta has neither REPLACE USING nor subqueries in DELETE)."""
    return f"""
  SELECT
    a.sk_customerid,
    a.sk_accountid,
    bigint(date_format(c.ct_date, 'yyyyMMdd')) sk_dateid,
    c.current_account_cash
  FROM {db}.currentaccountbalances c
  JOIN {db}.dimaccount a
    ON
      c.accountid = a.accountid
      AND a.iscurrent
  where c.latest_batch
"""


def factholdings(db: str, batch_date: str) -> str:
    return f"""
  INSERT INTO {db}.factholdings
  with new_events as (
    SELECT
      h.hh_h_t_id tradeid,
      h.hh_t_id currenttradeid,
      h.hh_after_qty currentholding,
      h.event_dt
    FROM {db}.bronzeholdings h
    where h.event_dt = {_date(batch_date)}
  )
  SELECT
    h.tradeid,
    currenttradeid,
    t.sk_customerid,
    t.sk_accountid,
    t.sk_securityid,
    t.sk_companyid,
    t.sk_closedateid sk_dateid,
    t.sk_closetimeid sk_timeid,
    t.tradeprice currentprice,
    currentholding
  FROM new_events h
  JOIN {db}.dimtrade t
    ON t.tradeid = h.tradeid
   AND t.sk_closedateid = bigint(date_format(h.event_dt, 'yyyyMMdd'))
"""


def factmarkethistory_src(db: str, batch_date: str) -> str:
    """Today's rows, with the 52-week high/low over the accumulated
    bronzedailymarket. No broadcast hint: companyyeareps is large at high SF
    and plain Spark honours hints literally, so AQE picks the join."""
    return f"""
  with new_dm as (
    select * from {db}.bronzedailymarket
    where dm_date = {_date(batch_date)}
  ),
  sym_min_max as (
    SELECT
      dm_s_symb,
      min_by(struct(dm_low, dm_date), dm_low) fiftytwoweeklow,
      max_by(struct(dm_high, dm_date), dm_high) fiftytwoweekhigh
    FROM {db}.bronzedailymarket
    where dm_date > date_sub({_date(batch_date)}, 365)
    group by all
  )
  SELECT
    s.sk_securityid,
    s.sk_companyid,
    bigint(date_format(dm.dm_date, 'yyyyMMdd')) sk_dateid,
    try_divide(dm.dm_close, f.prev_year_basic_eps) AS peratio,
    (try_divide(s.dividend, dm.dm_close)) / 100 yield,
    agg.fiftytwoweekhigh.dm_high fiftytwoweekhigh,
    bigint(date_format(agg.fiftytwoweekhigh.dm_date, 'yyyyMMdd')) sk_fiftytwoweekhighdate,
    agg.fiftytwoweeklow.dm_low fiftytwoweeklow,
    bigint(date_format(agg.fiftytwoweeklow.dm_date, 'yyyyMMdd')) sk_fiftytwoweeklowdate,
    dm.dm_close closeprice,
    dm.dm_high dayhigh,
    dm.dm_low daylow,
    dm.dm_vol volume
  FROM new_dm dm
  JOIN sym_min_max agg
    ON
      dm.dm_s_symb = agg.dm_s_symb
  JOIN {db}.dimsecurity s
    ON
      s.symbol = dm.dm_s_symb
      AND dm.dm_date >= s.effectivedate
      AND dm.dm_date < s.enddate
  LEFT JOIN {db}.companyyeareps f
    ON
      f.sk_companyid = s.sk_companyid
      AND quarter(dm.dm_date) = quarter(f.qtr_start_date)
      AND year(dm.dm_date) = year(f.qtr_start_date)
"""


def delete_dateids(db: str, table: str, dateids: list[int]) -> str | None:
    """Literal IN-list delete for the selective overwrite; None when the batch
    produced no rows (nothing to replace)."""
    ids = sorted({int(d) for d in dateids if d is not None})
    if not ids:
        return None
    return f"DELETE FROM {db}.{table} WHERE sk_dateid IN ({','.join(map(str, ids))})"


def insert_from_view(db: str, table: str, view: str) -> str:
    return f"INSERT INTO {db}.{table} SELECT * FROM {view}"
