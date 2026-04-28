-- Databricks notebook source
CREATE OR REFRESH STREAMING TABLE dimcustomer (
  sk_customerid BIGINT COMMENT 'Surrogate key for CustomerID', 
  customerid BIGINT COMMENT 'Customer identifier', 
  taxid STRING COMMENT 'Customer’s tax identifier', 
  status STRING COMMENT 'Customer status type', 
  lastname STRING COMMENT 'Customers last name.', 
  firstname STRING COMMENT 'Customers first name.', 
  middleinitial STRING COMMENT 'Customers middle name initial', 
  gender STRING COMMENT 'Gender of the customer', 
  tier TINYINT COMMENT 'Customer tier', 
  dob DATE COMMENT 'Customer’s date of birth.', 
  addressline1 STRING COMMENT 'Address Line 1', 
  addressline2 STRING COMMENT 'Address Line 2', 
  postalcode STRING COMMENT 'Zip or Postal Code', 
  city STRING COMMENT 'City', 
  stateprov STRING COMMENT 'State or Province', 
  country STRING COMMENT 'Country', 
  phone1 STRING COMMENT 'Phone number 1', 
  phone2 STRING COMMENT 'Phone number 2', 
  phone3 STRING COMMENT 'Phone number 3', 
  email1 STRING COMMENT 'Email address 1', 
  email2 STRING COMMENT 'Email address 2', 
  nationaltaxratedesc STRING COMMENT 'National Tax rate description', 
  nationaltaxrate FLOAT COMMENT 'National Tax rate', 
  localtaxratedesc STRING COMMENT 'Local Tax rate description', 
  localtaxrate FLOAT COMMENT 'Local Tax rate', 
  iscurrent BOOLEAN GENERATED ALWAYS AS (nvl2(__END_AT, false, true)) COMMENT 'True if this is the current record', 
  effectivedate DATE COMMENT 'Beginning of date range when this record was the current record', 
  enddate DATE GENERATED ALWAYS AS (nvl(__END_AT, date('9999-12-31'))) COMMENT 'Ending of date range when this record was the current record. A record that is not expired will use the date 9999-12-31.', 
  __START_AT DATE COMMENT 'Beginning of date range when this record was the current record', 
  __END_AT DATE COMMENT 'Ending of date range when this record was the current record.'
)
PARTITIONED BY (iscurrent);

-- COMMAND ----------

CREATE FLOW dimcustomer_incremental AS AUTO CDC INTO
  dimcustomer
FROM (
  SELECT
    bigint(concat(date_format(c.update_dt, 'yyyyMMdd'), customerid)) sk_customerid,
    customerid,
    taxid,
    decode(status, 
      'ACTV',	'Active',
      'CMPT','Completed',
      'CNCL','Canceled',
      'PNDG','Pending',
      'SBMT','Submitted',
      'INAC','Inactive') status,
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
    nvl2(
      c_local_1,
      concat(
        nvl2(c_ctry_1, '+' || c_ctry_1 || ' ', ''),
        nvl2(c_area_1, '(' || c_area_1 || ') ', ''),
        c_local_1,
        nvl(c_ext_1, '')),
      c_local_1) phone1,
    nvl2(
      c_local_2,
      concat(
        nvl2(c_ctry_2, '+' || c_ctry_2 || ' ', ''),
        nvl2(c_area_2, '(' || c_area_2 || ') ', ''),
        c_local_2,
        nvl(c_ext_2, '')),
      c_local_2) phone2,
    nvl2(
      c_local_3,
      concat(
        nvl2(c_ctry_3, '+' || c_ctry_3 || ' ', ''),
        nvl2(c_area_3, '(' || c_area_3 || ') ', ''),
        c_local_3,
        nvl(c_ext_3, '')),
      c_local_3) phone3,
    email1,
    email2,
    r_nat.tx_name as nationaltaxratedesc,
    r_nat.tx_rate as nationaltaxrate,
    r_lcl.tx_name as localtaxratedesc,
    r_lcl.tx_rate as localtaxrate,
    update_dt effectivedate
  FROM stream(bronzecustomer) c
  JOIN tpcdi_incremental_staging_${scale_factor}.TaxRate r_lcl 
    ON c.lcl_tx_id = r_lcl.TX_ID
  JOIN tpcdi_incremental_staging_${scale_factor}.TaxRate r_nat 
    ON c.nat_tx_id = r_nat.TX_ID  
)
KEYS
  (customerid)
SEQUENCE BY
  effectivedate
STORED AS
  SCD TYPE 2;

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE dimaccount (
  sk_accountid BIGINT COMMENT 'Surrogate key for AccountID', 
  accountid BIGINT COMMENT 'Customer account identifier', 
  sk_brokerid BIGINT COMMENT 'Surrogate key of managing broker', 
  sk_customerid BIGINT COMMENT 'Surrogate key of customer', 
  customerid BIGINT COMMENT 'Customer identifier',
  accountdesc STRING COMMENT 'Name of customer account', 
  taxstatus TINYINT COMMENT 'Tax status of this account', 
  status STRING COMMENT 'Account status, active or closed', 
  iscurrent BOOLEAN GENERATED ALWAYS AS (nvl2(__END_AT, false, true)) COMMENT 'True if this is the current record', 
  effectivedate DATE COMMENT 'Beginning of date range when this record was the current record', 
  enddate DATE GENERATED ALWAYS AS (nvl(__END_AT, date('9999-12-31'))) COMMENT 'Ending of date range when this record was the current record. A record that is not expired will use the date 9999-12-31.', 
  __START_AT DATE COMMENT 'Beginning of date range when this record was the current record', 
  __END_AT DATE COMMENT 'Ending of date range when this record was the current record.'
)
PARTITIONED BY (iscurrent);

-- COMMAND ----------

CREATE FLOW dimaccount_incremental AS AUTO CDC INTO
  dimaccount
FROM (
  SELECT
    bigint(concat(date_format(a.update_dt, 'yyyyMMdd'), a.accountid)) sk_accountid,
    accountid,
    brokerid sk_brokerid,
    dc.sk_customerid,
    a.customerid,
    accountdesc,
    taxstatus,
    decode(a.status, 
      'ACTV',	'Active',
      'CMPT','Completed',
      'CNCL','Canceled',
      'PNDG','Pending',
      'SBMT','Submitted',
      'INAC','Inactive',
      a.status) status,
    update_dt effectivedate
  FROM stream(bronzeaccount) a
  JOIN dimcustomer dc
    ON 
      dc.iscurrent
      and dc.customerid = a.customerid
)
KEYS
  (accountid)
SEQUENCE BY
  effectivedate
STORED AS
  SCD TYPE 2;

-- COMMAND ----------

CREATE FLOW account_updates_from_customers AS AUTO CDC INTO
  dimaccount
FROM (
  with accounts as (
    select *
    FROM bronzeaccount
    QUALIFY row_number() over (partition by accountid order by update_dt desc)
  )
  SELECT
    bigint(concat(date_format(c.update_dt, 'yyyyMMdd'), a.accountid)) sk_accountid,
    a.accountid, 
    a.brokerid sk_brokerid,
    bigint(concat(date_format(c.update_dt, 'yyyyMMdd'), c.customerid)) sk_customerid,
    c.customerid,
    a.accountdesc,
    a.taxstatus,
    decode(a.status, 
      'ACTV',	'Active',
      'CMPT','Completed',
      'CNCL','Canceled',
      'PNDG','Pending',
      'SBMT','Submitted',
      'INAC','Inactive',
      a.status) status,
    c.update_dt effectivedate
  FROM stream(bronzecustomer) c
  JOIN accounts a
    ON 
      c.customerid = a.customerid
      and c.update_dt > a.update_dt
  WHERE
    c.cdc_flag = 'U'  
)
KEYS
  (accountid)
SEQUENCE BY
  effectivedate
STORED AS
  SCD TYPE 2

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE dimtrade (
  tradeid BIGINT NOT NULL COMMENT 'Trade identifier',
  sk_brokerid BIGINT COMMENT 'Surrogate key for BrokerID',
  sk_createdateid BIGINT COMMENT 'Surrogate key for date created',
  sk_createtimeid BIGINT COMMENT 'Surrogate key for time created',
  sk_closedateid BIGINT COMMENT 'Surrogate key for date closed',
  sk_closetimeid BIGINT COMMENT 'Surrogate key for time closed',
  status STRING COMMENT 'Trade status',
  type STRING COMMENT 'Trade type',
  cashflag BOOLEAN COMMENT 'Is this trade a cash or margin trade?',
  sk_securityid BIGINT COMMENT 'Surrogate key for SecurityID',
  sk_companyid BIGINT COMMENT 'Surrogate key for CompanyID',
  quantity INT COMMENT 'Quantity of securities traded.',
  bidprice DOUBLE COMMENT 'The requested unit price.',
  sk_customerid BIGINT COMMENT 'Surrogate key for CustomerID',
  sk_accountid BIGINT COMMENT 'Surrogate key for AccountID',
  executedby STRING COMMENT 'Name of person executing the trade.',
  tradeprice DOUBLE COMMENT 'Unit price at which the security was traded.',
  fee DOUBLE COMMENT 'Fee charged for placing this trade request',
  commission DOUBLE COMMENT 'Commission earned on this trade',
  tax DOUBLE COMMENT 'Amount of tax due on this trade'
)
PARTITIONED BY (sk_closedateid);

-- COMMAND ----------

CREATE FLOW dimtrade_incremental AS AUTO CDC INTO
  dimtrade
FROM (
  SELECT 
    t.tradeid,
    da.sk_brokerid,
    case when t.cdc_flag = "I" THEN bigint(date_format(t.t_dts, 'yyyyMMdd')) END sk_createdateid,
    case when t.cdc_flag = "I" THEN bigint(date_format(t.t_dts, 'HHmmss')) END sk_createtimeid,
    case when t.status IN ("CMPT", "CNCL") THEN bigint(date_format(t.t_dts, 'yyyyMMdd')) END sk_closedateid,
    case when t.status IN ("CMPT", "CNCL") THEN bigint(date_format(t.t_dts, 'HHmmss')) END sk_closetimeid,
    decode(t.status, 
      'ACTV',	'Active',
      'CMPT','Completed',
      'CNCL','Canceled',
      'PNDG','Pending',
      'SBMT','Submitted',
      'INAC','Inactive') status,
    decode(t.t_tt_id,
      'TMB', 'Market Buy',
      'TMS', 'Market Sell',
      'TSL', 'Stop Loss',
      'TLS', 'Limit Sell',
      'TLB', 'Limit Buy') type,
    if(t.cashflag = 1, TRUE, FALSE) cashflag,
    ds.sk_securityid,
    ds.sk_companyid,
    t.quantity,
    t.bidprice,
    da.sk_customerid,
    da.sk_accountid,
    t.executedby,
    t.tradeprice,
    t.fee,
    t.commission,
    t.tax,
    t.t_dts
  FROM STREAM(bronzetrade) t
  JOIN dimaccount da
    ON 
      t.t_ca_id = da.accountid 
      AND da.iscurrent
  JOIN tpcdi_incremental_staging_${scale_factor}.dimsecurity ds
    ON 
      ds.symbol = t.t_s_symb
      AND date(t_dts) >= ds.effectivedate 
      AND date(t_dts) < ds.enddate
)
KEYS (tradeid)
IGNORE NULL UPDATES
SEQUENCE BY t_dts
COLUMNS * EXCEPT (t_dts)
STORED AS SCD TYPE 1

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE factholdings (
  tradeid BIGINT COMMENT 'Key for Orignial Trade Indentifier',
  currenttradeid BIGINT COMMENT 'Key for the current trade',
  sk_customerid BIGINT COMMENT 'Surrogate key for Customer Identifier',
  sk_accountid BIGINT COMMENT 'Surrogate key for Account Identifier',
  sk_securityid BIGINT COMMENT 'Surrogate key for Security Identifier',
  sk_companyid BIGINT COMMENT 'Surrogate key for Company Identifier',
  sk_dateid BIGINT COMMENT 'Surrogate key for the date associated with the current trade',
  sk_timeid BIGINT COMMENT 'Surrogate key for the time associated with the current trade',
  currentprice DOUBLE COMMENT 'Unit price of this security for the current trade',
  currentholding INT COMMENT 'Quantity of a security held after the current trade.'
)
PARTITIONED BY (sk_dateid);

-- COMMAND ----------

CREATE FLOW factholdings_incremental
AS INSERT INTO factholdings BY NAME
SELECT
  h.hh_h_t_id tradeid,
  h.hh_t_id currenttradeid,
  t.sk_customerid,
  t.sk_accountid,
  t.sk_securityid,
  t.sk_companyid,
  t.sk_closedateid sk_dateid,
  t.sk_closetimeid sk_timeid,
  t.tradeprice currentprice,
  h.hh_after_qty currentholding
FROM STREAM(bronzeholdings) h
JOIN dimtrade t 
  ON 
    t.tradeid = h.hh_h_t_id
    and t.sk_closedateid = bigint(date_format(h.event_dt, 'yyyyMMdd'))

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE factwatches (
  sk_customerid BIGINT COMMENT 'Surrogate Key of the customer associated with watch list',
  sk_securityid BIGINT COMMENT 'Surrogate Key of the security listed on watch list',
  customerid BIGINT COMMENT 'Customer associated with watch list',
  symbol STRING COMMENT 'Security listed on watch list',
  sk_dateid_dateplaced BIGINT COMMENT 'Date the watch list item was added',
  sk_dateid_dateremoved BIGINT COMMENT 'Date the watch list item was removed',
  removed BOOLEAN COMMENT 'True if this watch has been removed'
)
PARTITIONED BY (removed);

-- COMMAND ----------

CREATE FLOW factwatches_incremental AS AUTO CDC INTO factwatches
FROM (
  SELECT
    c.sk_customerid sk_customerid,
    s.sk_securityid sk_securityid,
    w.w_c_id customerid,
    w.w_s_symb symbol,
    case when w_action != 'CNCL' then BIGINT(date_format(w_dts, 'yyyyMMdd')) end sk_dateid_dateplaced,
    case when w_action = 'CNCL' then BIGINT(date_format(w_dts, 'yyyyMMdd')) end sk_dateid_dateremoved,
    if(w_action = 'CNCL', True, False) removed,
    w.w_dts
  from STREAM(bronzewatches) w
  JOIN tpcdi_incremental_staging_${scale_factor}.dimsecurity s 
    ON 
      s.symbol = w.w_s_symb
      AND s.iscurrent
  JOIN dimcustomer c 
    ON
      w.w_c_id = c.customerid
      AND c.iscurrent
)
KEYS (customerid, symbol)
IGNORE NULL UPDATES
SEQUENCE BY w_dts
COLUMNS * except(w_dts)
STORED AS SCD TYPE 1;

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW factmarkethistorystg AS 
select 
  dm_s_symb, 
  array_sort(
    collect_list(
      struct(
        dm_date,
        dm_high,
        dm_low
      )
    )
  ) date_high_low
  from bronzedailymarket src
  where dm_date >= (select date_sub(max(dm_date), 380) from bronzedailymarket)
  group by all;

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE factmarkethistory (
  sk_securityid BIGINT COMMENT 'Surrogate key for SecurityID',
  sk_companyid BIGINT COMMENT 'Surrogate key for CompanyID',
  sk_dateid BIGINT COMMENT 'Surrogate key for the date',
  peratio DOUBLE COMMENT 'Price to earnings per share ratio',
  yield DOUBLE COMMENT 'Dividend to price ratio, as a percentage',
  fiftytwoweekhigh DOUBLE COMMENT 'Security highest price in last 52 weeks from this day',
  sk_fiftytwoweekhighdate BIGINT COMMENT 'Earliest date on which the 52 week high price was set',
  fiftytwoweeklow DOUBLE COMMENT 'Security lowest price in last 52 weeks from this day',
  sk_fiftytwoweeklowdate BIGINT COMMENT 'Earliest date on which the 52 week low price was set',
  closeprice DOUBLE COMMENT 'Security closing price on this day',
  dayhigh DOUBLE COMMENT 'Highest price for the security on this day',
  daylow DOUBLE COMMENT 'Lowest price for the security on this day',
  volume INT COMMENT 'Trading volume of the security on this day'
)
PARTITIONED BY (sk_dateid) AS 
with dm as (
  select 
    dm.dm_date,
    dm.dm_s_symb, 
    dm.dm_close,
    dm.dm_high,
    dm.dm_low,
    dm.dm_vol,
    filter(
      fmh.date_high_low,
      x -> x.dm_date between date_sub(dm.dm_date, 365) and dm.dm_date
    ) date_high_low
  from STREAM(bronzedailymarket) dm
  join factmarkethistorystg fmh
    on 
      dm.dm_s_symb = fmh.dm_s_symb
)
SELECT 
  s.sk_securityid,
  s.sk_companyid,
  bigint(date_format(dm.dm_date, 'yyyyMMdd')) sk_dateid,
  try_divide(dm.dm_close, f.prev_year_basic_eps) AS peratio,
  (try_divide(s.dividend, dm.dm_close)) / 100 yield,
  array_max(dm.date_high_low.dm_high) fiftytwoweekhigh,
  bigint(date_format(get(dm.date_high_low.dm_date, array_position(dm.date_high_low.dm_high, fiftytwoweekhigh) - 1), 'yyyyMMdd')) sk_fiftytwoweekhighdate,
  array_min(dm.date_high_low.dm_low) fiftytwoweeklow,
  bigint(date_format(get(dm.date_high_low.dm_date, array_position(dm.date_high_low.dm_low, fiftytwoweeklow) - 1), 'yyyyMMdd')) sk_fiftytwoweeklowdate,
  dm.dm_close closeprice,
  dm.dm_high dayhigh,
  dm.dm_low daylow,
  dm.dm_vol volume  
FROM dm
JOIN tpcdi_incremental_staging_${scale_factor}.dimsecurity s 
  ON 
    s.symbol = dm.dm_s_symb
    AND dm.dm_date >= s.effectivedate 
    AND dm.dm_date < s.enddate
LEFT JOIN tpcdi_incremental_staging_${scale_factor}.companyyeareps f 
  ON 
    f.sk_companyid = s.sk_companyid
    AND quarter(dm.dm_date) = quarter(f.qtr_start_date)
    AND year(dm.dm_date) = year(f.qtr_start_date);

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW dailytransactionstotals (
  accountid BIGINT COMMENT 'Customer account identifier', 
  totalcashtransactions DECIMAL(15,2) COMMENT 'Cash transactions totals for the day',
  ct_date DATE COMMENT 'Date of the transactions'
)
PARTITIONED BY (ct_date) AS
SELECT
  accountid,
  cast(sum(ct_amt) as DECIMAL(15,2)) totalcashtransactions,
  event_dt ct_date
FROM bronzecashtransaction
GROUP BY ALL;

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW factcashbalances (
  sk_customerid BIGINT NOT NULL COMMENT 'Surrogate key for CustomerID',
  sk_accountid BIGINT NOT NULL COMMENT 'Surrogate key for AccountID',
  sk_dateid BIGINT NOT NULL COMMENT 'Surrogate key for the date',
  cash DECIMAL(25,2) COMMENT 'Cash balance for the account after applying'
)
PARTITIONED BY (sk_dateid) AS 
SELECT 
  a.sk_customerid, 
  a.sk_accountid, 
  bigint(date_format(c.ct_date, 'yyyyMMdd')) sk_dateid,
  sum(c.totalcashtransactions) OVER (partition by c.accountid order by c.ct_date) cash
FROM dailytransactionstotals c
JOIN dimaccount a 
  ON 
    c.accountid = a.accountid
    AND c.ct_date >= a.effectivedate 
    AND c.ct_date < a.enddate