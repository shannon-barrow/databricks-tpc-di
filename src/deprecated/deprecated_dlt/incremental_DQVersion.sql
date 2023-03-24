-- Databricks notebook source
-- MAGIC %md
-- MAGIC # This notebook is a Duplicate of the incremental notebook, except this version has some tables with constraints added
-- MAGIC * If you want to add data quality checks to the pipeline, use this version of the incremental notebook instead

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # FactMarketHistory

-- COMMAND ----------

CREATE OR REFRESH TEMPORARY LIVE TABLE tempDailyMarketHistorical AS SELECT
  dmh.*,
  sk_dateid,
  min(dm_low) OVER (
    PARTITION BY dm_s_symb
    ORDER BY dm_date ASC ROWS BETWEEN 364 PRECEDING AND CURRENT ROW
  ) fiftytwoweeklow,
  max(dm_high) OVER (
    PARTITION by dm_s_symb
    ORDER BY dm_date ASC ROWS BETWEEN 364 PRECEDING AND CURRENT ROW
  ) fiftytwoweekhigh
FROM LIVE.DailyMarketHistorical dmh
JOIN LIVE.DimDate d 
  ON d.datevalue = dm_date;

-- COMMAND ----------

CREATE OR REFRESH TEMPORARY LIVE TABLE TempFactMarketHistory AS SELECT
  * FROM (
    SELECT 
      a.*,
      b.sk_dateid AS sk_fiftytwoweeklowdate,
      c.sk_dateid AS sk_fiftytwoweekhighdate
    FROM
      LIVE.tempDailyMarketHistorical a
    JOIN LIVE.tempDailyMarketHistorical b 
      ON
        a.dm_s_symb = b.dm_s_symb
        AND a.fiftytwoweeklow = b.dm_low
        AND b.dm_date between add_months(a.dm_date, -12) AND a.dm_date
    JOIN LIVE.tempDailyMarketHistorical c 
      ON 
        a.dm_s_symb = c.dm_s_symb
        AND a.fiftytwoweekhigh = c.dm_high
        AND c.dm_date between add_months(a.dm_date, -12) AND a.dm_date) dmh
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY dm_s_symb, dm_date 
    ORDER BY sk_fiftytwoweeklowdate, sk_fiftytwoweekhighdate) = 1;

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE FactMarketHistory (
  ${factmarkethistoryschema}
  CONSTRAINT valid_security_symbol EXPECT (sk_securityid IS NOT NULL),
  CONSTRAINT no_earnings_for_company EXPECT (PERatio IS NOT NULL)) AS
SELECT 
  s.sk_securityid,
  s.sk_companyid,
  sk_dateid,
  fmh.dm_close / sum_fi_basic_eps AS peratio,
  (s.dividend / fmh.dm_close) / 100 yield,
  fiftytwoweekhigh,
  sk_fiftytwoweekhighdate,
  fiftytwoweeklow,
  sk_fiftytwoweeklowdate,
  dm_close closeprice,
  dm_high dayhigh,
  dm_low daylow,
  dm_vol volume,
  fmh.batchid
FROM LIVE.TempFactMarketHistory fmh
LEFT JOIN LIVE.DimSecurity s 
  ON 
    s.symbol = fmh.dm_s_symb
    AND fmh.dm_date >= s.effectivedate 
    AND fmh.dm_date < s.enddate
LEFT JOIN (
  SELECT
    sk_companyid,
    fi_qtr_start_date,
    sum(fi_basic_eps) OVER (PARTITION BY companyid ORDER BY fi_qtr_start_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) - fi_basic_eps sum_fi_basic_eps
  FROM LIVE.Financial
  JOIN LIVE.DimCompany
	USING (sk_companyid)) f 
  ON 
    f.sk_companyid = s.sk_companyid
    AND quarter(fmh.dm_date) = quarter(fi_qtr_start_date)
    AND year(fmh.dm_date) = year(fi_qtr_start_date)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # DimCustomer

-- COMMAND ----------

CREATE LIVE VIEW CustomersUnionedView AS SELECT
  C_ID customerid,
  C_TAX_ID taxid,
  decode(_ActionType,
    'NEW', 'Active',
    'UPDCUST', 'Active',
    'INACT', 'Inactive') as status,
  C_L_NAME lastname,
  C_F_NAME firstname,
  C_M_NAME middleinitial,
  CASE
      WHEN C_GNDR IN ('M', 'F') THEN C_GNDR
      ELSE 'U'
    END as gender,
  C_TIER tier,
  C_DOB dob,
  C_ADLINE1 addressline1,
  C_ADLINE2 addressline2,
  C_ZIPCODE postalcode,
  C_CITY city,
  C_STATE_PROV stateprov,
  C_CTRY country,
  CASE
    WHEN isnull(c_local_1) then c_local_1
    ELSE concat(
      nvl2(c_ctry_1, '+' || c_ctry_1 || ' ', ''),
      nvl2(c_area_1, '(' || c_area_1 || ') ', ''),
      c_local_1,
      nvl(c_ext_1, '')) END as phone1,
  CASE
    WHEN isnull(c_local_2) then c_local_2
    ELSE concat(
      nvl2(c_ctry_2, '+' || c_ctry_2 || ' ', ''),
      nvl2(c_area_2, '(' || c_area_2 || ') ', ''),
      c_local_2,
      nvl(c_ext_2, '')) END as phone2,
  CASE
    WHEN isnull(c_local_3) then c_local_3
    ELSE concat(
      nvl2(c_ctry_3, '+' || c_ctry_3 || ' ', ''),
      nvl2(c_area_3, '(' || c_area_3 || ') ', ''),
      c_local_3,
      nvl(c_ext_3, '')) END as phone3,
  C_PRIM_EMAIL email1,
  C_ALT_EMAIL email2,
  C_LCL_TX_ID lcl_tx_id,
  C_NAT_TX_ID nat_tx_id,
  batchid,
  _ActionTS,
  to_date(_ActionTS) effectivedate
FROM ${staging_db}.CustomerMgmt
WHERE
  _ActionType in ('NEW', 'INACT', 'UPDCUST')
  AND C_ID IS NOT NULL
UNION ALL
SELECT
  c.customerid,
  nullif(c.taxid, '') taxid,
  nullif(s.st_name, '') as status,
  nullif(c.lastname, '') lastname,
  nullif(c.firstname, '') firstname,
  nullif(c.middleinitial, '') middleinitial,
  CASE
    WHEN UPPER(c.gender) IN ('M', 'F') THEN UPPER(c.gender)
    ELSE 'U'
    END AS gender,
  c.tier,
  c.dob,
  nullif(c.addressline1, '') addressline1,
  nullif(c.addressline2, '') addressline2,
  nullif(c.postalcode, '') postalcode,
  nullif(c.city, '') city,
  nullif(c.stateprov, '') stateprov,
  nullif(c.country, '') country,
  CASE
    WHEN isnull(c_local_1) then c_local_1
    ELSE concat(
      nvl2(c_ctry_1, '+' || c_ctry_1 || ' ', ''),
      nvl2(c_area_1, '(' || c_area_1 || ') ', ''),
      c_local_1,
      nvl(c_ext_1, '')) END as phone1,
  CASE
    WHEN isnull(c_local_2) then c_local_2
    ELSE concat(
      nvl2(c_ctry_2, '+' || c_ctry_2 || ' ', ''),
      nvl2(c_area_2, '(' || c_area_2 || ') ', ''),
      c_local_2,
      nvl(c_ext_2, '')) END as phone2,
  CASE
    WHEN isnull(c_local_3) then c_local_3
    ELSE concat(
      nvl2(c_ctry_3, '+' || c_ctry_3 || ' ', ''),
      nvl2(c_area_3, '(' || c_area_3 || ') ', ''),
      c_local_3,
      nvl(c_ext_3, '')) END as phone3,
  nullif(c.email1, '') email1,
  nullif(c.email2, '') email2,
  c.LCL_TX_ID, 
  c.NAT_TX_ID,
  c.batchid,
  timestamp(bd.batchdate) _ActionTS,
  bd.batchdate effectivedate
FROM LIVE.CustomerRaw c
JOIN LIVE.BatchDate bd
  ON c.batchid = bd.batchid
JOIN LIVE.StatusType s 
  ON c.status = s.st_id

-- COMMAND ----------

CREATE OR REFRESH TEMPORARY LIVE TABLE DimCustomerTemp AS SELECT
  customerid,
  _ActionTS,
  coalesce(taxid, last_value(taxid) IGNORE NULLS OVER (
      PARTITION BY customerid
      ORDER BY _ActionTS)) taxid,
  status,
  coalesce(lastname, last_value(lastname) IGNORE NULLS OVER (
      PARTITION BY customerid
      ORDER BY _ActionTS)) lastname,
  coalesce(firstname, last_value(firstname) IGNORE NULLS OVER (
      PARTITION BY customerid
      ORDER BY _ActionTS)) firstname,
  coalesce(middleinitial, last_value(middleinitial) IGNORE NULLS OVER (
      PARTITION BY customerid
      ORDER BY _ActionTS)) middleinitial,
  coalesce(gender, last_value(gender) IGNORE NULLS OVER (
      PARTITION BY customerid
      ORDER BY _ActionTS)) gender,
  coalesce(tier, last_value(tier) IGNORE NULLS OVER (
      PARTITION BY customerid
      ORDER BY _ActionTS)) tier,
  coalesce(dob, last_value(dob) IGNORE NULLS OVER (
      PARTITION BY customerid
      ORDER BY _ActionTS)) dob,
  coalesce(addressline1, last_value(addressline1) IGNORE NULLS OVER (
      PARTITION BY customerid
      ORDER BY _ActionTS)) addressline1,
  coalesce(addressline2, last_value(addressline2) IGNORE NULLS OVER (
      PARTITION BY customerid
      ORDER BY _ActionTS)) addressline2,
  coalesce(postalcode, last_value(postalcode) IGNORE NULLS OVER (
      PARTITION BY customerid
      ORDER BY _ActionTS)) postalcode,
  coalesce(CITY, last_value(CITY) IGNORE NULLS OVER (
      PARTITION BY customerid
      ORDER BY _ActionTS)) CITY,
  coalesce(stateprov, last_value(stateprov) IGNORE NULLS OVER (
      PARTITION BY customerid
      ORDER BY _ActionTS)) stateprov,
  coalesce(country, last_value(country) IGNORE NULLS OVER (
      PARTITION BY customerid
      ORDER BY _ActionTS)) country,
  coalesce(phone1, last_value(phone1) IGNORE NULLS OVER (
      PARTITION BY customerid
      ORDER BY _ActionTS)) phone1,
  coalesce(phone2, last_value(phone2) IGNORE NULLS OVER (
      PARTITION BY customerid
      ORDER BY _ActionTS)) phone2,
  coalesce(phone3, last_value(phone3) IGNORE NULLS OVER (
      PARTITION BY customerid
      ORDER BY _ActionTS)) phone3,
  coalesce(email1, last_value(email1) IGNORE NULLS OVER (
      PARTITION BY customerid
      ORDER BY _ActionTS)) email1,
  coalesce(email2, last_value(email2) IGNORE NULLS OVER (
      PARTITION BY customerid
      ORDER BY _ActionTS)) email2,
  coalesce(LCL_TX_ID, last_value(LCL_TX_ID) IGNORE NULLS OVER (
      PARTITION BY customerid
      ORDER BY _ActionTS)) LCL_TX_ID,
  coalesce(NAT_TX_ID, last_value(NAT_TX_ID) IGNORE NULLS OVER (
      PARTITION BY customerid
      ORDER BY _ActionTS)) NAT_TX_ID,
  batchid,
  case
    when lead(_ActionTS) OVER (PARTITION BY customerid ORDER BY _ActionTS) is null then true
    else false end iscurrent,
  effectivedate,
  coalesce(lead(effectivedate) OVER (PARTITION BY customerid ORDER BY _ActionTS), date('9999-12-31')) enddate
FROM LIVE.CustomersUnionedView c

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE DimCustomer (
  ${dimcustomerschema}
  CONSTRAINT invalid_customer_tier EXPECT (tier IN (1,2,3) AND tier is NOT null),
  CONSTRAINT enddate_effectivedate_not_equal EXPECT (enddate != effectivedate) ON VIOLATION DROP ROW)
  PARTITIONED BY (iscurrent) AS 
SELECT 
  monotonically_increasing_id() as sk_customerid,
  c.customerid,
  c.taxid,
  c.status,
  c.lastname,
  c.firstname,
  c.middleinitial,
  c.gender,
  c.tier,
  c.dob,
  c.addressline1,
  c.addressline2,
  c.postalcode,
  c.city,
  c.stateprov,
  c.country,
  c.phone1,
  c.phone2,
  c.phone3,
  c.email1,
  c.email2,
  r_nat.TX_NAME as nationaltaxratedesc,
  r_nat.TX_RATE as nationaltaxrate,
  r_lcl.TX_NAME as localtaxratedesc,
  r_lcl.TX_RATE as localtaxrate,
  p.agencyid,
  p.creditrating,
  p.networth,
  p.marketingnameplate,
  c.iscurrent,
  c.batchid,
  c.effectivedate,
  c.enddate
FROM LIVE.DimCustomerTemp c
JOIN LIVE.TaxRate r_lcl 
  ON c.LCL_TX_ID = r_lcl.TX_ID
JOIN LIVE.TaxRate r_nat 
  ON c.NAT_TX_ID = r_nat.TX_ID
LEFT JOIN LIVE.Prospect p 
  on upper(p.lastname) = upper(c.lastname)
  and upper(p.firstname) = upper(c.firstname)
  and upper(p.addressline1) = upper(c.addressline1)
  and upper(nvl(p.addressline2, '')) = upper(nvl(c.addressline2, ''))
  and upper(p.postalcode) = upper(c.postalcode)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Prospect

-- COMMAND ----------

CREATE OR REFRESH TEMPORARY LIVE TABLE ProspectTemp AS SELECT 
  * FROM (
  SELECT
    agencyid,
    max(batchid) recordbatchid,
    lastname,
    firstname,
    middleinitial,
    gender,
    addressline1,
    addressline2,
    postalcode,
    city,
    state,
    country,
    phone,
    income,
    numbercars,
    numberchildren,
    maritalstatus,
    age,
    creditrating,
    ownorrentflag,
    employer,
    numbercreditcards,
    networth,
    min(batchid) batchid
  FROM LIVE.ProspectRaw p
  GROUP BY
    agencyid,
    lastname,
    firstname,
    middleinitial,
    gender,
    addressline1,
    addressline2,
    postalcode,
    city,
    state,
    country,
    phone,
    income,
    numbercars,
    numberchildren,
    maritalstatus,
    age,
    creditrating,
    ownorrentflag,
    employer,
    numbercreditcards,
    networth)
QUALIFY ROW_NUMBER() OVER (PARTITION BY agencyid ORDER BY batchid DESC) = 1

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE Prospect (${prospectschema}) AS SELECT 
  agencyid,
  recdate.sk_dateid sk_recorddateid,
  origdate.sk_dateid sk_updatedateid,
  p.batchid,
  nvl2(c.customerid, True, False) iscustomer, 
  p.lastname,
  p.firstname,
  p.middleinitial,
  p.gender,
  p.addressline1,
  p.addressline2,
  p.postalcode,
  city,
  state,
  country,
  phone,
  income,
  numbercars,
  numberchildren,
  maritalstatus,
  age,
  creditrating,
  ownorrentflag,
  employer,
  numbercreditcards,
  networth,
  if(
    isnotnull(
      if(networth > 1000000 or income > 200000,"HighValue+","") || 
      if(numberchildren > 3 or numbercreditcards > 5,"Expenses+","") ||
      if(age > 45, "Boomer+", "") ||
      if(income < 50000 or creditrating < 600 or networth < 100000, "MoneyAlert+","") ||
      if(numbercars > 3 or numbercreditcards > 7, "Spender+","") ||
      if(age < 25 and networth > 1000000, "Inherited+","")),
    left(
      if(networth > 1000000 or income > 200000,"HighValue+","") || 
      if(numberchildren > 3 or numbercreditcards > 5,"Expenses+","") ||
      if(age > 45, "Boomer+", "") ||
      if(income < 50000 or creditrating < 600 or networth < 100000, "MoneyAlert+","") ||
      if(numbercars > 3 or numbercreditcards > 7, "Spender+","") ||
      if(age < 25 and networth > 1000000, "Inherited+",""),
      length(
        if(networth > 1000000 or income > 200000,"HighValue+","") || 
        if(numberchildren > 3 or numbercreditcards > 5,"Expenses+","") ||
        if(age > 45, "Boomer+", "") ||
        if(income < 50000 or creditrating < 600 or networth < 100000, "MoneyAlert+","") ||
        if(numbercars > 3 or numbercreditcards > 7, "Spender+","") ||
        if(age < 25 and networth > 1000000, "Inherited+",""))
      -1),
    NULL) marketingnameplate
FROM LIVE.ProspectTemp p
JOIN (
  SELECT 
    sk_dateid,
    batchid
  FROM LIVE.BatchDate b 
  JOIN LIVE.DimDate d 
    ON b.batchdate = d.datevalue) recdate
  ON p.recordbatchid = recdate.batchid
JOIN (
  SELECT 
    sk_dateid,
    batchid
  FROM LIVE.BatchDate b 
  JOIN LIVE.DimDate d 
    ON b.batchdate = d.datevalue) origdate
  ON p.batchid = origdate.batchid
LEFT JOIN (
  SELECT 
    customerid,
    lastname,
    firstname,
    addressline1,
    addressline2,
    postalcode
  FROM 
    LIVE.DimCustomerTemp
  WHERE 
    iscurrent) c
  ON 
    upper(p.LastName) = upper(c.lastname)
    and upper(p.FirstName) = upper(c.firstname)
    and upper(p.AddressLine1) = upper(c.addressline1)
    and upper(nvl(p.addressline2, '')) = upper(nvl(c.addressline2, ''))
    and upper(p.PostalCode) = upper(c.postalcode)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # DimAccount

-- COMMAND ----------

CREATE LIVE View HistAccountView AS SELECT
  accountid,
  customerid,
  accountdesc,
  taxstatus,
  brokerid,
  decode(_ActionType,
    "NEW","Active",
    "ADDACCT","Active",
    "UPDACCT","Active",
    "UPDCUST","Active",
    "CLOSEACCT","Inactive",
    "INACT","Inactive") status,
  unix_seconds(_ActionTS) record_time,
  date(_ActionTS) effectivedate,
  1 batchid
FROM (
  SELECT
    CA_ID accountid,
    c_id customerid,
    CA_NAME accountdesc,
    CA_TAX_ST taxstatus,
    ca_b_id brokerid,
    _ActionTS,
    _ActionType
  FROM ${staging_db}.CustomerMgmt c
  WHERE _ActionType NOT IN ('UPDCUST', 'INACT')
  UNION ALL
  SELECT DISTINCT
    c.CA_ID accountid,
    u.c_id customerid,
    null accountdesc,
    null taxstatus,
    null brokerid,
    u._ActionTS,
    u._ActionType
  FROM (
    SELECT
      c_id,
      _ActionTS,
      _ActionType
    FROM ${staging_db}.CustomerMgmt c
    WHERE _ActionType IN ('UPDCUST', 'INACT')) u
  JOIN (
    SELECT
      c_id,
      ca_id,
      _ActionTS
    FROM ${staging_db}.CustomerMgmt c
    WHERE _ActionType NOT IN ('UPDCUST', 'CLOSEACCT', 'INACT')) c
    ON u.c_id = c.c_id
  WHERE
    u._ActionTS > c._ActionTS)

-- COMMAND ----------

CREATE LIVE VIEW AllAccountsView AS SELECT
  accountid,
  a.ca_c_id customerid,
  accountDesc,
  TaxStatus,
  a.ca_b_id brokerid,
  st_name as status,
  unix_seconds(TIMESTAMP(bd.batchdate)) + 1 record_time,
  bd.batchdate effectivedate,
  a.batchid
FROM LIVE.AccountRaw a
JOIN LIVE.BatchDate bd
  ON a.batchid = bd.batchid
JOIN LIVE.StatusType st 
  ON a.CA_ST_ID = st.st_id
UNION ALL
SELECT * FROM LIVE.HistAccountView

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## The Updated DimCustomer Surrogate Key Values Need to be carried to DimAccount
-- MAGIC * Surrogate Keys need referential integrity
-- MAGIC * Code has to go get DimCustoer current batch, match the CURRENT batches customerid to the PREVIOUS CURRENT customerid, take the previous sk_customerid
-- MAGIC * This is because the DimAccount table does not have a customerid, only the sk_customerid. So we need the OLD active surrogate key so we can update it to the NEW surrogate key

-- COMMAND ----------

CREATE LIVE VIEW AccountsUpdatedCustsView AS SELECT
  accountid,
  customerid,
  cast(null as STRING) accountDesc,
  cast(null as TINYINT) TaxStatus,
  cast(null as BIGINT) brokerid,
  cast(null as STRING) status,
  unix_seconds(TIMESTAMP(effectivedate)) record_time,
  effectivedate,
  batchid
FROM (
  SELECT
    a.accountid,
    c.customerid,
    c.batchid,
    bd.batchdate effectivedate
  FROM LIVE.AllAccountsView a
  JOIN LIVE.CustomerRaw c
    ON a.customerid = c.customerid
  JOIN LIVE.BatchDate bd
    ON c.batchid = bd.batchid
  WHERE
    a.batchid < c.batchid
  GROUP BY
    a.accountid,
    c.customerid,
    c.batchid,
    bd.batchdate)
UNION ALL
SELECT * 
FROM LIVE.AllAccountsView 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Quick note, DLT SCD Type 2 COULD go here in lieu of the coalescing and assigning enddate

-- COMMAND ----------

CREATE OR REFRESH TEMPORARY LIVE TABLE AccountTemp AS SELECT * FROM ( 
  SELECT
    a.accountid,
    a.customerid,
    a.accountdesc,
    a.TaxStatus,
    a.brokerid,
    a.status,
    nvl2(a.enddate, true, false) iscurrent,
    a.batchid,
    a.effectivedate,
    nvl(a.enddate, date('9999-12-31')) enddate
  FROM (
    SELECT
      accountid,
      customerid,
      coalesce(accountdesc, last_value(accountdesc) IGNORE NULLS OVER (
        PARTITION BY accountid ORDER BY record_time)) accountdesc,
      coalesce(taxstatus, last_value(taxstatus) IGNORE NULLS OVER (
        PARTITION BY accountid ORDER BY record_time)) taxstatus,
      coalesce(brokerid, last_value(brokerid) IGNORE NULLS OVER (
        PARTITION BY accountid ORDER BY record_time)) brokerid,
      coalesce(status, last_value(status) IGNORE NULLS OVER (
        PARTITION BY accountid ORDER BY record_time)) status,
      effectivedate,
      lead(effectivedate) OVER (PARTITION BY accountid ORDER BY record_time) enddate,
      batchid
    FROM LIVE.AccountsUpdatedCustsView) a )
WHERE effectivedate != enddate

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE DimAccount (
  ${dimaccountschema}
  CONSTRAINT valid_dimcustomer_join EXPECT (sk_customerid IS NOT NULL),
  CONSTRAINT valid_dimbroker_join EXPECT (sk_brokerid IS NOT NULL),
  CONSTRAINT enddate_effectivedate_not_equal EXPECT (enddate != effectivedate) ON VIOLATION DROP ROW) 
  PARTITIONED BY (iscurrent) AS 
SELECT
  monotonically_increasing_id() as sk_accountid,
  a.accountid,
  b.sk_brokerid,
  c.sk_customerid,
  a.accountdesc,
  a.TaxStatus,
  a.status,
  a.iscurrent,
  a.batchid,
  a.effectivedate,
  a.enddate
FROM LIVE.AccountTemp a
LEFT JOIN LIVE.DimCustomer c 
  ON 
    c.customerid = a.customerid
    AND a.EffectiveDate >= c.EffectiveDate 
    AND a.EndDate <= c.EndDate
LEFT JOIN LIVE.DimBroker b 
  ON a.brokerid = b.brokerid

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # DimTrade

-- COMMAND ----------

--CREATE OR REFRESH TEMPORARY LIVE TABLE AllTradesTemp 
CREATE LIVE VIEW AllTradesView AS SELECT
  t_id tradeid,
  th_dts t_dts,
  t_st_id,
  t_tt_id,
  t_is_cash,
  t_s_symb,
  t_qty AS quantity,
  t_bid_price AS bidprice,
  t_ca_id,
  t_exec_name AS executedby,
  t_trade_price AS tradeprice,
  t_chrg AS fee,
  t_comm AS commission,
  t_tax AS tax,
  batchid,
  CASE 
    WHEN (th_st_id == "SBMT" AND t_tt_id IN ("TMB", "TMS")) OR th_st_id = "PNDG" THEN true 
    ELSE false END AS create_flg,
  CASE 
    WHEN th_st_id IN ("CMPT", "CNCL") THEN true 
    ELSE false END AS close_flg  
FROM LIVE.TradeHistoryRaw th
JOIN LIVE.TradeBatch1 t
  ON th_t_id = t_id
UNION ALL
SELECT
  t_id tradeid,
  t_dts,
  t_st_id,
  t_tt_id,
  t_is_cash,
  t_s_symb,
  t_qty AS quantity,
  t_bid_price AS bidprice,
  t_ca_id,
  t_exec_name AS executedby,
  t_trade_price AS tradeprice,
  t_chrg AS fee,
  t_comm AS commission,
  t_tax AS tax,
  batchid,
  CASE WHEN cdc_flag = 'I' THEN true ELSE false END AS create_flg,
  CASE WHEN t_st_id IN ("CMPT", "CNCL") THEN true ELSE false END AS close_flg
FROM LIVE.TradeIncremental trade

-- COMMAND ----------

--CREATE OR REFRESH LIVE TABLE TradeTemp 
CREATE LIVE VIEW TradeView AS SELECT
  tradeid,
  t_dts,
  CASE WHEN create_flg then sk_dateid else cast(NULL AS BIGINT) END AS sk_createdateid,
  CASE WHEN create_flg then sk_timeid else cast(NULL AS BIGINT) END AS sk_createtimeid,
  CASE WHEN close_flg then sk_dateid else cast(NULL AS BIGINT) END AS sk_closedateid,
  CASE WHEN close_flg then sk_timeid else cast(NULL AS BIGINT) END AS sk_closetimeid,
  CASE 
    WHEN t_is_cash = 1 then TRUE
    WHEN t_is_cash = 0 then FALSE
    ELSE cast(null as BOOLEAN) END AS cashflag,
  t_st_id,
  t_tt_id,
  t_s_symb,
  quantity,
  bidprice,
  t_ca_id,
  executedby,
  tradeprice,
  fee,
  commission,
  tax,
  batchid
FROM LIVE.AllTradesView t
JOIN LIVE.DimDate dd
  ON to_date(t.t_dts) = dd.datevalue
JOIN LIVE.DimTime dt
  ON date_format(t.t_dts, 'HH:mm:ss') = dt.timevalue

-- COMMAND ----------

CREATE OR REFRESH TEMPORARY LIVE TABLE DimTradeTemp AS SELECT * FROM (
  SELECT
    tradeid,
    t_dts,
    min(t_dts) OVER (PARTITION BY tradeid) first_trade_ts,
    coalesce(sk_createdateid, last_value(sk_createdateid) IGNORE NULLS OVER (
      PARTITION BY tradeid ORDER BY t_dts)) sk_createdateid,
    coalesce(sk_createtimeid, last_value(sk_createtimeid) IGNORE NULLS OVER (
      PARTITION BY tradeid ORDER BY t_dts)) sk_createtimeid,
    coalesce(sk_closedateid, last_value(sk_closedateid) IGNORE NULLS OVER (
      PARTITION BY tradeid ORDER BY t_dts)) sk_closedateid,
    coalesce(sk_closetimeid, last_value(sk_closetimeid) IGNORE NULLS OVER (
      PARTITION BY tradeid ORDER BY t_dts)) sk_closetimeid,
    cashflag,
    t_st_id,
    t_tt_id,
    t_s_symb,
    quantity,
    bidprice,
    t_ca_id,
    executedby,
    tradeprice,
    fee,
    commission,
    tax,
    batchid
  FROM LIVE.TradeView)
QUALIFY ROW_NUMBER() OVER (PARTITION BY tradeid ORDER BY t_dts desc) = 1

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE DimTrade (
  ${dimtradeschema}
  CONSTRAINT valid_security_join  EXPECT (sk_securityid IS NOT NULL),
  CONSTRAINT valid_account_join  EXPECT (sk_accountid IS NOT NULL),
  CONSTRAINT invalid_trade_commission EXPECT (commission IS NULL OR commission <= tradeprice * quantity),
  CONSTRAINT invalid_trade_fee EXPECT (fee IS NULL OR fee <= tradeprice * quantity)) AS 
SELECT
  trade.tradeid,
  sk_brokerid,
  trade.sk_createdateid,
  trade.sk_createtimeid,
  trade.sk_closedateid,
  trade.sk_closetimeid,
  st_name status,
  tt_name type,
  trade.cashflag,
  sk_securityid,
  sk_companyid,
  trade.quantity,
  trade.bidprice,
  sk_customerid,
  sk_accountid,
  trade.executedby,
  trade.tradeprice,
  trade.fee,
  trade.commission,
  trade.tax,
  trade.batchid
FROM LIVE.DimTradeTemp trade
JOIN LIVE.StatusType status
  ON status.st_id = trade.t_st_id
JOIN LIVE.TradeType tt
  ON tt.tt_id == trade.t_tt_id
LEFT JOIN LIVE.DimSecurity ds
  ON 
    ds.symbol = trade.t_s_symb
    AND to_date(trade.first_trade_ts) >= ds.effectivedate 
    AND to_date(trade.first_trade_ts) < ds.enddate
LEFT JOIN LIVE.DimAccount da
  ON 
    trade.t_ca_id = da.accountid 
    AND to_date(trade.first_trade_ts) >= da.effectivedate 
    AND to_date(trade.first_trade_ts) < da.enddate

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # FactHoldings

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE FactHoldings (
  ${factholdingsschema}
  CONSTRAINT valid_trade_join EXPECT (currentprice IS NOT NULL)) 
AS SELECT 
  hh_h_t_id tradeid,
  hh_t_id currenttradeid,
  sk_customerid,
  sk_accountid,
  sk_securityid,
  sk_companyid,
  sk_closedateid sk_dateid,
  sk_closetimeid sk_timeid,
  tradeprice currentprice,
  hh_after_qty currentholding,
  hh.batchid
FROM LIVE.HoldingHistory hh
LEFT JOIN LIVE.DimTrade dt
  ON tradeid = hh_t_id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # FactCashBalances

-- COMMAND ----------

CREATE OR REFRESH TEMPORARY LIVE TABLE FactCashBalancesTemp AS SELECT
  c.accountid,
  d.sk_dateid, 
  c.datevalue,
  sum(account_daily_total) OVER (partition by c.accountid order by d.sk_dateid) cash,
  c.batchid
FROM (
  SELECT 
    ct_ca_id accountid,
    to_date(ct_dts) datevalue,
    sum(ct_amt) account_daily_total,
    batchid
  FROM LIVE.CashTransaction
  GROUP BY
    accountid,
    datevalue,
    batchid) c 
JOIN LIVE.DimDate d 
  ON c.datevalue = d.datevalue

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE FactCashBalances (${factcashbalancesschema}
  CONSTRAINT account_joined  EXPECT (sk_accountid IS NOT NULL)) 
AS SELECT 
  a.sk_customerid, 
  a.sk_accountid, 
  c.sk_dateid, 
  c.cash,
  c.batchid
FROM LIVE.FactCashBalancesTemp c
LEFT JOIN LIVE.DimAccount a 
  ON 
    c.accountid = a.accountid
    AND c.datevalue >= a.effectivedate 
    AND c.datevalue < a.enddate 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # FactWatches

-- COMMAND ----------

--CREATE OR REFRESH TEMPORARY LIVE TABLE FactWatchesHistoryTemp 
CREATE LIVE VIEW FactWatchesHistoryView AS SELECT 
  w_c_id customerid,
  w_s_symb symbol,
  w_action,
  d.sk_dateid,
  date(w_dts) dateplaced,
  wh.batchid
FROM LIVE.WatchHistory wh
JOIN LIVE.DimDate d
  ON d.datevalue = date(w_dts)

-- COMMAND ----------

CREATE OR REFRESH TEMPORARY LIVE TABLE FactWatchesTemp AS SELECT
  actv.customerid,
  actv.dateplaced,
  actv.symbol,
  actv.sk_dateid sk_dateid_dateplaced,
  cncl.sk_dateid sk_dateid_dateremoved,
  actv.batchid
FROM (SELECT * FROM LIVE.FactWatchesHistoryView WHERE w_action = 'ACTV') actv
LEFT JOIN (SELECT * FROM LIVE.FactWatchesHistoryView WHERE w_action = 'CNCL') cncl 
  ON
    actv.customerid = cncl.customerid
    AND actv.symbol = cncl.symbol

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE FactWatches (
  ${factwatchesschema}
  CONSTRAINT valid_symbol EXPECT (sk_securityid IS NOT NULL),
  CONSTRAINT valid_customer_id EXPECT (sk_customerid IS NOT NULL)) 
AS SELECT
  c.sk_customerid sk_customerid,
  s.sk_securityid sk_securityid,
  sk_dateid_dateplaced,
  sk_dateid_dateremoved,
  fw.batchid
FROM LIVE.FactWatchesTemp fw
LEFT JOIN LIVE.DimSecurity s 
  ON 
    s.symbol = fw.symbol
    AND fw.dateplaced >= s.effectivedate 
    AND fw.dateplaced < s.enddate
LEFT JOIN LIVE.DimCustomer c 
  ON
    fw.customerid = c.customerid
    AND fw.dateplaced >= c.effectivedate 
    AND fw.dateplaced < c.enddate

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # DImessages
-- MAGIC * Best approach seems to be union each of the alerts from various tables and load into DImessages

-- COMMAND ----------

-- CREATE LIVE VIEW SPratingAlertsView AS SELECT
--   CURRENT_TIMESTAMP() as MessageDateAndTime,
--   1 BatchID,
--   'DimCompany' MessageSource,
--   'Invalid SPRating' MessageText,
--   'Alert' as MessageType,
--   concat('CO_ID = ', cik, ', CO_SP_RATE = ', SPrating) MessageData
-- FROM LIVE.FinwireCmpView cmp
-- WHERE SPrating NOT IN ('AAA','AA','A','BBB','BB','B','CCC','CC','C','D','AA+','A+','BBB+','BB+','B+','CCC+','AA-','A-','BBB-','BB-','B-','CCC-') 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## DIMESSAGES IS A WIP! 
-- MAGIC * IF WE END UP USING IT, WE NEED TO GROUP BY BATCHID FOR EACH SUBQUERY SINCE IT WILL BE RUN AT THE END
-- MAGIC * THEN WINDOW WITH A SUM TO GET RUNNING TOTAL FOR EACH MESSAGESOURCE, MESSAGETEXT

-- COMMAND ----------

-- CREATE LIVE TABLE DIMessages (${dimessagesschema}) AS SELECT
--   CURRENT_TIMESTAMP() as MessageDateAndTime,
--   case
--     when BatchID is null then 0
--     else BatchID
--     end as BatchID,
--   MessageSource,
--   MessageText,
--   'Validation' as MessageType,
--   MessageData
-- from (
--     select
--       'DimAccount' as MessageSource,
--       'Row count' as MessageText,
--       count(*) as MessageData
--     from
--       {wh_db}.DimAccount
--     union
--     select
--       'DimBroker' as MessageSource,
--       'Row count' as MessageText,
--       count(*) as MessageData
--     from
--       {wh_db}.DimBroker
--     union
--     select
--       'DimCompany' as MessageSource,
--       'Row count' as MessageText,
--       count(*) as MessageData
--     from
--       {wh_db}.DimCompany
--     union
--     select
--       'DimCustomer' as MessageSource,
--       'Row count' as MessageText,
--       count(*) as MessageData
--     from
--       {wh_db}.DimCustomer
--     union
--     select
--       'DimDate' as MessageSource,
--       'Row count' as MessageText,
--       count(*) as MessageData
--     from
--       {wh_db}.DimDate
--     union
--     select
--       'DimSecurity' as MessageSource,
--       'Row count' as MessageText,
--       count(*) as MessageData
--     from
--       {wh_db}.DimSecurity
--     union
--     select
--       'DimTime' as MessageSource,
--       'Row count' as MessageText,
--       count(*) as MessageData
--     from
--       {wh_db}.DimTime
--     union
--     select
--       'DimTrade' as MessageSource,
--       'Row count' as MessageText,
--       count(*) as MessageData
--     from
--       {wh_db}.DimTrade
--     union
--     select
--       'FactCashBalances' as MessageSource,
--       'Row count' as MessageText,
--       count(*) as MessageData
--     from
--       {wh_db}.FactCashBalances
--     union
--     select
--       'FactHoldings' as MessageSource,
--       'Row count' as MessageText,
--       count(*) as MessageData
--     from
--       {wh_db}.FactHoldings
--     union
--     select
--       'FactMarketHistory' as MessageSource,
--       'Row count' as MessageText,
--       count(*) as MessageData
--     from
--       {wh_db}.FactMarketHistory
--     union
--     select
--       'FactWatches' as MessageSource,
--       'Row count' as MessageText,
--       count(*) as MessageData
--     from
--       {wh_db}.FactWatches
--     union
--     select
--       'Financial' as MessageSource,
--       'Row count' as MessageText,
--       count(*) as MessageData
--     from
--       {wh_db}.Financial
--     union
--     select
--       'Industry' as MessageSource,
--       'Row count' as MessageText,
--       count(*) as MessageData
--     from
--       {wh_db}.Industry
--     union
--     select
--       'Prospect' as MessageSource,
--       'Row count' as MessageText,
--       count(*) as MessageData
--     from
--       {wh_db}.Prospect
--     union
--     select
--       'StatusType' as MessageSource,
--       'Row count' as MessageText,
--       count(*) as MessageData
--     from
--       {wh_db}.StatusType
--     union
--     select
--       'TaxRate' as MessageSource,
--       'Row count' as MessageText,
--       count(*) as MessageData
--     from
--       {wh_db}.TaxRate
--     union
--     select
--       'TradeType' as MessageSource,
--       'Row count' as MessageText,
--       count(*) as MessageData
--     from
--       {wh_db}.TradeType
--     union
--     select
--       'FactCashBalances' as MessageSource,
--       'Row count joined' as MessageText,
--       count(*) as MessageData
--     from
--       {wh_db}.FactCashBalances f
--       inner join {wh_db}.DimAccount a on f.SK_AccountID = a.SK_AccountID
--       inner join {wh_db}.DimCustomer c on f.SK_CustomerID = c.SK_CustomerID
--       inner join {wh_db}.DimBroker b on a.SK_BrokerID = b.SK_BrokerID
--       inner join {wh_db}.DimDate d on f.SK_DateID = d.SK_DateID
--     union
--     select
--       'FactHoldings' as MessageSource,
--       'Row count joined' as MessageText,
--       count(*) as MessageData
--     from
--       {wh_db}.FactHoldings f
--       inner join {wh_db}.DimAccount a on f.SK_AccountID = a.SK_AccountID
--       inner join {wh_db}.DimCustomer c on f.SK_CustomerID = c.SK_CustomerID
--       inner join {wh_db}.DimBroker b on a.SK_BrokerID = b.SK_BrokerID
--       inner join {wh_db}.DimDate d on f.SK_DateID = d.SK_DateID
--       inner join {wh_db}.DimTime t on f.SK_TimeID = t.SK_TimeID
--       inner join {wh_db}.DimCompany m on f.SK_CompanyID = m.SK_CompanyID
--       inner join {wh_db}.DimSecurity s on f.SK_SecurityID = s.SK_SecurityID
--     union
--     select
--       'FactMarketHistory' as MessageSource,
--       'Row count joined' as MessageText,
--       count(*) as MessageData
--     from
--       {wh_db}.FactMarketHistory f
--       inner join {wh_db}.DimDate d on f.SK_DateID = d.SK_DateID
--       inner join {wh_db}.DimCompany m on f.SK_CompanyID = m.SK_CompanyID
--       inner join {wh_db}.DimSecurity s on f.SK_SecurityID = s.SK_SecurityID
--     union
--     select
--       'FactWatches' as MessageSource,
--       'Row count joined' as MessageText,
--       count(*) as MessageData
--     from
--       {wh_db}.FactWatches f
--       inner join {wh_db}.DimCustomer c on f.SK_CustomerID = c.SK_CustomerID
--       inner join {wh_db}.DimDate dp on f.SK_DateID_DatePlaced = dp.SK_DateID
--       inner join {wh_db}.DimSecurity s on f.SK_SecurityID = s.SK_SecurityID
--       /* Additional information used at Audit time */
--     union
--     select
--       'DimCustomer' as MessageSource,
--       'Inactive customers' as MessageText,
--       count(*)
--     from
--       {wh_db}.DimCustomer
--     where
--       IsCurrent = 1
--       and Status = 'Inactive'
--     union
--     select
--       'FactWatches' as MessageSource,
--       'Inactive watches' as MessageText,
--       count(*)
--     from
--       {wh_db}.FactWatches
--     where
--       SK_DATEID_DATEREMOVED is not null
--   )
-- ---------------------------------------------------
-- -- ADDING ALERTS FROM HERE
-- ---------------------------------------------------
-- UNION
-- SELECT
--   CURRENT_TIMESTAMP() as MessageDateAndTime,
--   case
--     when BatchID is null then 0
--     else BatchID
--     end as BatchID,
--   MessageSource,
--   MessageText,
--   'Alert' as MessageType,
--   MessageData
-- FROM (SELECT {batch_id} batchid)
-- CROSS JOIN (
--   SELECT 
--     'DimCustomer' MessageSource,
--     'Invalid customer tier' MessageText,
--     concat('C_ID = ', customerid, ', C_TIER = ', nvl(cast(tier as string), 'null')) MessageData
--   FROM (
--     SELECT 
--         customerid, 
--         tier
--     FROM {wh_db}.DimCustomer
--     WHERE 
--       batchid = {batch_id}
--       AND (
--         tier NOT IN (1,2,3)
--         OR tier is null)
--     QUALIFY ROW_NUMBER() OVER (PARTITION BY customerid ORDER BY enddate desc) = 1)
--   UNION
--   SELECT DISTINCT
--     'DimCustomer' MessageSource,
--     'DOB out of range' MessageText,
--     concat('C_ID = ', customerid, ', C_DOB = ', dob) MessageData
--   FROM {wh_db}.DimCustomer dc
--   JOIN {wh_db}.batchdate bd
--     USING (batchid)
--   WHERE
--     dc.batchid = {batch_id}
--     AND bd.batchid = {batch_id} 
--     AND (
--       datediff(YEAR, dob, batchdate) >= 100
--       OR dob > batchdate)
--   UNION
--   SELECT DISTINCT
--     'DimTrade' MessageSource,
--     'Invalid trade commission' MessageText,
--     concat('T_ID = ', tradeid, ', T_COMM = ', commission) MessageData
--   FROM {wh_db}.DimTrade
--   WHERE
--     batchid = {batch_id}
--     AND commission IS NOT NULL
--     AND commission > tradeprice * quantity
--   UNION
--   SELECT DISTINCT
--     'DimTrade' MessageSource,
--     'Invalid trade fee' MessageText,
--     concat('T_ID = ', tradeid, ', T_CHRG = ', fee) MessageData
--   FROM {wh_db}.DimTrade
--   WHERE
--     batchid = {batch_id}
--     AND fee IS NOT NULL
--     AND fee > tradeprice * quantity
--   UNION
--   SELECT DISTINCT
--     'FactMarketHistory' MessageSource,
--     'No earnings for company' MessageText,
--     concat('DM_S_SYMB = ', symbol) MessageData
--   FROM {wh_db}.FactMarketHistory fmh
--   JOIN {wh_db}.DimSecurity ds 
--     ON 
--       ds.sk_securityid = fmh.sk_securityid
--   WHERE
--     fmh.batchid = {batch_id}
--     AND PERatio IS NULL
-- )
