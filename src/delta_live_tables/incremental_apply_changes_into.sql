-- Databricks notebook source
-- MAGIC %md
-- MAGIC # DimCustomer

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **DimCustomer Table needs a Staging table first. Then create final Customer table and the dependent tables**
-- MAGIC * Since all updates only show the changed column, it needs to coalesce with previous record.  
-- MAGIC * Any other tables needing data from the Customer table then needs to wait to get the data until AFTER this fully realized record(s) have been created
-- MAGIC * This includes: 
-- MAGIC   * Prospect (Customer and Prospect need data from the other table and join on name/address - which you cannot get until data is coalesced)
-- MAGIC   * Account table needs each change of Customer record to get the surrogate key of the customer record. This only occurs once a Customer record gets updated
-- MAGIC
-- MAGIC ### Staging Customer table unions the historical to the incremental
-- MAGIC ~~1) Window results by customerid and order by the update timestamp~~  
-- MAGIC ~~2) Then coalesce the current row to the last row and ignore nulls~~  
-- MAGIC 1) Leverage SCD Type 2 Native Capabilities in DLT to APPLY CHANGES INTO the table, keeping history and satisfying the effective/end dates natively using a generated column

-- COMMAND ----------

-- CREATE OR REFRESH STREAMING LIVE TABLE DimCustomerStg (${DimCustomerStg.schema}) PARTITIONED BY (iscurrent);
-- APPLY CHANGES INTO LIVE.DimCustomerStg
-- FROM (
--   WITH CustomerHistory as (
--     SELECT
--       customerid,
--       taxid,
--       status,
--       lastname,
--       firstname,
--       middleinitial,
--       gender,
--       tier,
--       dob,
--       addressline1,
--       addressline2,
--       postalcode,
--       city,
--       stateprov,
--       country,
--       phone1,
--       phone2,
--       phone3,
--       email1,
--       email2,
--       lcl_tx_id,
--       nat_tx_id,
--       1 batchid,
--       update_ts
--     FROM
--       STREAM(${cust_mgmt_schema}.CustomerMgmt) c
--     WHERE
--       ActionType in ('NEW', 'INACT', 'UPDCUST')
--   ),
--   CustomerIncrementalRaw AS (
--     SELECT
--       * except(cdc_flag, cdc_dsn),
--       cast(substring(_metadata.file_path FROM (position('/Batch', _metadata.file_path) + 6) FOR 1) as int) batchid
--     FROM STREAM(read_files(
--       "${files_directory}sf=${scale_factor}/Batch[23]",
--       format => "csv",
--       inferSchema => False,
--       header => False,
--       sep => "|",
--       fileNamePattern => "Customer.txt",
--       schema => "${CustomerIncremental.schema}"
--     ))
--   ),
--   CustomerIncremental as (
--     SELECT
--       c.customerid,
--       nullif(c.taxid, '') taxid,
--       nullif(s.st_name, '') as status,
--       nullif(c.lastname, '') lastname,
--       nullif(c.firstname, '') firstname,
--       nullif(c.middleinitial, '') middleinitial,
--       gender,
--       c.tier,
--       c.dob,
--       nullif(c.addressline1, '') addressline1,
--       nullif(c.addressline2, '') addressline2,
--       nullif(c.postalcode, '') postalcode,
--       nullif(c.city, '') city,
--       nullif(c.stateprov, '') stateprov,
--       nullif(c.country, '') country,
--       CASE
--         WHEN isnull(c_local_1) then c_local_1
--         ELSE concat(
--           nvl2(c_ctry_1, '+' || c_ctry_1 || ' ', ''),
--           nvl2(c_area_1, '(' || c_area_1 || ') ', ''),
--           c_local_1,
--           nvl(c_ext_1, '')
--         )
--       END as phone1,
--       CASE
--         WHEN isnull(c_local_2) then c_local_2
--         ELSE concat(
--           nvl2(c_ctry_2, '+' || c_ctry_2 || ' ', ''),
--           nvl2(c_area_2, '(' || c_area_2 || ') ', ''),
--           c_local_2,
--           nvl(c_ext_2, '')
--         )
--       END as phone2,
--       CASE
--         WHEN isnull(c_local_3) then c_local_3
--         ELSE concat(
--           nvl2(c_ctry_3, '+' || c_ctry_3 || ' ', ''),
--           nvl2(c_area_3, '(' || c_area_3 || ') ', ''),
--           c_local_3,
--           nvl(c_ext_3, '')
--         )
--       END as phone3,
--       nullif(c.email1, '') email1,
--       nullif(c.email2, '') email2,
--       c.LCL_TX_ID,
--       c.NAT_TX_ID,
--       c.batchid,
--       timestamp(bd.batchdate) update_ts
--     FROM
--       CustomerIncrementalRaw c
--       JOIN LIVE.BatchDate bd ON c.batchid = bd.batchid
--       JOIN LIVE.StatusType s ON c.status = s.st_id
--   )
--   SELECT * FROM CustomerHistory
--   UNION ALL
--   SELECT * FROM CustomerIncremental
-- )
-- KEYS (customerid)
-- IGNORE NULL UPDATES
-- SEQUENCE BY update_ts
-- COLUMNS * EXCEPT (update_ts)
-- STORED AS SCD TYPE 2;

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE DimCustomer (${DimCustomer.schema}) AS SELECT 
  sk_customerid,
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
FROM LIVE.DimCustomerStg c
LEFT JOIN LIVE.TaxRate r_lcl 
  ON c.LCL_TX_ID = r_lcl.TX_ID
LEFT JOIN LIVE.TaxRate r_nat 
  ON c.NAT_TX_ID = r_nat.TX_ID
LEFT JOIN LIVE.Prospect p 
  on upper(p.lastname) = upper(c.lastname)
  and upper(p.firstname) = upper(c.firstname)
  and upper(p.addressline1) = upper(c.addressline1)
  and upper(nvl(p.addressline2, '')) = upper(nvl(c.addressline2, ''))
  and upper(p.postalcode) = upper(c.postalcode)
WHERE effectivedate < enddate;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Prospect

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Prospect gets a full load in each batch - need to handle this as SCD Type 1.  Need to keep latest record if any change occurred. Otherwise, if no change occurs, only update the recordbatchid. To do this:**
-- MAGIC 1) Group by all the columns that would trigger a change
-- MAGIC 2) Then take min/max batchid for the batchid/recordbatchid respectively
-- MAGIC 3) Then do a window to take the latest record per agencyid (QUALIFY WINDOW where ROW=1)
-- MAGIC 4) From here its just business logic for marketingnameplate and other joins
-- MAGIC
-- MAGIC This is made slightly more complicated since we need to join to DimCustomer to find out if the prospect is also a customer - which necessitates the DimCustomer staging table

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE Prospect (${Prospect.schema}) AS 
with prospect_raw as (
  SELECT
    *,
    cast(substring(_metadata.file_path FROM (position('/Batch', _metadata.file_path) + 6) FOR 1) as int) batchid
  FROM
    read_files(
      "${files_directory}sf=${scale_factor}/Batch*",
      format => "csv",
      inferSchema => False,
      header => False,
      sep => ",",
      fileNamePattern => "Prospect.csv",
      schema => "${ProspectRaw.schema}"
    )
)
SELECT 
  agencyid,
  bigint(date_format(recdate.batchdate, 'yyyyMMdd')) sk_recorddateid,
  bigint(date_format(origdate.batchdate, 'yyyyMMdd')) sk_updatedateid,
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
FROM (
  SELECT 
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
    FROM prospect_raw p
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
  QUALIFY ROW_NUMBER() OVER (PARTITION BY agencyid ORDER BY batchid DESC) = 1) p
JOIN LIVE.BatchDate recdate
  ON p.recordbatchid = recdate.batchid
JOIN LIVE.BatchDate origdate
  ON p.batchid = origdate.batchid
LEFT JOIN (
  SELECT 
    customerid,
    lastname,
    firstname,
    addressline1,
    addressline2,
    postalcode
  FROM LIVE.DimCustomerStg
  WHERE iscurrent) c
  ON 
    upper(p.LastName) = upper(c.lastname)
    and upper(p.FirstName) = upper(c.firstname)
    and upper(p.AddressLine1) = upper(c.addressline1)
    and upper(nvl(p.addressline2, '')) = upper(nvl(c.addressline2, ''))
    and upper(p.PostalCode) = upper(c.postalcode);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # DimAccount

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **This table was made more complicated because it needs to carry over update DimCustomer surrogate keys**
-- MAGIC * A customer has a 1->Many relationship with Accounts.
-- MAGIC * Therefore, a change to customer will lead to a different current SK of that customer
-- MAGIC * This leads to an update to that existing customer's account records to point to latest Customer SK
-- MAGIC * But the current start/end date of customer records, SK of customer record, and the start/end date of Account records are not known until the staging tables are created for each

-- COMMAND ----------

-- CREATE OR REFRESH STREAMING LIVE TABLE DimAccountStg (${DimAccountStg.schema});
-- APPLY CHANGES INTO LIVE.DimAccountStg
-- FROM (
--   WITH AccountIncremental AS (
--     SELECT
--       * except(cdc_flag, cdc_dsn),
--       cast(substring(_metadata.file_path FROM (position('/Batch', _metadata.file_path) + 6) FOR 1) as int) batchid
--     FROM STREAM(read_files(
--       "${files_directory}sf=${scale_factor}/Batch[23]",
--       format => "csv",
--       inferSchema => False,
--       header => False,
--       sep => "|",
--       fileNamePattern => "Account.txt",
--       schema => "${AccountIncremental.schema}"
--     ))
--   )
--   SELECT
--     accountid,
--     customerid,
--     accountdesc,
--     taxstatus,
--     brokerid,
--     status,
--     update_ts,
--     1 batchid
--   FROM
--     STREAM(${cust_mgmt_schema}.CustomerMgmt) c
--   WHERE
--     ActionType NOT IN ('UPDCUST', 'INACT')
--   UNION ALL
--   SELECT
--     accountid,
--     a.ca_c_id customerid,
--     accountDesc,
--     TaxStatus,
--     a.ca_b_id brokerid,
--     st_name as status,
--     TIMESTAMP(bd.batchdate) update_ts,
--     a.batchid
--   FROM
--     AccountIncremental a
--     JOIN LIVE.BatchDate bd ON a.batchid = bd.batchid
--     JOIN LIVE.StatusType st ON a.CA_ST_ID = st.st_id
-- )
-- KEYS (accountid)
-- IGNORE NULL UPDATES
-- SEQUENCE BY update_ts
-- COLUMNS * EXCEPT (update_ts)
-- STORED AS SCD TYPE 2;

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE DimAccount (${DimAccount.schema}) AS SELECT
  bigint(concat(date_format(a.effectivedate, 'yyyyMMdd'), a.accountid)) sk_accountid,
  a.accountid,
  b.sk_brokerid,
  a.sk_customerid,
  a.accountdesc,
  a.TaxStatus,
  a.status,
  a.batchid,
  a.effectivedate,
  a.enddate
FROM (
  SELECT
    a.* except(effectivedate, enddate, customerid),
    c.sk_customerid,
    if(a.effectivedate < c.effectivedate, c.effectivedate, a.effectivedate) effectivedate,
    if(a.enddate > c.enddate, c.enddate, a.enddate) enddate
  FROM (
    SELECT * 
    FROM LIVE.DimAccountStg a
    WHERE effectivedate < enddate
  ) a
  FULL OUTER JOIN (
    SELECT * 
    FROM LIVE.DimCustomerStg 
    WHERE effectivedate < enddate
  ) c 
    ON 
      a.customerid = c.customerid
      AND c.enddate > a.effectivedate
      AND c.effectivedate < a.enddate
) a
JOIN LIVE.DimBroker b 
  ON a.brokerid = b.brokerid;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # FactMarketHistory

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE FactMarketHistory (${FactMarketHistory.schema}) AS
WITH dailymarkethistorical AS (
  SELECT
    *,
    1 batchid
  FROM read_files(
    "${files_directory}sf=${scale_factor}/Batch1",
    format => "csv",
    inferSchema => False,
    header => False,
    sep => "|",
    fileNamePattern => "DailyMarket.txt",
    schema => "${DailyMarketHistorical.schema}"
  )
),
DailyMarketIncremental AS (
  SELECT
    * except(cdc_flag, cdc_dsn),
    cast(substring(_metadata.file_path FROM (position('/Batch', _metadata.file_path) + 6) FOR 1) as int) batchid
  FROM read_files(
    "${files_directory}sf=${scale_factor}/Batch[23]",
    format => "csv",
    inferSchema => False,
    header => False,
    sep => "|",
    fileNamePattern => "DailyMarket.txt",
    schema => "${DailyMarketIncremental.schema}"
  )
),
DailyMarket as (
  SELECT
    dm.*,
    min_by(struct(dm_low, dm_date), dm_low) OVER (
      PARTITION BY dm_s_symb
      ORDER BY dm_date ASC ROWS BETWEEN 364 PRECEDING AND CURRENT ROW
    ) fiftytwoweeklow,
    max_by(struct(dm_high, dm_date), dm_high) OVER (
      PARTITION by dm_s_symb
      ORDER BY dm_date ASC ROWS BETWEEN 364 PRECEDING AND CURRENT ROW
    ) fiftytwoweekhigh
  FROM
    (
      SELECT * FROM dailymarkethistorical
      UNION ALL
      SELECT * FROM DailyMarketIncremental
    ) dm
),
CompanyFinancialsStg as (
  SELECT
    sk_companyid,
    fi_qtr_start_date,
    sum(fi_basic_eps) OVER (PARTITION BY companyid ORDER BY fi_qtr_start_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) - fi_basic_eps sum_fi_basic_eps
  FROM LIVE.Financial
  JOIN LIVE.DimCompany
    USING (sk_companyid)
)
SELECT 
  s.sk_securityid,
  s.sk_companyid,
  bigint(date_format(dm_date, 'yyyyMMdd')) sk_dateid,
  fmh.dm_close / sum_fi_basic_eps AS peratio,
  (s.dividend / fmh.dm_close) / 100 yield,
  fiftytwoweekhigh.dm_high fiftytwoweekhigh,
  bigint(date_format(fiftytwoweekhigh.dm_date, 'yyyyMMdd')) sk_fiftytwoweekhighdate,
  fiftytwoweeklow.dm_low fiftytwoweeklow,
  bigint(date_format(fiftytwoweeklow.dm_date, 'yyyyMMdd')) sk_fiftytwoweeklowdate,
  dm_close closeprice,
  dm_high dayhigh,
  dm_low daylow,
  dm_vol volume,
  fmh.batchid
FROM DailyMarket fmh
JOIN LIVE.DimSecurity s 
  ON 
    s.symbol = fmh.dm_s_symb
    AND fmh.dm_date >= s.effectivedate 
    AND fmh.dm_date < s.enddate
LEFT JOIN CompanyFinancialsStg f 
  ON 
    f.sk_companyid = s.sk_companyid
    AND quarter(fmh.dm_date) = quarter(fi_qtr_start_date)
    AND year(fmh.dm_date) = year(fi_qtr_start_date);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # DimTrade

-- COMMAND ----------

-- CREATE OR REFRESH STREAMING LIVE TABLE TradeStg;
-- APPLY CHANGES INTO LIVE.TradeStg
-- FROM (
--   WITH trade as (
--     SELECT
--       *,
--       1 batchid
--     FROM 
--       STREAM(read_files(
--         "${files_directory}sf=${scale_factor}/Batch1",
--         format => "csv",
--         inferSchema => False,
--         header => False,
--         sep => "|",
--         fileNamePattern => "Trade.txt",
--         schema => "${TradeHistory.schema}"
--     ))
--   ),
--   TradeHistoryRaw AS (
--     SELECT
--       *
--     FROM 
--       read_files(
--         "${files_directory}sf=${scale_factor}/Batch1",
--         format => "csv",
--         inferSchema => False,
--         header => False,
--         sep => "|",
--         fileNamePattern => "TradeHistory.txt",
--         schema => "${TradeHistoryRaw.schema}"
--     )
--   ),
--   TradeIncremental AS (
--     SELECT
--       t_id tradeid,
--       t_dts,
--       t_st_id,
--       t_tt_id,
--       t_is_cash,
--       t_s_symb,
--       t_qty AS quantity,
--       t_bid_price AS bidprice,
--       t_ca_id,
--       t_exec_name AS executedby,
--       t_trade_price AS tradeprice,
--       t_chrg AS fee,
--       t_comm AS commission,
--       t_tax AS tax,
--       cast(substring(_metadata.file_path FROM (position('/Batch', _metadata.file_path) + 6) FOR 1) as int) batchid,
--       CASE 
--         WHEN cdc_flag = 'I' THEN TRUE 
--         WHEN t_st_id IN ("CMPT", "CNCL") THEN FALSE 
--         ELSE cast(null as boolean) END AS create_flg
--     FROM STREAM(read_files(
--       "${files_directory}sf=${scale_factor}/Batch[23]",
--       format => "csv",
--       inferSchema => False,
--       header => False,
--       sep => "|",
--       fileNamePattern => "Trade.txt",
--       schema => "${TradeIncremental.schema}"
--     ))
--   ),
--   TradeHistory as (
--     SELECT
--       t_id tradeid,
--       th_dts t_dts,
--       t_st_id,
--       t_tt_id,
--       t_is_cash,
--       t_s_symb,
--       t_qty AS quantity,
--       t_bid_price AS bidprice,
--       t_ca_id,
--       t_exec_name AS executedby,
--       t_trade_price AS tradeprice,
--       t_chrg AS fee,
--       t_comm AS commission,
--       t_tax AS tax,
--       1 batchid,
--       CASE 
--         WHEN (th_st_id == "SBMT" AND t_tt_id IN ("TMB", "TMS")) OR th_st_id = "PNDG" THEN TRUE 
--         WHEN th_st_id IN ("CMPT", "CNCL") THEN FALSE 
--         ELSE cast(null as boolean) END AS create_flg
--     FROM trade t
--     JOIN TradeHistoryRaw th
--       ON th_t_id = t_id
--   )
--   SELECT
--     * except(create_flg, t_is_cash),
--     if(t_is_cash = 1, TRUE, FALSE) cashflag,
--     if(create_flg, t_dts, cast(NULL AS TIMESTAMP)) create_ts,
--     if(!create_flg, t_dts, cast(NULL AS TIMESTAMP)) close_ts
--   FROM (
--     SELECT * FROM TradeHistory
--     UNION ALL
--     SELECT * FROM TradeIncremental
--   )
-- )
-- KEYS (tradeid)
-- IGNORE NULL UPDATES
-- SEQUENCE BY t_dts
-- COLUMNS * EXCEPT (t_dts)
-- STORED AS SCD TYPE 1;

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE DimTrade (${DimTrade.schema}) AS SELECT
  trade.tradeid,
  sk_brokerid,
  bigint(date_format(create_ts, 'yyyyMMdd')) sk_createdateid,
  bigint(date_format(create_ts, 'HHmmss')) sk_createtimeid,
  bigint(date_format(close_ts, 'yyyyMMdd')) sk_closedateid,
  bigint(date_format(close_ts, 'HHmmss')) sk_closetimeid,
  st_name status,
  tt_name type,
  cashflag,
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
FROM LIVE.TradeStg trade
JOIN LIVE.StatusType status
  ON status.st_id = trade.status
JOIN LIVE.TradeType tt
  ON tt.tt_id == trade.t_tt_id
JOIN LIVE.DimSecurity ds
  ON 
    ds.symbol = trade.t_s_symb
    AND to_date(trade.create_ts) >= ds.effectivedate 
    AND to_date(trade.create_ts) < ds.enddate
JOIN LIVE.DimAccount da
  ON 
    trade.t_ca_id = da.accountid 
    AND to_date(trade.create_ts) >= da.effectivedate 
    AND to_date(trade.create_ts) < da.enddate

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # FactHoldings

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE FactHoldings (${FactHoldings.schema}) AS 
WITH Holdings as (
  SELECT 
    *,
    1 batchid
  FROM read_files(
    "${files_directory}sf=${scale_factor}/Batch1",
    format => "csv",
    inferSchema => False,
    header => False,
    sep => "|",
    fileNamePattern => "HoldingHistory.txt",
    schema => "${HoldingHistory.schema}"
  )
  UNION ALL
  SELECT
    * except(cdc_flag, cdc_dsn),
    cast(substring(_metadata.file_path FROM (position('/Batch', _metadata.file_path) + 6) FOR 1) as int) batchid 
  FROM read_files(
    "${files_directory}sf=${scale_factor}/Batch[23]",
    format => "csv",
    inferSchema => False,
    header => False,
    sep => "|",
    fileNamePattern => "HoldingHistory.txt",
    schema => "${HoldingIncremental.schema}"
  )
)
SELECT
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
  h.batchid
FROM Holdings h
  JOIN LIVE.DimTrade dt 
    ON tradeid = hh_t_id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # FactCashBalances

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE FactCashBalances (${FactCashBalances.schema}) AS 
with CashTransactions as (
  SELECT
    *,
    1 batchid
  FROM 
    read_files(
      "${files_directory}sf=${scale_factor}/Batch1",
      format => "csv",
      inferSchema => False,
      header => False,
      sep => "|",
      fileNamePattern => "CashTransaction.txt",
      schema => "${CashTransactionHistory.schema}"
    )
  UNION ALL
  SELECT
    * except(cdc_flag, cdc_dsn),
    cast(substring(_metadata.file_path FROM (position('/Batch', _metadata.file_path) + 6) FOR 1) as int) batchid
  FROM
    read_files(
      "${files_directory}sf=${scale_factor}/Batch[23]",
      format => "csv",
      inferSchema => False,
      header => False,
      sep => "|",
      fileNamePattern => "CashTransaction.txt",
      schema => "${CashTransactionIncremental.schema}"
    )
),
CashTransactionsAgg as (
  SELECT 
    ct_ca_id accountid,
    to_date(ct_dts) datevalue,
    sum(ct_amt) account_daily_total,
    batchid
  FROM CashTransactions
  GROUP BY
    accountid,
    datevalue,
    batchid
)
SELECT
  a.sk_customerid, 
  a.sk_accountid, 
  bigint(date_format(datevalue, 'yyyyMMdd')) sk_dateid,
  sum(account_daily_total) OVER (partition by c.accountid order by c.datevalue) cash,
  c.batchid
FROM CashTransactionsAgg c 
JOIN LIVE.DimAccount a 
  ON 
    c.accountid = a.accountid
    AND c.datevalue >= a.effectivedate 
    AND c.datevalue < a.enddate;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # FactWatches

-- COMMAND ----------

-- CREATE OR REFRESH STREAMING LIVE TABLE FactWatchesStg;
-- APPLY CHANGES INTO LIVE.FactWatchesStg
-- FROM ( 
--   WITH all_watches as (
--     SELECT
--       *,
--       1 batchid
--     FROM 
--       STREAM(read_files(
--         "${files_directory}sf=${scale_factor}/Batch1",
--         format => "csv",
--         inferSchema => False,
--         header => False,
--         sep => "|",
--         fileNamePattern => "WatchHistory.txt",
--         schema => "${WatchHistory.schema}"
--       ))
--     UNION ALL
--     SELECT
--       * except(cdc_flag, cdc_dsn),
--       cast(substring(_metadata.file_path FROM (position('/Batch', _metadata.file_path) + 6) FOR 1) as int) batchid
--     FROM
--       STREAM(read_files(
--         "${files_directory}sf=${scale_factor}/Batch[23]",
--         format => "csv",
--         inferSchema => False,
--         header => False,
--         sep => "|",
--         fileNamePattern => "WatchHistory.txt",
--         schema => "${WatchIncremental.schema}"
--       ))
--   )
--   SELECT 
--     wh.w_c_id customerid,
--     wh.w_s_symb symbol,
--     if(w_action = 'ACTV', to_date(w_dts), null) dateplaced,
--     if(w_action = 'CNCL', to_date(w_dts), null) dateremoved,
--     w_dts,
--     batchid 
--   FROM all_watches wh
-- )
-- KEYS (customerid, symbol)
-- IGNORE NULL UPDATES
-- SEQUENCE BY w_dts
-- COLUMNS * except(w_dts)
-- STORED AS SCD TYPE 1;

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE FactWatches (${FactWatches.schema}) AS SELECT
  c.sk_customerid sk_customerid,
  s.sk_securityid sk_securityid,
  bigint(date_format(dateplaced, 'yyyyMMdd')) sk_dateid_dateplaced,
  bigint(date_format(dateremoved, 'yyyyMMdd')) sk_dateid_dateremoved,
  wh.batchid
FROM LIVE.FactWatchesStg wh
JOIN LIVE.DimSecurity s 
  ON 
    s.symbol = wh.symbol
    AND wh.dateplaced >= s.effectivedate 
    AND wh.dateplaced < s.enddate
JOIN LIVE.DimCustomer c 
  ON
    wh.customerid = c.customerid
    AND wh.dateplaced >= c.effectivedate 
    AND wh.dateplaced < c.enddate;
