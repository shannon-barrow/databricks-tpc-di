-- Databricks notebook source
INSERT INTO ${catalog}.${wh_db}.FactCashBalances
with CashTransactons as (
  SELECT * FROM ${catalog}.${wh_db}_stage.v_CashTransactionHistory
  UNION ALL
  SELECT * FROM ${catalog}.${wh_db}_stage.v_CashTransactionIncremental
),
CashTransactionsAgg as (
  SELECT 
    ct_ca_id accountid,
    to_date(ct_dts) datevalue,
    sum(ct_amt) account_daily_total,
    batchid
  FROM CashTransactons
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
JOIN ${catalog}.${wh_db}.DimAccount a 
  ON 
    c.accountid = a.accountid
    AND c.datevalue >= a.effectivedate 
    AND c.datevalue < a.enddate
