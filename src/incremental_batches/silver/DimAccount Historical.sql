-- Databricks notebook source
INSERT INTO ${catalog}.${wh_db}_${scale_factor}.DimAccount
WITH account AS (
  SELECT
    accountid,
    customerid,
    coalesce(
      accountdesc,
      last_value(accountdesc) IGNORE NULLS OVER (
        PARTITION BY accountid
        ORDER BY update_ts
      )
    ) accountdesc,
    coalesce(
      taxstatus,
      last_value(taxstatus) IGNORE NULLS OVER (
        PARTITION BY accountid
        ORDER BY update_ts
      )
    ) taxstatus,
    coalesce(
      brokerid,
      last_value(brokerid) IGNORE NULLS OVER (
        PARTITION BY accountid
        ORDER BY update_ts
      )
    ) brokerid,
    coalesce(
      status,
      last_value(status) IGNORE NULLS OVER (
        PARTITION BY accountid
        ORDER BY update_ts
      )
    ) status,
    date(update_ts) effectivedate,
    nvl(
      lead(date(update_ts)) OVER (
        PARTITION BY accountid
        ORDER BY update_ts
      ),
      date('9999-12-31')
    ) enddate,
    1 batchid
  FROM 
    ${catalog}.${wh_db}_${scale_factor}_stage.CustomerMgmt c
  WHERE
    ActionType NOT IN ('UPDCUST', 'INACT')  
),
with_cust_updates AS (
  SELECT
    a.* except(effectivedate, enddate, customerid),
    c.sk_customerid,
    if(
      a.effectivedate < c.effectivedate,
      c.effectivedate,
      a.effectivedate
    ) effectivedate,
    if(a.enddate > c.enddate, c.enddate, a.enddate) enddate
  FROM account a
  FULL OUTER JOIN ${catalog}.${wh_db}_${scale_factor}.DimCustomer c 
    ON 
      a.batchid = c.batchid
      and a.customerid = c.customerid
      AND c.enddate > a.effectivedate
      AND c.effectivedate < a.enddate
  WHERE a.effectivedate < a.enddate
)
SELECT
  bigint(concat(date_format(a.effectivedate, 'yyyyMMdd'), a.accountid)) sk_accountid,
  a.accountid,
  b.sk_brokerid,
  a.sk_customerid,
  a.accountdesc,
  a.TaxStatus,
  a.status,
  if(a.enddate = date('9999-12-31'), true, false) iscurrent,
  a.batchid,
  a.effectivedate,
  a.enddate
FROM with_cust_updates a
JOIN ${catalog}.${wh_db}_${scale_factor}.DimBroker b 
  ON a.brokerid = b.brokerid
