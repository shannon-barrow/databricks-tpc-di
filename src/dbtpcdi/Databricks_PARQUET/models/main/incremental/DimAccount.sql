{{
    config(
        materialized = 'table'
    )
}}
WITH accountincremental as (
    SELECT
        * except(cdc_flag, cdc_dsn),
        int(substring(_metadata.file_path FROM (position('/Batch', _metadata.file_path) + 6) FOR 1)) batchid
    FROM
        parquet.`{{var("rawfilelocation")}}/sf={{var("scalefactor")}}/Batch{2,3}/Account.parquet`
),
account AS (
  SELECT
    accountid,
    customerid,
    accountdesc,
    taxstatus,
    brokerid,
    status,
    update_ts,
    1 batchid
  FROM {{var('catalog')}}.{{var('stagingschema')}}.customermgmt  c
  WHERE
    ActionType NOT IN ('UPDCUST', 'INACT')
  UNION ALL
    SELECT
        accountid,
        customerid,
        accountDesc,
        taxstatus,
        brokerid,
        decode(a.status,
        'ACTV', 'Active',
        'CMPT', 'Completed',
        'CNCL', 'Canceled',
        'PNDG', 'Pending',
        'SBMT', 'Submitted',
        'INAC', 'Inactive') status,
        TIMESTAMP(bd.batchdate) update_ts,
        a.batchid
    FROM accountincremental a
    JOIN {{ ref('BatchDate') }}  bd
        ON a.batchid = bd.batchid
),
account_final AS (
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
    batchid
  FROM account a
),
account_cust_updates AS (
  SELECT
    a.* except(effectivedate, enddate, customerid),
    c.sk_customerid,
    if(
      a.effectivedate < c.effectivedate,
      c.effectivedate,
      a.effectivedate
    ) effectivedate,
    if(a.enddate > c.enddate, c.enddate, a.enddate) enddate
  FROM account_final a
  FULL OUTER JOIN {{ ref('DimCustomer') }}   c 
    ON a.customerid = c.customerid
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
  (a.enddate = date('9999-12-31')) iscurrent,
  a.batchid,
  a.effectivedate,
  a.enddate
FROM account_cust_updates a
JOIN {{ ref('DimBroker') }}  b 
  ON a.brokerid = b.brokerid;