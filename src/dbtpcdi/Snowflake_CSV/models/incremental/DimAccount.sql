{{
    config(
        materialized = 'table'
    )
}}
WITH AccountIncremental AS (
  select
    $3::bigint  as accountid,
    $4::bigint  as brokerid,
    $5::bigint  as customerid,
    $6::string  as accountDesc,
    $7::tinyint as taxstatus,
    $8::string  as status,
    try_cast(SUBSTR(METADATA$FILENAME, position('/Batch' in METADATA$FILENAME) + 6, 1) as int) batchid
  from
    @{{ var('stage') }}
    (
      FILE_FORMAT => 'TXT_PIPE',
      PATTERN     => '.*Account[.]txt'
    ) t
),
Account AS (
  SELECT
    accountid,
    customerid,
    accountdesc,
    taxstatus,
    brokerid,
    status,
    update_ts,
    1 AS batchid
  FROM {{var('catalog')}}.{{var('stagingschema')}}.customermgmt_clean c
  WHERE ActionType NOT IN ('UPDCUST', 'INACT')
  UNION ALL
  SELECT
    accountid,
    customerid,
    accountdesc,
    taxstatus,
    brokerid,
    CASE
      WHEN a.status = 'ACTV' THEN 'Active'
      WHEN a.status = 'CMPT' THEN 'Completed'
      WHEN a.status = 'CNCL' THEN 'Canceled'
      WHEN a.status = 'PNDG' THEN 'Pending'
      WHEN a.status = 'SBMT' THEN 'Submitted'
      WHEN a.status = 'INAC' THEN 'Inactive'
      ELSE NULL
    END AS status,
    TO_TIMESTAMP(bd.batchdate) AS update_ts,
    a.batchid
  FROM AccountIncremental a
  JOIN {{ ref('BatchDate') }} bd
    ON a.batchid = bd.batchid
),
AccountFinal AS (
  SELECT
    accountid,
    customerid,
    COALESCE(accountdesc, LAST_VALUE(accountdesc IGNORE NULLS) OVER (PARTITION BY accountid ORDER BY update_ts)) AS accountdesc,
    COALESCE(taxstatus, LAST_VALUE(taxstatus IGNORE NULLS) OVER (PARTITION BY accountid ORDER BY update_ts )) AS taxstatus,
    COALESCE(brokerid, LAST_VALUE(brokerid IGNORE NULLS) OVER (PARTITION BY accountid ORDER BY update_ts )) AS brokerid,
    COALESCE(status, LAST_VALUE(status IGNORE NULLS) OVER (PARTITION BY accountid ORDER BY update_ts )) AS status,
    DATE(update_ts) AS effectivedate,
    COALESCE(
      LEAD(DATE(update_ts)) OVER (PARTITION BY accountid ORDER BY update_ts),
      DATE('9999-12-31')
    ) AS enddate,
    batchid
  FROM Account
),
AccountCustomerUpdates AS (
  SELECT
    a.accountid,
    a.accountdesc,
    a.taxstatus,
    a.brokerid,
    a.status,
    c.sk_customerid,
    CASE
      WHEN a.effectivedate < c.effectivedate THEN c.effectivedate
      ELSE a.effectivedate
    END AS effectivedate,
    
    CASE
      WHEN a.enddate > c.enddate THEN c.enddate
      ELSE a.enddate
    END AS enddate,
    a.batchid
  FROM AccountFinal a
  FULL OUTER JOIN {{ ref('DimCustomer') }} c
     ON a.customerid = c.customerid
     AND c.enddate > a.effectivedate
     AND c.effectivedate < a.enddate
  WHERE a.effectivedate < a.enddate 
)
SELECT
  CAST(CONCAT(TO_VARCHAR(a.effectivedate, 'YYYYMMDD'), a.accountid) AS BIGINT) AS sk_accountid,
  a.accountid,
  b.sk_brokerid,
  a.sk_customerid,
  a.accountdesc,
  a.taxstatus,
  a.status,
  (a.enddate = DATE('9999-12-31')) AS iscurrent,
  a.batchid,
  a.effectivedate,
  a.enddate
FROM AccountCustomerUpdates a
JOIN {{ ref('DimBroker') }} b 
   ON a.brokerid = b.brokerid