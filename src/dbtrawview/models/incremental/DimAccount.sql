{{
    config(
        materialized = 'table'
    )
}}
SELECT
  
  a.accountid,
  b.sk_brokerid,
  a.sk_customerid,
  a.accountdesc,
  a.TaxStatus,
  a.status,
  a.batchid,
  a.effectivedate,
  bigint(concat(date_format(a.effectivedate, 'yyyyMMdd'), cast(a.accountid as string))) as sk_accountid,
  a.enddate
FROM (
  SELECT
    a.* except(effectivedate, enddate, customerid),
    c.sk_customerid,
    if(a.effectivedate < c.effectivedate, c.effectivedate, a.effectivedate) effectivedate,
    if(a.enddate > c.enddate, c.enddate, a.enddate) enddate
  FROM (
    SELECT *
    FROM (
      SELECT
        accountid,
        customerid,
        coalesce(accountdesc, last_value(accountdesc) IGNORE NULLS OVER (
          PARTITION BY accountid ORDER BY update_ts)) accountdesc,
        coalesce(taxstatus, last_value(taxstatus) IGNORE NULLS OVER (
          PARTITION BY accountid ORDER BY update_ts)) taxstatus,
        coalesce(brokerid, last_value(brokerid) IGNORE NULLS OVER (
          PARTITION BY accountid ORDER BY update_ts)) brokerid,
        coalesce(status, last_value(status) IGNORE NULLS OVER (
          PARTITION BY accountid ORDER BY update_ts)) status,
        date(update_ts) effectivedate,
        nvl(lead(date(update_ts)) OVER (PARTITION BY accountid ORDER BY update_ts), date('9999-12-31')) enddate,
        batchid
      FROM (
        SELECT
          accountid,
          customerid,
          accountdesc,
          taxstatus,
          brokerid,
          status,
          update_ts,
          1 batchid
        FROM {{ ref('CustomerMgmtView') }} c
        WHERE ActionType NOT IN ('UPDCUST', 'INACT')
        UNION ALL
        SELECT
          accountid,
          a.ca_c_id customerid,
          accountDesc,
          TaxStatus,
          a.ca_b_id brokerid,
          st_name as status,
          TIMESTAMP(bd.batchdate) update_ts,
          a.batchid
        FROM {{ ref('AccountIncremental') }} a
        JOIN {{ ref('BatchDate') }} bd
          ON a.batchid = bd.batchid
        JOIN {{ source('tpcdi', 'StatusType') }} st 
          ON a.CA_ST_ID = st.st_id
      ) a
    ) a
    WHERE a.effectivedate < a.enddate
  ) a
  FULL OUTER JOIN {{ ref('DimCustomerStg') }} c 
    ON 
      a.customerid = c.customerid
      AND c.enddate > a.effectivedate
      AND c.effectivedate < a.enddate
) a
LEFT JOIN {{ ref('DimBroker') }} b 
  ON a.brokerid = b.brokerid;
