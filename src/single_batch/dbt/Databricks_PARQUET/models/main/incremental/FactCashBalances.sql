{{
    config(
        materialized = 'table'
    )
}}
WITH historical as (
    SELECT
        accountid,
        to_date(ct_dts) datevalue,
        sum(ct_amt) account_daily_total
    FROM parquet.`{{var("rawfilelocation")}}/sf={{var("scalefactor")}}/Batch1/CashTransaction*.parquet`
    GROUP BY ALL
),
alltransactions as (
  SELECT
    *,
    1 batchid FROM historical
  UNION ALL
  SELECT
    accountid,
    to_date(ct_dts) datevalue,
    sum(ct_amt) account_daily_total,
    int(substring(_metadata.file_path FROM (position('/Batch', _metadata.file_path) + 6) FOR 1)) batchid
  FROM parquet.`{{var("rawfilelocation")}}/sf={{var("scalefactor")}}/Batch{2,3}/CashTransaction.parquet`
  GROUP BY ALL
)
SELECT a.sk_customerid,
       a.sk_accountid,
       bigint(date_format(datevalue, 'yyyyMMdd')) sk_dateid,
       sum(account_daily_total)  OVER (partition by c.accountid order by datevalue) cash, c.batchid
FROM alltransactions c
JOIN {{ref ('DimAccount') }} a
ON
    c.accountid = a.accountid
    AND c.datevalue >= a.effectivedate
    AND c.datevalue < a.enddate;