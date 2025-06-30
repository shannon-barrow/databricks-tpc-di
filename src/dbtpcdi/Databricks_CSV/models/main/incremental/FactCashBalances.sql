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
    FROM read_files(
      '{{var("rawfilelocation")}}/sf={{var("scalefactor")}}/Batch1',
      format          => "csv",
      header          => "false",
      inferSchema     => false,
      sep             => "|",
      schemaEvolutionMode => 'none',
      fileNamePattern => "CashTransaction\\.txt",
      schema => "accountid BIGINT, ct_dts TIMESTAMP, ct_amt DOUBLE, ct_name STRING"
    )
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
  FROM read_files(
      '{{var("rawfilelocation")}}/sf={{var("scalefactor")}}/Batch{2,3}',
      format          => "csv",
      header          => "false",
      inferSchema     => false,
      sep             => "|",
      schemaEvolutionMode => 'none',
      fileNamePattern => "CashTransaction\\.txt",
      schema => "cdc_flag STRING, cdc_dsn BIGINT, accountid BIGINT, ct_dts TIMESTAMP, ct_amt DOUBLE, ct_name STRING"
    )
  GROUP BY ALL
)
SELECT a.sk_customerid,
       a.sk_accountid,
       bigint(date_format(datevalue, 'yyyyMMdd')) sk_dateid,
       sum(account_daily_total)                   OVER (partition by c.accountid order by datevalue) cash, c.batchid
FROM alltransactions c
JOIN {{ref ('DimAccount') }} a
ON
    c.accountid = a.accountid
    AND c.datevalue >= a.effectivedate
    AND c.datevalue < a.enddate;