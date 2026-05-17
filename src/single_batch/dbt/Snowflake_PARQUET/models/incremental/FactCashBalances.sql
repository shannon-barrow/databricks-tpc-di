{{
    config(
        materialized = 'table'
    )
}}
with cashtransactionhistorical as (
    select
        $1:accountid::BIGINT     AS accountid,
        CONVERT_TIMEZONE('UTC', $1:ct_dts::TIMESTAMP)     AS ct_dts,
        $1:ct_amt::DOUBLE        AS ct_amt,
        $1:ct_name::STRING       AS ct_name,
        1          as batchid
    from
        @{{ var('stage') }}/Batch1
        (
            FILE_FORMAT => 'parquet_format',
            PATTERN     => '.*CashTransaction_.*[.]parquet'
        )
),
cashtransactionincremental as (
    select
        $1:accountid::BIGINT     AS accountid,
        CONVERT_TIMEZONE('UTC', $1:ct_dts::TIMESTAMP)     AS ct_dts,
        $1:ct_amt::DOUBLE        AS ct_amt,
        $1:ct_name::STRING       AS ct_name,
        try_cast(SUBSTR(METADATA$FILENAME, position('/Batch' in METADATA$FILENAME) + 6, 1) as int) batchid
    from
        @{{ var('stage') }}
        (
            FILE_FORMAT => 'parquet_format',
            PATTERN     => '.*Batch[23]/CashTransaction[.]parquet'
        )
),
dailytotals as (
    SELECT
        accountid,
        to_date(ct_dts) datevalue,
        sum(ct_amt) account_daily_total,
        batchid
    FROM cashtransactionhistorical
    GROUP BY
        accountid,
        datevalue,
        batchid
    UNION ALL
    SELECT
        accountid,
        to_date(ct_dts) datevalue,
        sum(ct_amt) account_daily_total,
        batchid
    FROM cashtransactionincremental
    GROUP BY
        accountid,
        datevalue,
        batchid
)
SELECT
  a.sk_customerid, 
  a.sk_accountid, 
  cast(TO_CHAR(datevalue, 'YYYYMMDD') as bigint) sk_dateid,
  sum(account_daily_total) OVER (partition by c.accountid order by c.datevalue) cash,
  c.batchid
FROM dailytotals c
JOIN {{ ref( 'DimAccount') }} a
  ON 
    c.accountid = a.accountid
    AND c.datevalue >= a.effectivedate 
    AND c.datevalue < a.enddate 
