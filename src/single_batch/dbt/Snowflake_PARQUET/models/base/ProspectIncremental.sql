{{
    config(
        materialized = 'table'
    )
}}
with ProspectRaw as (
  select
    $1:agencyid::STRING           AS agencyid,
    $1:lastname::STRING           AS lastname,
    $1:firstname::STRING          AS firstname,
    $1:middleinitial::STRING      AS middleinitial,
    $1:gender::STRING             AS gender,
    $1:addressline1::STRING       AS addressline1,
    $1:addressline2::STRING       AS addressline2,
    $1:postalcode::STRING         AS postalcode,
    $1:city::STRING               AS city,
    $1:state::STRING              AS state,
    $1:country::STRING            AS country,
    $1:phone::STRING              AS phone,
    $1:income::STRING             AS income,
    $1:numbercars::INT            AS numbercars,
    $1:numberchildren::INT        AS numberchildren,
    $1:maritalstatus::STRING      AS maritalstatus,
    $1:age::INT                   AS age,
    $1:creditrating::INT          AS creditrating,
    $1:ownorrentflag::STRING      AS ownorrentflag,
    $1:employer::STRING           AS employer,
    $1:numbercreditcards::INT     AS numbercreditcards,
    $1:networth::INT              AS networth,
    try_cast(SUBSTR(METADATA$FILENAME, position('/Batch' in METADATA$FILENAME) + 6, 1) as int) batchid
  from
    @{{ var('stage') }}
    (
      FILE_FORMAT => 'parquet_format',
      PATTERN     => '.*Prospect_.*[.]parquet'
    )
),
p AS (
    SELECT
        *,
        IFF(
            CONCAT(
                IFF(networth::FLOAT > 1000000 OR income::FLOAT > 200000, 'HighValue+', ''),
                IFF(numberchildren::INT > 3 OR numbercreditcards::INT > 5, 'Expenses+', ''),
                IFF(age::INT > 45, 'Boomer+', ''),
                IFF(income::FLOAT < 50000 OR creditrating::INT < 600 OR networth::FLOAT < 100000, 'MoneyAlert+', ''),
                IFF(numbercars::INT > 3 OR numbercreditcards::INT > 7, 'Spender+', ''),
                IFF(age::INT < 25 AND networth::FLOAT > 1000000, 'Inherited+', '')
            ) <> '',
            LEFT(
                CONCAT(
                    IFF(networth::FLOAT > 1000000 OR income::FLOAT > 200000, 'HighValue+', ''),
                    IFF(numberchildren::INT > 3 OR numbercreditcards::INT > 5, 'Expenses+', ''),
                    IFF(age::INT > 45, 'Boomer+', ''),
                    IFF(income::FLOAT < 50000 OR creditrating::INT < 600 OR networth::FLOAT < 100000, 'MoneyAlert+', ''),
                    IFF(numbercars::INT > 3 OR numbercreditcards::INT > 7, 'Spender+', ''),
                    IFF(age::INT < 25 AND networth::FLOAT > 1000000, 'Inherited+', '')
                ),
                GREATEST(
                    LENGTH(
                        CONCAT_WS('',
                            IFF(networth::FLOAT > 1000000 OR income::FLOAT > 200000, 'HighValue+', NULL),
                            IFF(numberchildren::INT > 3 OR numbercreditcards::INT > 5, 'Expenses+', NULL),
                            IFF(age::INT > 45, 'Boomer+', NULL),
                            IFF(income::FLOAT < 50000 OR creditrating::INT < 600 OR networth::FLOAT < 100000, 'MoneyAlert+', NULL),
                            IFF(numbercars::INT > 3 OR numbercreditcards::INT > 7, 'Spender+', NULL),
                            IFF(age::INT < 25 AND networth::FLOAT > 1000000, 'Inherited+', NULL)
                        )
                    ) - 1,
                    0
                )
            ),
            NULL
        ) AS marketingnameplate
    FROM ProspectRaw
)
SELECT * FROM (
  SELECT
    * exclude(batchid),
    max(batchid) recordbatchid,
    min(batchid) batchid
  FROM p
  GROUP BY ALL
)
WHERE recordbatchid = 3