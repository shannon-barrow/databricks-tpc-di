{{
    config(
        materialized = 'table'
    )
}}
with ProspectRaw as (
  select
    $1::string  as agencyid,
    $2::string  as lastname,
    $3::string  as firstname,
    $4::string  as middleinitial,
    $5::string  as gender,
    $6::string  as addressline1,
    $7::string  as addressline2,
    $8::string  as postalcode,
    $9::string  as city,
    $10::string as state,
    $11::string as country,
    $12::string as phone,
    $13::string as income,
    $14::int    as numbercars,
    $15::int    as numberchildren,
    $16::string as maritalstatus,
    $17::int    as age,
    $18::int    as creditrating,
    $19::string as ownorrentflag,
    $20::string as employer,
    $21::int    as numbercreditcards,
    $22::int    as networth,
    try_cast(SUBSTR(METADATA$FILENAME, position('/Batch' in METADATA$FILENAME) + 6, 1) as int) batchid
  from
    @{{ var('stage') }}
    (
      FILE_FORMAT => 'TXT_CSV',
      PATTERN     => '.*Prospect[.]csv'
    ) t
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