{{
    config(
        materialized = 'table'

    )
}}

WITH ProspectRaw AS (
    SELECT *  EXCLUDE(Value), 1 AS batchid_union FROM {{source('tpcdi', 'ProspectRaw_batch_1') }}
    UNION ALL
    SELECT * EXCLUDE(Value), 2 AS batchid_union FROM {{source('tpcdi', 'ProspectRaw_batch_2') }}
    UNION ALL
    SELECT * EXCLUDE(Value), 3 AS batchid_union FROM {{source('tpcdi', 'ProspectRaw_batch_3') }}
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
        ) AS marketingnameplate,
        batchid_union::INT AS batchid
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
