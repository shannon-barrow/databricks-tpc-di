{{
    config(
        materialized = 'view'
    )
}}
select
    *,
    1 as batchid
from
    {{ source('tpcdi', 'DailyMarketHistorical') }}
