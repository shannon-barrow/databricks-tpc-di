{{
    config(
        materialized = 'view'
    )
}}
select
    *,
    2 as batchid
from
    {{ source('tpcdi', 'DailyMarketIncrementaldos') }}

 UNION ALL

 select
    *,
    3 as batchid
from
    {{ source('tpcdi', 'DailyMarketIncrementaltres') }}


