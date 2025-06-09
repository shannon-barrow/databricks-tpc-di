{{
    config(
        materialized = 'view'
    )
}}
select
    * EXCLUDE(Value) ,
    1 as batchid
from
    {{source('tpcdi', 'DailyMarket') }}
