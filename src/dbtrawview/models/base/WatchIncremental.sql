{{
    config(
        materialized = 'view'
    )
}}
select
    *,
    2 as batchid
from
    {{ source('tpcdi', 'WatchIncrementaldos') }}

 UNION ALL

 select
    *,
    3 as batchid
from
    {{ source('tpcdi', 'WatchIncrementaltres') }}


