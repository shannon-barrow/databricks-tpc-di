{{
    config(
        materialized = 'view'
    )
}}
select
    *,
    2 as batchid
from
    {{ source('tpcdi', 'TradeIncrementaldos') }}

 UNION ALL

 select
    *,
    3 as batchid
from
    {{ source('tpcdi', 'TradeIncrementaltres') }}


