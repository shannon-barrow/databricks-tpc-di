{{
    config(
        materialized = 'view'
    )
}}
select
    *,
    2 as batchid
from
    {{ source('tpcdi', 'AccountIncrementaldos') }}

 UNION ALL

 select
    *,
    3 as batchid
from
    {{ source('tpcdi', 'AccountIncrementaltres') }}

