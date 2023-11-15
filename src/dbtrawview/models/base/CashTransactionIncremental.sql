{{
    config(
        materialized = 'view'
    )
}}
select
    *,
    2 as batchid
from
    {{ source('tpcdi', 'CashTransactionIncrementaldos') }}

 UNION ALL

 select
    *,
    3 as batchid
from
    {{ source('tpcdi', 'CashTransactionIncrementaltres') }}


