{{
    config(
        materialized = 'view'
    )
}}
select
    *,
    2 as batchid
from
    {{source('tpcdi', 'CashTransactionIncremental_batch_2') }}

 UNION ALL

 select
    *,
    3 as batchid
from
    {{source('tpcdi', 'CashTransactionIncremental_batch_3') }}


