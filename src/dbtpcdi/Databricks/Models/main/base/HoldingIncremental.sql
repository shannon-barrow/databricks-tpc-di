{{
    config(
        materialized = 'view'
    )
}}
select
    *,
    2 as batchid
from
    {{source('tpcdi', 'HoldingIncremental_batch_2') }}

 UNION ALL

 select
    *,
    3 as batchid
from
    {{source('tpcdi', 'HoldingIncremental_batch_3') }}


