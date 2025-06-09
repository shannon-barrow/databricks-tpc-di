{{
    config(
        materialized = 'view'
    )
}}
select
    *  EXCLUDE(Value),
    2 as batchid
from
    {{source('tpcdi', 'HoldingIncremental_batch_2') }}

 UNION ALL

 select
    * EXCLUDE(Value),
    3 as batchid
from
    {{source('tpcdi', 'HoldingIncremental_batch_3') }}


