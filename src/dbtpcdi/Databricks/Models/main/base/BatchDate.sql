{{
    config(
        materialized = 'table'
    )
}}

select
    *,
    1 as batchid
from
    {{source('tpcdi', 'BatchDate_batch_1') }}

 UNION ALL

select
    *,
    2 as batchid
from
    {{source('tpcdi', 'BatchDate_batch_2') }}

 UNION ALL

 select
    *,
    3 as batchid
from
    {{source('tpcdi', 'BatchDate_batch_3') }}

