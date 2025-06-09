{{
    config(
        materialized = 'view'
    )
}}
select
    *,
    2 as batchid
from
    {{source('tpcdi', 'WatchIncremental_batch_2') }}

 UNION ALL

 select
    *,
    3 as batchid
from
    {{source('tpcdi', 'WatchIncremental_batch_3') }}


