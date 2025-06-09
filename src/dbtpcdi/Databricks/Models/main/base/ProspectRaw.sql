{{
    config(
        materialized = 'view'
    )
}}

select
    *,
    1 as batchid
from
    {{source('tpcdi', 'ProspectRaw_batch_1') }}

 UNION ALL

select
    *,
    2 as batchid
from
    {{source('tpcdi', 'ProspectRaw_batch_2') }}

 UNION ALL

 select
    *,
    3 as batchid
from
    {{source('tpcdi', 'ProspectRaw_batch_3') }}



