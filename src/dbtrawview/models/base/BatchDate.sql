{{
    config(
        materialized = 'view'
    )
}}

select
    *,
    1 as batchid
from
    {{ source('tpcdi', 'BatchDateuno') }}

 UNION ALL

select
    *,
    2 as batchid
from
    {{ source('tpcdi', 'BatchDatedos') }}

 UNION ALL

 select
    *,
    3 as batchid
from
    {{ source('tpcdi', 'BatchDatetres') }}

