{{
    config(
        materialized = 'view'
    )
}}

select
    *,
    1 as batchid
from
    {{ source('tpcdi', 'ProspectRawuno') }}

 UNION ALL

select
    *,
    2 as batchid
from
    {{ source('tpcdi', 'ProspectRawdos') }}

 UNION ALL

 select
    *,
    3 as batchid
from
    {{ source('tpcdi', 'ProspectRawtres') }}
