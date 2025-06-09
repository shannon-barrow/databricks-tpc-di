{{
    config(
        materialized = 'table'
    )
}}

select
    *  EXCLUDE(Value)
from
    {{source("tpcdi", 'IndustryRaw') }}

