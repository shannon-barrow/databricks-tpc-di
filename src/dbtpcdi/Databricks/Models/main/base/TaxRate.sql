{{
    config(
        materialized = 'table'
    )
}}
select
    *
from
    {{source('tpcdi', 'TaxRate') }}
