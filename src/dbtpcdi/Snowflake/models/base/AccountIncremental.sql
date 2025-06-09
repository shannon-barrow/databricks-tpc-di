{{
    config(
        materialized = 'view'
    )
}}
select
    * EXCLUDE(cdc_flag, cdc_dsn),
    2 as batchid
from
    {{source('tpcdi', 'AccountIncremental_batch_2') }}

 UNION ALL

 select
     * EXCLUDE(cdc_flag, cdc_dsn),
    3 as batchid
from
    {{source('tpcdi', 'AccountIncremental_batch_3') }}

