{{
  config(
    materialized = "table"

  )
}}

select
    * EXCLUDE(Value),
    1 as batchid
from
    {{source('tpcdi', 'BatchDate_batch_1') }}

 UNION ALL

select
    * EXCLUDE(Value),
    2 as batchid
from
    {{source('tpcdi', 'BatchDate_batch_2') }}

 UNION ALL

 select
    * EXCLUDE(Value),
    3 as batchid
from
    {{source('tpcdi', 'BatchDate_batch_3') }}

