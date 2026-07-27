{{
  config(
    partition_by = {
      'field': 'update_dt',
      'data_type': 'date',
      'copy_partitions': true,
    },
  )
}}

{# BQ variant — per-batch "cust_update" rows derived from bronzecustomer
   SCD2 events that touch an account. Pure SQL, no external stage read.

   Translations from Snowflake:
     - -1::bigint                                  -> CAST(-1 AS INT64)
     - substr(sk_customerid::varchar, 9)           -> SUBSTR(CAST(sk_customerid AS STRING), 9)
     - c.customerid::varchar = ...                  -> CAST(c.customerid AS STRING) = ...
     - cast('{{ batch_date }}' as date)            -> DATE '{{ batch_date }}'
#}

select
  'cust_update'           as cdc_flag,
  CAST(-1 AS INT64)       as cdc_dsn,
  a.accountid,
  a.sk_brokerid           as brokerid,
  c.customerid,
  a.accountdesc,
  a.taxstatus,
  a.status,
  c.update_dt
from {{ ref('bronzecustomer') }} c
join {{ source('run_schema', 'dimaccount') }} a
  on CAST(c.customerid AS STRING) = SUBSTR(CAST(a.sk_customerid AS STRING), 9)
 and a.iscurrent
 and c.update_dt > a.effectivedate
where c.cdc_flag = 'U'
  and c.update_dt = DATE '{{ var("batch_date") }}'
