{{
  config(
    materialized='incremental',
    incremental_strategy='append',
    on_schema_change='ignore',
  )
}}

{# Redshift variant — per-batch "cust_update" rows derived from
   bronzecustomer SCD2 events that touch an account. Pure SQL, no S3 read.
   Translated from BQ's bq_bronze/account_updates_from_customer.sql:
     - CAST(... AS INT64)       -> CAST(... AS BIGINT)
     - CAST(... AS STRING)      -> CAST(... AS VARCHAR(MAX))
     - DATE '{{ var(...) }}'    -> '{{ var(...) }}'::DATE
   Otherwise identical join + filter logic. #}

select
  'cust_update'             as cdc_flag,
  CAST(-1 AS BIGINT)        as cdc_dsn,
  a.accountid,
  a.sk_brokerid             as brokerid,
  c.customerid,
  a.accountdesc,
  a.taxstatus,
  a.status,
  c.update_dt
from {{ ref('bronzecustomer') }} c
join {{ source('run_schema', 'dimaccount') }} a
  on CAST(c.customerid AS VARCHAR(MAX)) = SUBSTR(CAST(a.sk_customerid AS VARCHAR(MAX)), 9)
 and a.iscurrent
 and c.update_dt > a.effectivedate
where c.cdc_flag = 'U'
  and c.update_dt = '{{ var("batch_date") }}'::DATE
