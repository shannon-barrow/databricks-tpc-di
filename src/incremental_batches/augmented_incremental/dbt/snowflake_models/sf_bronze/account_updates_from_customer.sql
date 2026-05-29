{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'append',
    on_schema_change = 'ignore',
  )
}}

{# Snowflake variant — per-batch "cust_update" rows derived from
   bronzecustomer SCD2 events that touch an account. Pure SQL (no
   stage read); same dependency shape as the Databricks model.

   Translations from Databricks:
     - cast(-1 as bigint)                          -> -1::bigint
     - substring(cast(sk_customerid as string), 9) -> substr(sk_customerid::varchar, 9)
   Comparison of c.customerid (BIGINT) against the substring result is
   forced to varchar on both sides to avoid Snowflake's stricter type
   coercion vs Spark. #}

select
  'cust_update'  as cdc_flag,
  -1::bigint     as cdc_dsn,
  a.accountid,
  a.sk_brokerid  as brokerid,
  c.customerid,
  a.accountdesc,
  a.taxstatus,
  a.status,
  c.update_dt
from {{ ref('bronzecustomer') }} c
join {{ source('run_schema', 'dimaccount') }} a
  on c.customerid::varchar = substr(a.sk_customerid::varchar, 9)
 and a.iscurrent
 and c.update_dt > a.effectivedate
where c.cdc_flag = 'U'
  and c.update_dt = cast('{{ var("batch_date") }}' as date)
