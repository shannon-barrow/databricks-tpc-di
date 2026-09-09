{{
  config(
    materialized='incremental',
    incremental_strategy='append',
    on_schema_change='ignore',
  )
}}

{# Fabric DW — per-batch "cust_update" rows derived from bronzecustomer SCD2
   events that touch an account. Pure SQL, no file read.

   Redshift -> Fabric T-SQL:
     - CAST(... AS VARCHAR(MAX))  -> CAST(... AS VARCHAR(30))  (Fabric DW has no
                                     VARCHAR(MAX); 30 covers the numeric ids)
     - SUBSTRING(s, 9)            -> SUBSTRING(s, 9, 30)  (T-SQL SUBSTRING needs
                                     a length arg; sk_customerid = yyyyMMdd||id
                                     so char 9+ is the customerid)
     - '{{ var }}'::DATE          -> CAST('{{ var }}' AS DATE)
#}

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
  on CAST(c.customerid AS VARCHAR(30)) = SUBSTRING(CAST(a.sk_customerid AS VARCHAR(30)), 9, 30)
 and a.iscurrent = 1
 and c.update_dt > a.effectivedate
where c.cdc_flag = 'U'
  and c.update_dt = CAST('{{ var("batch_date") }}' AS DATE)
