{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by = 'sk_dateid',
    use_replace_on_for_insert_overwrite = True,
    on_schema_change = 'ignore',
    file_format = 'delta',
    full_refresh = false,
  )
}}

{# Stock dbt-databricks insert_overwrite. The
   use_replace_on_for_insert_overwrite=True flag (1.11+) makes dbt emit
   Delta's REPLACE-on-partition primitive that works on SQL Warehouses
   without the spark.session config tweak. Effect: today's sk_dateid
   partition is replaced; prior days untouched. Mirrors Classic's
   INSERT OVERWRITE dynamic-partition behaviour. #}

{# For each account touched this batch, write its (sk_customerid,
   sk_accountid, sk_dateid, cash) row at the latest sk_dateid. Old
   partitions for prior dates stay intact. #}

select
  a.sk_customerid,
  a.sk_accountid,
  cast(date_format(c.ct_date, 'yyyyMMdd') as bigint) as sk_dateid,
  c.current_account_cash as cash
from {{ ref('currentaccountbalances') }} c
join {{ ref('dimaccount') }} a
  on c.accountid = a.accountid
 and a.iscurrent
where c.latest_batch
