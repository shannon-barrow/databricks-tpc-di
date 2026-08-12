{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'append',
    on_schema_change = 'ignore',
    file_format = 'delta',
  )
}}

{# Per-batch "cust_update" rows derived from bronzecustomer SCD2 events that
   also touch an account. Staged here (rather than in bronzeaccount) to keep
   bronzeaccount pure; silver/dimaccount UNIONs them with new_events from
   bronzeaccount before applying SCD2. Depends only on bronzecustomer + an
   AS-OF read of dimaccount, so it runs in parallel with dimcustomer.

   Liquid clustered on update_dt (+ dataSkippingNumIndexedCols=34) — defined in
   setup_dbt.py (which pre-creates the table), not here, so dbt-databricks
   doesn't re-issue ALTER TABLE CLUSTER BY every batch ("setup-owns-layout"). #}

select
  'cust_update' as cdc_flag,
  cast(-1 as bigint) as cdc_dsn,
  a.accountid,
  a.sk_brokerid as brokerid,
  c.customerid,
  a.accountdesc,
  a.taxstatus,
  a.status,
  c.update_dt
from {{ ref('bronzecustomer') }} c
join {{ source('run_schema', 'dimaccount') }} a
  on c.customerid = substring(cast(a.sk_customerid as string), 9)
 and a.iscurrent
 and c.update_dt > a.effectivedate
where c.cdc_flag = 'U'
  and c.update_dt = cast('{{ var("batch_date") }}' as date)
