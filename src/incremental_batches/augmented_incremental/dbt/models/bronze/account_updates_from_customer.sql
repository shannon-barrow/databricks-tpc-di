{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'append',
    on_schema_change = 'ignore',
    file_format = 'delta',
  )
}}

{# Per-batch "cust_update" rows derived from bronzecustomer SCD2 events that
   also touch an account. Mirrors:
     * Cluster's `bronze/account_updates_from_customer.py` (which inserts
       directly into bronzeaccount)
     * SDP's `account_updates_from_customers` flow (which writes straight
       to dimaccount)

   We keep bronzeaccount pure (Account.txt drops only) and stage these
   derived rows here. silver/dimaccount UNIONs them with new_events
   from bronzeaccount before applying SCD2.

   DAG benefit: this model only depends on bronzecustomer + a source-read
   of the dimaccount AS-OF-batch-start, so it can start as soon as
   bronzecustomer finishes — in parallel with dimcustomer. dimaccount
   waits for both to complete.

   Table is pre-created by setup_dbt.py with CLUSTER BY (update_dt) +
   delta.dataSkippingNumIndexedCols=34 ("setup-owns-layout" — dbt
   model configs intentionally omit liquid_clustered_by/tblproperties
   so dbt-databricks doesn't issue ALTER TABLE noise on every batch). #}

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
