{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'append',
    partition_by = 'update_dt',
    on_schema_change = 'ignore',
    file_format = 'delta',
  )
}}

{# Two sources go into bronzeaccount, mirroring the Classic build:
    1. Direct ingestion of the day's Account.txt drop.
    2. Derived "cust_update" rows that cascade Customer SCD2 changes onto
       the matching account row, so silver/dimaccount can keep its
       customerid surrogate-key chain in sync.

    The Classic build expresses (2) as a separate streaming notebook that
    INSERTs into bronzeaccount. dbt's "one model per table" rule makes
    that awkward, so the two sources are UNIONed here. The end state is
    identical: bronzeaccount holds both kinds of rows, and silver/
    dimaccount sees both at MERGE time.

    DAG order: bronzecustomer -> bronzeaccount -> silver/dimaccount.
    Acyclic because we read dimaccount via {{ source(...) }} (its
    AS-OF-BATCH-START state), not {{ ref(...) }}. #}

{%- set schema_str -%}
cdc_flag STRING, cdc_dsn BIGINT, accountid BIGINT, brokerid BIGINT,
customerid BIGINT, accountdesc STRING, taxstatus TINYINT, status STRING,
update_dt DATE
{%- endset -%}

with from_account_file as (
  select * from {{ read_daily_csv('Account.txt', schema_str) }}
  {{ since_last_load('update_dt') }}
),

from_customer_events as (
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
  {% if is_incremental() %}
  and c.update_dt > coalesce(
        (select max(update_dt) from {{ this }} where cdc_flag = 'cust_update'),
        cast('1900-01-01' as date)
      )
  {% endif %}
)

select * from from_account_file
union all
select * from from_customer_events
