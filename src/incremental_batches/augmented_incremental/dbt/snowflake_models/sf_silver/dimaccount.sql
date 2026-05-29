{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'sk_accountid',
    merge_update_columns = ['iscurrent', 'enddate'],
    on_schema_change = 'ignore',
    full_refresh = false,
  )
}}

{# Snowflake variant of dimaccount — SCD2 via dbt-snowflake merge.
   Two upstream sources unioned (bronzeaccount + account_updates_from_customer),
   deduped by (update_dt, accountid) keeping highest cdc_flag.

   Translations from Databricks:
     - date_format(d, 'yyyyMMdd') -> to_char(d, 'YYYYMMDD')
     - cast(... as bigint)        -> ::number(38,0)
     - cast('9999-12-31' as date) -> to_date('9999-12-31')
     - select * except(col)       -> Snowflake supports EXCLUDE clause as of 2024
                                     (`SELECT * EXCLUDE col`). Keep `except` if
                                     the adapter handles it; otherwise spell out
                                     the columns. dbt-snowflake compiles this
                                     via the SQL engine — Snowflake natively
                                     supports both `EXCLUDE (col)` and the
                                     non-standard Spark `except(col)`. We use
                                     the standard form.
     - decode/qualify/row_number  -> Snowflake supports natively #}

with new_events as (
  select * from {{ ref('bronzeaccount') }}
  where update_dt = cast('{{ var("batch_date") }}' as date)
  union all
  select * from {{ ref('account_updates_from_customer') }}
  where update_dt = cast('{{ var("batch_date") }}' as date)
),

deduped as (
  select * exclude (cdc_dsn)
  from new_events
  qualify row_number() over (
    partition by update_dt, accountid order by cdc_flag desc
  ) = 1
),

new_rows as (
  select
    (to_char(a.update_dt, 'YYYYMMDD') || a.accountid::varchar)::number(38,0) as sk_accountid,
    a.accountid,
    a.brokerid as sk_brokerid,
    dc.sk_customerid,
    a.accountdesc,
    a.taxstatus,
    decode(a.status,
      'ACTV', 'Active',
      'CMPT', 'Completed',
      'CNCL', 'Canceled',
      'PNDG', 'Pending',
      'SBMT', 'Submitted',
      'INAC', 'Inactive',
      a.status) as status,
    true as iscurrent,
    a.update_dt as effectivedate,
    to_date('9999-12-31') as enddate
  from deduped a
  join {{ ref('dimcustomer') }} dc
    on dc.iscurrent
   and dc.customerid = a.customerid
)

{% if is_incremental() %},

close_rows as (
  select
    t.sk_accountid,
    t.accountid, t.sk_brokerid, t.sk_customerid,
    t.accountdesc, t.taxstatus, t.status,
    false as iscurrent,
    t.effectivedate,
    n.effectivedate as enddate
  from {{ this }} t
  join new_rows n on t.accountid = n.accountid
  where t.iscurrent
)

select * from new_rows
union all
select * from close_rows
{% else %}
select * from new_rows
{% endif %}
