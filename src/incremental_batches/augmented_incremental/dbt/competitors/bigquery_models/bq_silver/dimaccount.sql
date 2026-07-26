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

{# BQ variant — SCD2 via dbt-bigquery merge.

   Translations from Snowflake:
     - select * exclude (col) -> SELECT * EXCEPT(col) (BQ standard)
     - qualify row_number()    -> BQ supports QUALIFY (since 2023)
     - to_char(d, 'YYYYMMDD')  -> FORMAT_DATE('%Y%m%d', d)
     - ::number(38,0)          -> CAST(... AS INT64)
     - decode(...)             -> CASE WHEN END
     - to_date('9999-12-31')   -> DATE '9999-12-31'
#}

with new_events as (
  select * from {{ ref('bronzeaccount') }}
  where update_dt = DATE '{{ var("batch_date") }}'
  union all
  select * from {{ ref('account_updates_from_customer') }}
  where update_dt = DATE '{{ var("batch_date") }}'
),

deduped as (
  select * except(cdc_dsn)
  from new_events
  qualify row_number() over (
    partition by update_dt, accountid order by cdc_flag desc
  ) = 1
),

new_rows as (
  select
    CAST(CONCAT(FORMAT_DATE('%Y%m%d', a.update_dt), CAST(a.accountid AS STRING)) AS INT64) as sk_accountid,
    a.accountid,
    a.brokerid as sk_brokerid,
    dc.sk_customerid,
    a.accountdesc,
    a.taxstatus,
    CASE a.status
      WHEN 'ACTV' THEN 'Active'
      WHEN 'CMPT' THEN 'Completed'
      WHEN 'CNCL' THEN 'Canceled'
      WHEN 'PNDG' THEN 'Pending'
      WHEN 'SBMT' THEN 'Submitted'
      WHEN 'INAC' THEN 'Inactive'
      ELSE a.status
    END as status,
    true as iscurrent,
    a.update_dt as effectivedate,
    DATE '9999-12-31' as enddate
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
