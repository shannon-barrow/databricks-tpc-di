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

{# Redshift -> Fabric T-SQL — SCD2 via dbt-fabric merge.
     - TO_CHAR(d, 'YYYYMMDD')  -> CONVERT(CHAR(8), d, 112)
     - CAST(x AS VARCHAR(MAX)) -> CAST(x AS VARCHAR(30))
     - boolean true/false      -> CAST(1 AS BIT) / CAST(0 AS BIT)
     - join/where <boolcol>    -> <boolcol> = 1
     - ROW_NUMBER() OVER(...) = 1 dedup kept as-is
#}

with new_events as (
  select * from {{ ref('bronzeaccount') }}
  where update_dt = CAST('{{ var("batch_date") }}' AS DATE)
  union all
  select * from {{ ref('account_updates_from_customer') }}
  where update_dt = CAST('{{ var("batch_date") }}' AS DATE)
),

ranked as (
  select
    cdc_flag, accountid, brokerid, customerid, accountdesc, taxstatus, status, update_dt,
    row_number() over (
      partition by update_dt, accountid order by cdc_flag desc
    ) as rn
  from new_events
),

deduped as (
  select cdc_flag, accountid, brokerid, customerid, accountdesc, taxstatus, status, update_dt
  from ranked
  where rn = 1
),

new_rows as (
  select
    CAST(CONVERT(CHAR(8), a.update_dt, 112) + CAST(a.accountid AS VARCHAR(30)) AS BIGINT) as sk_accountid,
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
    CAST(1 AS BIT) as iscurrent,
    a.update_dt as effectivedate,
    CAST('9999-12-31' AS DATE) as enddate
  from deduped a
  join {{ ref('dimcustomer') }} dc
    on dc.iscurrent = 1
   and dc.customerid = a.customerid
)

{% if is_incremental() %},

close_rows as (
  select
    t.sk_accountid,
    t.accountid, t.sk_brokerid, t.sk_customerid,
    t.accountdesc, t.taxstatus, t.status,
    CAST(0 AS BIT) as iscurrent,
    t.effectivedate,
    n.effectivedate as enddate
  from {{ this }} t
  join new_rows n on t.accountid = n.accountid
  where t.iscurrent = 1
)

select * from new_rows
union all
select * from close_rows
{% else %}
select * from new_rows
{% endif %}
