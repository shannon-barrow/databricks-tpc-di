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

{# Redshift variant — SCD2 via dbt-redshift merge.

   Translations from BQ:
     - FORMAT_DATE('%Y%m%d', d)              -> TO_CHAR(d, 'YYYYMMDD')
     - CAST(... AS INT64)                    -> CAST(... AS BIGINT)
     - CAST(x AS STRING)                     -> CAST(x AS VARCHAR(MAX))
     - DATE '9999-12-31'                     -> CAST('9999-12-31' AS DATE)
     - IF(c, t, f)                           -> CASE WHEN c THEN t ELSE f END
     - SELECT * EXCEPT(col)                  -> spell columns out (Redshift has no EXCEPT/EXCLUDE)
     - QUALIFY ROW_NUMBER() OVER (...) = 1   -> wrap in subquery + WHERE rn = 1
     - boolean literal `true`/`false`         -> kept as Redshift BOOLEAN
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
  -- Redshift doesn't support QUALIFY or SELECT * EXCEPT — drop cdc_dsn by
  -- omission in the projection above and filter rn=1 here.
  select cdc_flag, accountid, brokerid, customerid, accountdesc, taxstatus, status, update_dt
  from ranked
  where rn = 1
),

new_rows as (
  select
    CAST(TO_CHAR(a.update_dt, 'YYYYMMDD') || CAST(a.accountid AS VARCHAR(MAX)) AS BIGINT) as sk_accountid,
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
    CAST('9999-12-31' AS DATE) as enddate
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
