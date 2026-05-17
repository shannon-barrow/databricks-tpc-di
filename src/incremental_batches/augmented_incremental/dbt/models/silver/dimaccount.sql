{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'sk_accountid',
    merge_update_columns = ['iscurrent', 'enddate'],
    on_schema_change = 'ignore',
    file_format = 'delta',
    full_refresh = false,
  )
}}

{# SCD2 via stock dbt-databricks merge + dual-row pattern (close + insert).
   See models/silver/dimcustomer.sql for the pattern explanation.

   Two upstream sources, kept separate per variant parity:
     * bronzeaccount                  — raw Account.txt rows (cdc_flag 'I'/'U')
     * account_updates_from_customer  — derived rows (cdc_flag 'cust_update')
                                        from bronzecustomer 'U' events
                                        joined to this dimaccount AS-OF
                                        batch start

   QUALIFY collapses (update_dt, accountid) duplicates: when a single
   batch_date has both an 'I' and a 'U' (or a cust_update) for the same
   account, ORDER BY cdc_flag DESC keeps the highest-sorting flag —
   matches the Classic build's tie-break. #}

with new_events as (
  select * from {{ ref('bronzeaccount') }}
  where update_dt = cast('{{ var("batch_date") }}' as date)
  union all
  select * from {{ ref('account_updates_from_customer') }}
  where update_dt = cast('{{ var("batch_date") }}' as date)
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
    cast(concat(date_format(a.update_dt, 'yyyyMMdd'), a.accountid) as bigint) as sk_accountid,
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
    cast('9999-12-31' as date) as enddate
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
