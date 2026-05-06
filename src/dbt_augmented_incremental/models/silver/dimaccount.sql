{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'scd2',
    unique_key = 'accountid',
    on_schema_change = 'ignore',
    file_format = 'delta',
    full_refresh = false,
  )
}}

{# SCD2 build for DimAccount. bronzeaccount holds two flavors of input:
   raw Account.txt rows (cdc_flag = 'I' / 'U') and customer-driven cascade
   rows (cdc_flag = 'cust_update'). Both flow into the same SCD2 MERGE.

   QUALIFY collapses (update_dt, accountid) duplicates: when a single
   batch_date has both an 'I' and a 'U' for the same account, prefer 'I'.
   ORDER BY cdc_flag DESC matches the Classic build's tie-break. #}

with new_events as (
  select * from {{ ref('bronzeaccount') }}
  {% if is_incremental() %}
  where update_dt > coalesce(
        (select max(effectivedate) from {{ this }}),
        cast('1900-01-01' as date)
      )
  {% endif %}
),

deduped as (
  select * except(cdc_dsn)
  from new_events
  qualify row_number() over (
    partition by update_dt, accountid order by cdc_flag desc
  ) = 1
)

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
