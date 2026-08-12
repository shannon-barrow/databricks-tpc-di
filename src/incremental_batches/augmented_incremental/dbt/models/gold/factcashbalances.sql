{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = ['sk_accountid', 'sk_dateid'],
    on_schema_change = 'ignore',
    file_format = 'delta',
    full_refresh = false,
  )
}}

{# merge on (sk_accountid, sk_dateid): each batch writes one (sk_customerid,
   sk_accountid, sk_dateid, cash) row per touched account at today's sk_dateid;
   new (account, date) pairs are inserted and prior dates are left intact.

   Liquid clustered on sk_dateid — defined in setup_dbt.py (which pre-creates
   the table), not here, so dbt-databricks doesn't re-issue ALTER TABLE
   CLUSTER BY every batch ("setup-owns-layout"). #}

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
