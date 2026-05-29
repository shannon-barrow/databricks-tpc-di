{{
  config(
    materialized = 'incremental',
    on_schema_change = 'ignore',
    full_refresh = false,
    incremental_strategy = 'merge',
    unique_key = ['sk_accountid', 'sk_dateid'],
  )
}}

{# BQ variant of factcashbalances. For each account touched this batch,
   write one row at today's sk_dateid.

   Translations from Snowflake:
     - to_char(d, 'YYYYMMDD') -> FORMAT_DATE('%Y%m%d', d)
     - ::number               -> CAST(... AS INT64)
#}

select
  a.sk_customerid,
  a.sk_accountid,
  CAST(FORMAT_DATE('%Y%m%d', c.ct_date) AS INT64) as sk_dateid,
  c.current_account_cash                            as cash
from {{ ref('currentaccountbalances') }} c
join {{ ref('dimaccount') }} a
  on c.accountid = a.accountid
 and a.iscurrent
where c.latest_batch
