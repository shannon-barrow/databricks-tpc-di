{{
  config(
    materialized = 'incremental',
    on_schema_change = 'ignore',
    full_refresh = false,
    incremental_strategy = 'merge',
    unique_key = ['sk_accountid', 'sk_dateid'],
  )
}}

{# Redshift variant of factcashbalances. For each account touched this batch,
   write one row at today's sk_dateid.

   Translations from BQ:
     - FORMAT_DATE('%Y%m%d', d) -> TO_CHAR(d, 'YYYYMMDD')
     - CAST(... AS INT64)       -> CAST(... AS BIGINT)
#}

select
  a.sk_customerid,
  a.sk_accountid,
  CAST(TO_CHAR(c.ct_date, 'YYYYMMDD') AS BIGINT) as sk_dateid,
  c.current_account_cash                          as cash
from {{ ref('currentaccountbalances') }} c
join {{ ref('dimaccount') }} a
  on c.accountid = a.accountid
 and a.iscurrent
where c.latest_batch
