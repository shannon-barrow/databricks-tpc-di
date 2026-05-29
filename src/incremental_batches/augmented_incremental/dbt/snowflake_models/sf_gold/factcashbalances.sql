{{
  config(
    materialized = 'incremental',
    on_schema_change = 'ignore',
    full_refresh = false,
    incremental_strategy = 'merge',
    unique_key = ['sk_accountid', 'sk_dateid'],
  )
}}

{# Snowflake variant of factcashbalances. For each account touched this
   batch, write one row at today's sk_dateid keyed (sk_accountid,
   sk_dateid). Older sk_dateid rows untouched.

   Translations:
     - date_format(d, 'yyyyMMdd') -> to_char(d, 'YYYYMMDD')
     - cast(... as bigint)        -> ::number #}

select
  a.sk_customerid,
  a.sk_accountid,
  to_char(c.ct_date, 'YYYYMMDD')::number as sk_dateid,
  c.current_account_cash                  as cash
from {{ ref('currentaccountbalances') }} c
join {{ ref('dimaccount') }} a
  on c.accountid = a.accountid
 and a.iscurrent
where c.latest_batch
