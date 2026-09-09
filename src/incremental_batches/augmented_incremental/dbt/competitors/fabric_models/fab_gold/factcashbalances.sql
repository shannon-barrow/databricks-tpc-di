{{
  config(
    materialized = 'incremental',
    on_schema_change = 'ignore',
    full_refresh = false,
    incremental_strategy = 'merge',
    unique_key = ['sk_accountid', 'sk_dateid'],
  )
}}

{# Redshift -> Fabric T-SQL — one row per account touched this batch at today's
   sk_dateid.
     - TO_CHAR(d, 'YYYYMMDD') -> CONVERT(CHAR(8), d, 112)
     - join/where <boolcol>   -> <boolcol> = 1
#}

select
  a.sk_customerid,
  a.sk_accountid,
  CAST(CONVERT(CHAR(8), c.ct_date, 112) AS BIGINT) as sk_dateid,
  c.current_account_cash                            as cash
from {{ ref('currentaccountbalances') }} c
join {{ ref('dimaccount') }} a
  on c.accountid = a.accountid
 and a.iscurrent = 1
where c.latest_batch = 1
