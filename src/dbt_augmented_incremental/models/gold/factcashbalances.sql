{{
  config(
    materialized = 'incremental',
    on_schema_change = 'ignore',
    file_format = 'delta',
    full_refresh = false,
  )
}}

{# switch to merge with composite key (sk_accountid, sk_dateid).
   Each batch writes one row per touched account keyed at today's sk_dateid;
   merge inserts new (account,date) pairs without updating existing ones.
   No `liquid_clustered_by` here on purpose — the table is pre-created in
   setup_dbt.py with CLUSTER BY (sk_dateid). Declaring it in dbt
   config would force per-batch ALTER TABLE CLUSTER BY. #}
{{ config(
    incremental_strategy='merge',
    unique_key=['sk_accountid','sk_dateid'],
) }}

{# For each account touched this batch, write its (sk_customerid,
   sk_accountid, sk_dateid, cash) row at the latest sk_dateid. Old
   partitions for prior dates stay intact. #}

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
