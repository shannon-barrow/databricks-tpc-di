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

{# merge on the composite key (sk_accountid, sk_dateid): each batch writes one
   row per touched account at today's sk_dateid, and merge inserts new
   (account, date) pairs without updating existing ones.

   The table is liquid clustered on sk_dateid, but that is defined in
   setup_dbt.py (which pre-creates the table), NOT in this dbt model. If we
   declared liquid_clustered_by here, dbt-databricks would re-issue an
   ALTER TABLE CLUSTER BY on every batch even when the layout already matches
   ("setup-owns-layout" pattern). #}

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
