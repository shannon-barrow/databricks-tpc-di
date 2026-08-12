{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'append',
    on_schema_change = 'ignore',
    file_format = 'delta',
  )
}}


{# Liquid clustered on update_dt (+ dataSkippingNumIndexedCols=34) — defined in
   setup_dbt.py (which pre-creates the table), not here, so dbt-databricks
   doesn't re-issue ALTER TABLE CLUSTER BY every batch ("setup-owns-layout"). #}

{# bronzeaccount holds ONLY the day's Account.txt drop. Customer-driven cascade
   rows (a Customer SCD2 update rippling onto matching account rows) are staged
   separately in `account_updates_from_customer`, which silver/dimaccount UNIONs
   in — keeping bronzeaccount pure. #}

{%- set schema_str -%}
cdc_flag STRING, cdc_dsn BIGINT, accountid BIGINT, brokerid BIGINT,
customerid BIGINT, accountdesc STRING, taxstatus TINYINT, status STRING,
update_dt DATE
{%- endset -%}

select * from {{ read_daily_csv('Account.txt', schema_str) }}
{{ since_last_load('update_dt') }}
