{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'append',
    on_schema_change = 'ignore',
  )
}}

{# Fabric DW bronze account — day's Account.txt via OPENROWSET(BULK). Customer-
   driven cascade rows are staged separately in account_updates_from_customer,
   which fab_silver/dimaccount UNIONs in — keeping bronzeaccount pure. #}

{%- set schema_str -%}
cdc_flag STRING, cdc_dsn BIGINT, accountid BIGINT, brokerid BIGINT,
customerid BIGINT, accountdesc STRING, taxstatus TINYINT, status STRING,
update_dt DATE
{%- endset -%}

select * from {{ read_daily_csv('Account.txt', schema_str) }} as src
{{ since_last_load('update_dt') }}
