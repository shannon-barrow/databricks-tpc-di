{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'append',
    on_schema_change = 'ignore',
  )
}}

{# Fabric DW bronze cash transaction — day's CashTransaction.txt via OPENROWSET(BULK). #}

{%- set schema_str -%}
cdc_flag STRING, cdc_dsn BIGINT, accountid BIGINT, ct_dts TIMESTAMP,
ct_amt DOUBLE, ct_name STRING, event_dt DATE
{%- endset -%}

select * from {{ read_daily_csv('CashTransaction.txt', schema_str) }} as src
{{ since_last_load('event_dt') }}
