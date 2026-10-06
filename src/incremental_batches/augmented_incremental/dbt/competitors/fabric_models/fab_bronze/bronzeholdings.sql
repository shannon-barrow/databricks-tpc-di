{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'append',
    on_schema_change = 'ignore',
  )
}}

{# Fabric DW bronze holdings — day's HoldingHistory.txt via OPENROWSET(BULK). #}

{%- set schema_str -%}
cdc_flag STRING, cdc_dsn BIGINT, hh_h_t_id BIGINT, hh_t_id BIGINT,
hh_before_qty INT, hh_after_qty INT, event_dt DATE
{%- endset -%}

select * from {{ read_daily_csv('HoldingHistory.txt', schema_str) }} as src
{{ since_last_load('event_dt') }}
