{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'append',
    on_schema_change = 'ignore',
  )
}}

{# Fabric DW bronze watches — day's WatchHistory.txt via OPENROWSET(BULK). #}

{%- set schema_str -%}
cdc_flag STRING, cdc_dsn BIGINT, w_c_id BIGINT, w_s_symb STRING,
w_dts TIMESTAMP, w_action STRING, event_dt DATE
{%- endset -%}

select * from {{ read_daily_csv('WatchHistory.txt', schema_str) }} as src
{{ since_last_load('event_dt') }}
