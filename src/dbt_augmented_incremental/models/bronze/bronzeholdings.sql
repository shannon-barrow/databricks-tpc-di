{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'append',
    partition_by = 'event_dt',
    on_schema_change = 'ignore',
    file_format = 'delta',
  )
}}

{%- set schema_str -%}
cdc_flag STRING, cdc_dsn BIGINT, hh_h_t_id BIGINT, hh_t_id BIGINT,
hh_before_qty INT, hh_after_qty INT, event_dt DATE
{%- endset -%}

select * from {{ read_daily_csv('HoldingHistory.txt', schema_str) }}
{{ since_last_load('event_dt') }}
