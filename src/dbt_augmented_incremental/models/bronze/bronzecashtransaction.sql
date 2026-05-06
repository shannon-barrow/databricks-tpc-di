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
cdc_flag STRING, cdc_dsn BIGINT, accountid BIGINT, ct_dts TIMESTAMP,
ct_amt DOUBLE, ct_name STRING, event_dt DATE
{%- endset -%}

select * from {{ read_daily_csv('CashTransaction.txt', schema_str) }}
{{ since_last_load('event_dt') }}
