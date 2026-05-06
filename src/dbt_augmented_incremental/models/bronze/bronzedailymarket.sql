{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'append',
    partition_by = 'dm_date',
    on_schema_change = 'ignore',
    file_format = 'delta',
  )
}}

{%- set schema_str -%}
cdc_flag STRING, cdc_dsn BIGINT, dm_date DATE, dm_s_symb STRING,
dm_close DOUBLE, dm_high DOUBLE, dm_low DOUBLE, dm_vol INT
{%- endset -%}

select * from {{ read_daily_csv('DailyMarket.txt', schema_str) }}
{{ since_last_load('dm_date') }}
