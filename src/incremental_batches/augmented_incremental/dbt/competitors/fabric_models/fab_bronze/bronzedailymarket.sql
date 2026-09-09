{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'append',
    on_schema_change = 'ignore',
  )
}}

{# Fabric DW bronze daily market — day's DailyMarket.txt via OPENROWSET(BULK).
   NOTE: unlike the 6 streaming bronze tables, this one is seeded by setup with
   the prior year of history (for the FMH 52-week lookback) then appended daily. #}

{%- set schema_str -%}
cdc_flag STRING, cdc_dsn BIGINT, dm_date DATE, dm_s_symb STRING,
dm_close DOUBLE, dm_high DOUBLE, dm_low DOUBLE, dm_vol INT
{%- endset -%}

select * from {{ read_daily_csv('DailyMarket.txt', schema_str) }} as src
{{ since_last_load('dm_date') }}
