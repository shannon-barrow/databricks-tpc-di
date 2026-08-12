{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'append',
    on_schema_change = 'ignore',
    file_format = 'delta',
  )
}}


{# Liquid clustered on dm_date (+ dataSkippingNumIndexedCols=34) — defined
   in setup_dbt.py (which pre-creates the table), not here, so dbt-databricks
   doesn't re-issue ALTER TABLE CLUSTER BY every batch ("setup-owns-layout"). #}

{%- set schema_str -%}
cdc_flag STRING, cdc_dsn BIGINT, dm_date DATE, dm_s_symb STRING,
dm_close DOUBLE, dm_high DOUBLE, dm_low DOUBLE, dm_vol INT
{%- endset -%}

select * from {{ read_daily_csv('DailyMarket.txt', schema_str) }}
{{ since_last_load('dm_date') }}
