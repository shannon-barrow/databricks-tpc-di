{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'append',
    on_schema_change = 'ignore',
    file_format = 'delta',
  )
}}


{% if var('use_liquid_clustering', false) %}
{# Liquid path: table is pre-created in setup_dbt_liquid.py with #}
{# CLUSTER BY + dataSkippingNumIndexedCols=34. We do NOT declare a layout #}
{# here — declaring liquid_clustered_by in the dbt config makes #}
{# dbt-databricks issue ALTER TABLE CLUSTER BY (and ALTER TABLE SET #}
{# TBLPROPERTIES) on every batch, even when the existing table matches. #}
{# Leaving layout out of dbt config => no DDL noise per batch. #}
{% else %}
{{ config(partition_by='dm_date') }}
{% endif %}

{%- set schema_str -%}
cdc_flag STRING, cdc_dsn BIGINT, dm_date DATE, dm_s_symb STRING,
dm_close DOUBLE, dm_high DOUBLE, dm_low DOUBLE, dm_vol INT
{%- endset -%}

select * from {{ read_daily_csv('DailyMarket.txt', schema_str) }}
{{ since_last_load('dm_date') }}
