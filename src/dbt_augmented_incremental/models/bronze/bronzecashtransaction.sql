{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'append',
    on_schema_change = 'ignore',
    file_format = 'delta',
  )
}}


{% if var('use_liquid_clustering', false) %}
{{ config(liquid_clustered_by='event_dt') }}
{% else %}
{{ config(partition_by='event_dt') }}
{% endif %}

{%- set schema_str -%}
cdc_flag STRING, cdc_dsn BIGINT, accountid BIGINT, ct_dts TIMESTAMP,
ct_amt DOUBLE, ct_name STRING, event_dt DATE
{%- endset -%}

select * from {{ read_daily_csv('CashTransaction.txt', schema_str) }}
{{ since_last_load('event_dt') }}
