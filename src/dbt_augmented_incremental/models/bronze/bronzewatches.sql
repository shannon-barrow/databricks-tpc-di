{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'append',
    on_schema_change = 'ignore',
    file_format = 'delta',
  )
}}


{% if var('use_liquid_clustering', false) %}
{{ config(liquid_clustered_by='event_dt', tblproperties={'delta.dataSkippingNumIndexedCols': '34'}) }}
{% else %}
{{ config(partition_by='event_dt') }}
{% endif %}

{%- set schema_str -%}
cdc_flag STRING, cdc_dsn BIGINT, w_c_id BIGINT, w_s_symb STRING,
w_dts TIMESTAMP, w_action STRING, event_dt DATE
{%- endset -%}

select * from {{ read_daily_csv('WatchHistory.txt', schema_str) }}
{{ since_last_load('event_dt') }}
