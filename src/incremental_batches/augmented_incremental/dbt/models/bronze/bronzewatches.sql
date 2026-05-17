{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'append',
    on_schema_change = 'ignore',
    file_format = 'delta',
  )
}}


{# Table is pre-created in setup_dbt.py with CLUSTER BY + #}
{# delta.dataSkippingNumIndexedCols=34. We do NOT declare a layout here — #}
{# declaring liquid_clustered_by in the dbt config makes dbt-databricks #}
{# issue ALTER TABLE CLUSTER BY (and ALTER TABLE SET TBLPROPERTIES) on #}
{# every batch, even when the existing table matches. Layout is owned by #}
{# the setup notebook ("setup-owns-layout" pattern), not dbt. #}

{%- set schema_str -%}
cdc_flag STRING, cdc_dsn BIGINT, w_c_id BIGINT, w_s_symb STRING,
w_dts TIMESTAMP, w_action STRING, event_dt DATE
{%- endset -%}

select * from {{ read_daily_csv('WatchHistory.txt', schema_str) }}
{{ since_last_load('event_dt') }}
