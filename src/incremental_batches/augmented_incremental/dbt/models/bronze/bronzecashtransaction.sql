{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'append',
    on_schema_change = 'ignore',
    file_format = 'delta',
  )
}}


{# Liquid clustered on event_dt (+ dataSkippingNumIndexedCols=34) — defined
   in setup_dbt.py (which pre-creates the table), not here, so dbt-databricks
   doesn't re-issue ALTER TABLE CLUSTER BY every batch ("setup-owns-layout"). #}

{%- set schema_str -%}
cdc_flag STRING, cdc_dsn BIGINT, accountid BIGINT, ct_dts TIMESTAMP,
ct_amt DOUBLE, ct_name STRING, event_dt DATE
{%- endset -%}

select * from {{ read_daily_csv('CashTransaction.txt', schema_str) }}
{{ since_last_load('event_dt') }}
