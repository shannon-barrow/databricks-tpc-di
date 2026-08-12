{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'append',
    on_schema_change = 'ignore',
    file_format = 'delta',
  )
}}


{# Table is liquid clustered on event_dt (plus delta.dataSkippingNumIndexedCols=34), but that layout is defined in
   setup_dbt.py, which pre-creates the table — NOT in this dbt model. If we
   declared liquid_clustered_by here, dbt-databricks would re-issue an
   ALTER TABLE CLUSTER BY (and ALTER TABLE SET TBLPROPERTIES) on every batch
   even when the layout already matches ("setup-owns-layout" pattern). #}

{%- set schema_str -%}
cdc_flag STRING, cdc_dsn BIGINT, accountid BIGINT, ct_dts TIMESTAMP,
ct_amt DOUBLE, ct_name STRING, event_dt DATE
{%- endset -%}

select * from {{ read_daily_csv('CashTransaction.txt', schema_str) }}
{{ since_last_load('event_dt') }}
