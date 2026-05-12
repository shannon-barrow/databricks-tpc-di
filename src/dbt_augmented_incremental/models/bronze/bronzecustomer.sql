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
cdc_flag STRING, cdc_dsn BIGINT, customerid BIGINT, taxid STRING, status STRING,
lastname STRING, firstname STRING, middleinitial STRING, gender STRING,
tier TINYINT, dob DATE, addressline1 STRING, addressline2 STRING,
postalcode STRING, city STRING, stateprov STRING, country STRING,
c_ctry_1 STRING, c_area_1 STRING, c_local_1 STRING, c_ext_1 STRING,
c_ctry_2 STRING, c_area_2 STRING, c_local_2 STRING, c_ext_2 STRING,
c_ctry_3 STRING, c_area_3 STRING, c_local_3 STRING, c_ext_3 STRING,
email1 STRING, email2 STRING, lcl_tx_id STRING, nat_tx_id STRING,
update_dt DATE
{%- endset -%}

select * from {{ read_daily_csv('Customer.txt', schema_str) }}
{{ since_last_load('update_dt') }}
