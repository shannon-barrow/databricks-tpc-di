{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'append',
    on_schema_change = 'ignore',
  )
}}

{# Fabric DW bronze customer — reads the day's Customer.txt from OneLake in
   place via OPENROWSET(BULK) (fabric__read_daily_csv). Fabric DW is autonomous
   (no CLUSTER BY / dataSkippingNumIndexedCols / file_format=delta). The derived
   OPENROWSET relation needs an explicit alias in T-SQL (`as src`), unlike
   Spark/Snowflake which allow an unaliased derived table. #}

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

select * from {{ read_daily_csv('Customer.txt', schema_str) }} as src
{{ since_last_load('update_dt') }}
