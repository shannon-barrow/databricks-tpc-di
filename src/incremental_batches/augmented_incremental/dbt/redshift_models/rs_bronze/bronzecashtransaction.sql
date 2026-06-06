{{
  config(
    materialized='incremental',
    incremental_strategy='append',
    on_schema_change='ignore',
    pre_hook=rs_bronze_copy_prehook('CashTransaction'),
  )
}}

select * from {{ rs_bronze_stg_table() }}
