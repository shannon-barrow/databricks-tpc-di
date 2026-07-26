{{
  config(
    materialized='incremental',
    incremental_strategy='append',
    on_schema_change='ignore',
    pre_hook=rs_bronze_copy_prehook('Customer'),
  )
}}

{# Per-batch Redshift bronze customer. Pre-hook above COPYs Customer.txt
   into a TEMP table matching this model's schema; body just appends.
   First run's target table is the staging CTAS from setup_rs.py — that's
   what gives `LIKE {{ this }}` a valid template. #}

select * from {{ rs_bronze_stg_table() }}
