{{
  config(
    materialized='incremental',
    incremental_strategy='append',
    on_schema_change='ignore',
    pre_hook=rs_bronze_copy_prehook('WatchHistory'),
  )
}}

select * from {{ rs_bronze_stg_table() }}
