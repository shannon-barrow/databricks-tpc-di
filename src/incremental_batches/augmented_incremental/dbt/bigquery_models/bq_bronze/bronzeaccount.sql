{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'append',
    on_schema_change = 'ignore',
  )
}}

select * from {{ source('csv', 'Account') }}
{{ since_last_load('update_dt') }}
