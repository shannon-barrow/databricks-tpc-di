{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'append',
    on_schema_change = 'ignore',
  )
}}

select * from {{ source('csv', 'CashTransaction') }}
{{ since_last_load('event_dt') }}
