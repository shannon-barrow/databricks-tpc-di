{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'append',
    on_schema_change = 'ignore',
  )
}}

select * from {{ source('csv', 'HoldingHistory') }}
{{ since_last_load('event_dt') }}
