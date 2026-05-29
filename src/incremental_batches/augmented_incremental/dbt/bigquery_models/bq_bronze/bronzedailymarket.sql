{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'append',
    on_schema_change = 'ignore',
  )
}}

select * from {{ source('csv', 'DailyMarket') }}
{{ since_last_load('dm_date') }}
