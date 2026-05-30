{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'append',
    on_schema_change = 'ignore',
  )
}}

{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by = {'field': 'dm_date', 'data_type': 'date'},
    on_schema_change = 'ignore',
  )
}}

{# Only bronze table that needs accumulator semantics — factmarkethistory
   does a 365-day lookback against bronzedailymarket. insert_overwrite by
   dm_date overwrites the current batch's date partition, leaving prior
   days intact. The pre-seeded historical bronzedailymarket (CLONEd from
   tpcdi_staging_sf{sf} in setup_bq.py) provides the prior-year window. #}

select * from {{ source('csv', 'DailyMarket') }}
{{ since_last_load('dm_date') }}
{{ since_last_load('dm_date') }}
