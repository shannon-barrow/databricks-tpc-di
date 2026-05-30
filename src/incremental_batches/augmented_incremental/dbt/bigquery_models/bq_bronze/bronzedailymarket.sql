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
    partition_by = {
      'field': 'dm_date',
      'data_type': 'date',
      'copy_partitions': true,
    },
    on_schema_change = 'ignore',
  )
}}

{# Only bronze table that needs accumulator semantics — factmarkethistory
   does a 365-day lookback against bronzedailymarket. insert_overwrite by
   dm_date overwrites today's partition with the day's rows, leaving prior
   days intact. copy_partitions=True does this via BQ's Copy Table API —
   metadata-only swap, zero compute cost. The pre-seeded historical
   bronzedailymarket (CLONEd from tpcdi_staging_sf{sf} in setup_bq.py)
   provides the prior-year window.

   No since_last_load filter: the external table's wildcard URI already
   resolves to just today's CSV file (simulate_filedrops_bq clears prior
   batches before writing the new one), so the model body produces only
   today's rows naturally. A trailing WHERE would also break dbt-bigquery's
   insert_overwrite SQL template. #}

select * from {{ source('csv', 'DailyMarket') }}
{{ since_last_load('dm_date') }}
