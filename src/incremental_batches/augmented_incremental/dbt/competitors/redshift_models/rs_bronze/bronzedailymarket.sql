{{
  config(
    materialized='incremental',
    incremental_strategy='append',
    on_schema_change='ignore',
    pre_hook=rs_bronze_copy_prehook('DailyMarket'),
  )
}}

{# bronzedailymarket needs accumulator semantics — factmarkethistory does a
   365-day lookback. Append per batch keeps the prior history intact. The
   pre-seeded historical bronzedailymarket from setup_rs's CTAS provides
   the prior-year window. #}

select * from {{ rs_bronze_stg_table() }}
