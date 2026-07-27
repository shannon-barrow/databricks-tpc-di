{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'append',
    on_schema_change = 'ignore',
  )
}}

{# Snowflake variant — reads the day's CSV file from an external stage
   instead of read_files(). Stage tree mirrors the Databricks Volume:
     @{snowflake_stage}/{tgt_db()}/{batch_date}/DailyMarket.txt

   Positional casts ($1::T) project the same columns the read_files
   call projects on Databricks. Same `since_last_load` re-run guard. #}

select * from (
  select
    $1::string  as cdc_flag,
    $2::bigint  as cdc_dsn,
    $3::date    as dm_date,
    $4::string  as dm_s_symb,
    $5::float   as dm_close,
    $6::float   as dm_high,
    $7::float   as dm_low,
    $8::int     as dm_vol
  from
    @{{ var('snowflake_stage') }}/{{ tgt_db() }}/{{ var('batch_date') }}/DailyMarket.txt
)
{{ since_last_load('dm_date') }}
