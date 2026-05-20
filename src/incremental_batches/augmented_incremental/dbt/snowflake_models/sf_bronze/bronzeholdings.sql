{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'append',
    on_schema_change = 'ignore',
  )
}}

{# Snowflake variant of bronzeholdings — stage-read HoldingHistory.txt. #}

select * from (
  select
    $1::string as cdc_flag,
    $2::bigint as cdc_dsn,
    $3::bigint as hh_h_t_id,
    $4::bigint as hh_t_id,
    $5::int    as hh_before_qty,
    $6::int    as hh_after_qty,
    $7::date   as event_dt
  from
    @{{ var('snowflake_stage') }}/{{ tgt_db() }}/{{ var('batch_date') }}/HoldingHistory.txt
)
{{ since_last_load('event_dt') }}
