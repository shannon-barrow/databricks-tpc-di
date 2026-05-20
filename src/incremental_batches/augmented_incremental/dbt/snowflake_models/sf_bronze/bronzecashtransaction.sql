{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'append',
    on_schema_change = 'ignore',
  )
}}

{# Snowflake variant of bronzecashtransaction — stage-read CashTransaction.txt. #}

select * from (
  select
    $1::string    as cdc_flag,
    $2::bigint    as cdc_dsn,
    $3::bigint    as accountid,
    $4::timestamp as ct_dts,
    $5::float     as ct_amt,
    $6::string    as ct_name,
    $7::date      as event_dt
  from
    @{{ var('snowflake_stage') }}/{{ tgt_db() }}/{{ var('batch_date') }}/CashTransaction.txt
)
{{ since_last_load('event_dt') }}
