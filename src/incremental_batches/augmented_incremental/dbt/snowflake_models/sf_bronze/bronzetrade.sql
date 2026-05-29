{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'append',
    on_schema_change = 'ignore',
  )
}}

{# Snowflake variant of bronzetrade — stage-read Trade.txt. #}

select * from (
  select
    $1::string    as cdc_flag,
    $2::bigint    as cdc_dsn,
    $3::bigint    as tradeid,
    $4::timestamp as t_dts,
    $5::string    as status,
    $6::string    as t_tt_id,
    $7::tinyint   as cashflag,
    $8::string    as t_s_symb,
    $9::int       as quantity,
    $10::float    as bidprice,
    $11::bigint   as t_ca_id,
    $12::string   as executedby,
    $13::float    as tradeprice,
    $14::float    as fee,
    $15::float    as commission,
    $16::float    as tax,
    $17::date     as event_dt
  from
    @{{ var('snowflake_stage') }}/{{ tgt_db() }}/{{ var('batch_date') }}/Trade.txt
)
{{ since_last_load('event_dt') }}
