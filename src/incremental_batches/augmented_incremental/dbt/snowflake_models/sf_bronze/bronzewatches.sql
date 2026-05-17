{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'append',
    on_schema_change = 'ignore',
  )
}}

{# Snowflake variant of bronzewatches — stage-read WatchHistory.txt. #}

select * from (
  select
    $1::string    as cdc_flag,
    $2::bigint    as cdc_dsn,
    $3::bigint    as w_c_id,
    $4::string    as w_s_symb,
    $5::timestamp as w_dts,
    $6::string    as w_action,
    $7::date      as event_dt
  from
    @{{ var('snowflake_stage') }}/{{ tgt_db() }}/{{ var('batch_date') }}/WatchHistory.txt
    (file_format => (type => csv field_delimiter => '|' skip_header => 0))
)
{{ since_last_load('event_dt') }}
