{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'append',
    on_schema_change = 'ignore',
  )
}}

{# Snowflake variant of bronzeaccount — stage-read Account.txt. Pure
   Account.txt drops only; customer-cascade rows are staged in
   account_updates_from_customer and UNIONed by silver/dimaccount. #}

select * from (
  select
    $1::string  as cdc_flag,
    $2::bigint  as cdc_dsn,
    $3::bigint  as accountid,
    $4::bigint  as brokerid,
    $5::bigint  as customerid,
    $6::string  as accountdesc,
    $7::tinyint as taxstatus,
    $8::string  as status,
    $9::date    as update_dt
  from
    @{{ var('snowflake_stage') }}/{{ tgt_db() }}/{{ var('batch_date') }}/Account.txt
    (file_format => (type => csv field_delimiter => '|' skip_header => 0))
)
{{ since_last_load('update_dt') }}
