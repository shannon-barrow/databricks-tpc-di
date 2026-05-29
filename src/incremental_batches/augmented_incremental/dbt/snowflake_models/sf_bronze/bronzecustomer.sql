{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'append',
    on_schema_change = 'ignore',
  )
}}

{# Snowflake variant of bronzecustomer — reads Customer.txt from the
   external stage. Same positional-cast pattern as the other Snowflake
   bronze models. Layout (cluster keys) is set in setup outside the dbt
   project, not declared here. #}

select * from (
  select
    $1::string  as cdc_flag,
    $2::bigint  as cdc_dsn,
    $3::bigint  as customerid,
    $4::string  as taxid,
    $5::string  as status,
    $6::string  as lastname,
    $7::string  as firstname,
    $8::string  as middleinitial,
    $9::string  as gender,
    $10::tinyint as tier,
    $11::date   as dob,
    $12::string as addressline1,
    $13::string as addressline2,
    $14::string as postalcode,
    $15::string as city,
    $16::string as stateprov,
    $17::string as country,
    $18::string as c_ctry_1,
    $19::string as c_area_1,
    $20::string as c_local_1,
    $21::string as c_ext_1,
    $22::string as c_ctry_2,
    $23::string as c_area_2,
    $24::string as c_local_2,
    $25::string as c_ext_2,
    $26::string as c_ctry_3,
    $27::string as c_area_3,
    $28::string as c_local_3,
    $29::string as c_ext_3,
    $30::string as email1,
    $31::string as email2,
    $32::string as lcl_tx_id,
    $33::string as nat_tx_id,
    $34::date   as update_dt
  from
    @{{ var('snowflake_stage') }}/{{ tgt_db() }}/{{ var('batch_date') }}/Customer.txt
)
{{ since_last_load('update_dt') }}
