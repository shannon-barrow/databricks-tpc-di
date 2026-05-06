{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'append',
    partition_by = 'event_dt',
    on_schema_change = 'ignore',
    file_format = 'delta',
  )
}}

{%- set schema_str -%}
cdc_flag STRING, cdc_dsn BIGINT, tradeid BIGINT, t_dts TIMESTAMP,
status STRING, t_tt_id STRING, cashflag TINYINT, t_s_symb STRING,
quantity INT, bidprice DOUBLE, t_ca_id BIGINT, executedby STRING,
tradeprice DOUBLE, fee DOUBLE, commission DOUBLE, tax DOUBLE,
event_dt DATE
{%- endset -%}

select * from {{ read_daily_csv('Trade.txt', schema_str) }}
{{ since_last_load('event_dt') }}
