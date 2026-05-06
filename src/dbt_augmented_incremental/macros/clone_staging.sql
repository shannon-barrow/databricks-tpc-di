{# Run-once macro that clones the per-SF shared staging schema into the
   run's per-user schema. Same logic as setup.py in augmented_incremental.
   Use as: dbt run-operation clone_staging --vars '{...}'  before the
   first batch.

   - Shallow CLONE for read-only reference tables (cheap metadata copy).
   - Deep CLONE for the SCD2 historical tables that the daily MERGEs will
     mutate (so we don't write into the shared staging schema).
#}
{% macro clone_staging() %}
  {%- set tgt = tgt_db() -%}
  {%- set staging = staging_db() -%}

  {%- set shallow_tbls = [
      'taxrate', 'dimdate', 'industry', 'tradetype', 'dimbroker',
      'financial', 'companyyeareps', 'dimsecurity', 'statustype',
      'dimcompany', 'dimtime', 'currentaccountbalances'
  ] -%}
  {%- set deep_tbls = [
      'dimtrade', 'factwatches', 'factholdings',
      'dimcustomer', 'dimaccount', 'factcashbalances'
  ] -%}

  {% do run_query("create schema if not exists " ~ var('catalog') ~ "." ~ tgt) %}

  {% for t in shallow_tbls %}
    {% set sql -%}
      create or replace table {{ var('catalog') }}.{{ tgt }}.{{ t }}
      shallow clone {{ staging }}.{{ t }}
    {%- endset %}
    {{ log("CLONE shallow " ~ t, info=True) }}
    {% do run_query(sql) %}
  {% endfor %}

  {% for t in deep_tbls %}
    {% set sql -%}
      create or replace table {{ var('catalog') }}.{{ tgt }}.{{ t }}
      deep clone {{ staging }}.{{ t }}
    {%- endset %}
    {{ log("CLONE deep " ~ t, info=True) }}
    {% do run_query(sql) %}
  {% endfor %}
{%- endmacro %}
