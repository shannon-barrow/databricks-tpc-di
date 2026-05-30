{# Adapter dispatch for reading the day's CSV files into a SELECT.

   Usage from a model:
     {{ read_daily_csv('Customer.txt', 'cdc_flag STRING, cdc_dsn BIGINT, ...') }}

   Returns a relation expression suitable as a FROM clause.

   - Databricks: read_files() with the declared schema (FAILFAST mode so a
     malformed CSV produces an obvious error rather than silent NULLs).
   - Snowflake:  staged file read via positional $1::T projection.
   - BigQuery:   pre-built wildcard external table from setup_bq.py.
#}
{% macro read_daily_csv(filename, schema_str) %}
  {{ return(adapter.dispatch('read_daily_csv', 'dbt_augmented_incremental')(filename, schema_str)) }}
{%- endmacro %}

{% macro databricks__read_daily_csv(filename, schema_str) %}
  read_files(
    '{{ daily_batch_dir() }}/{{ filename }}',
    format => 'csv',
    schema => '{{ schema_str }}',
    sep => '|',
    header => false,
    mode => 'FAILFAST'
  )
{%- endmacro %}

{% macro bigquery__read_daily_csv(filename, schema_str) %}
  {# BigQuery: SELECT from the pre-built wildcard external table that
     setup_bq.py creates under `{wh_db}_{sf}_bronze`. One external table
     per dataset, URI pattern `gs://.../_dailybatches/{wh_db}_{sf}/*/{Dataset}.txt`
     — the wildcard always resolves to the current batch's file because
     simulate_filedrops_bq clears the prior batch's dir before writing the
     new one. Reads are zero-copy + cheap: BQ scans the day's CSV directly.

     `schema_str` is intentionally ignored here — the external table's
     column types are declared at create time (see DATASET_SCHEMAS in
     setup_bq.py). `filename` is `Customer.txt` / `DailyMarket.txt` / etc.;
     the BQ external table is named just the stem (`Customer`). #}
  {%- set table_name = filename.rsplit('.', 1)[0] -%}
  (select * from `{{ var('catalog') }}.{{ tgt_db() }}_bronze.{{ table_name }}`)
{%- endmacro %}

{% macro snowflake__read_daily_csv(filename, schema_str) %}
  {# Snowflake reads from an external stage. The {{ var('snowflake_stage') }}
     variable controls which stage; default 'tpcdi_stage'. The stage URL
     should point at the same _dailybatches/ tree the Databricks adapter
     reads via read_files(). The schema_str is parsed positionally. #}
  {%- set parts = schema_str.split(',') -%}
  {%- set col_select -%}
    {%- for part in parts -%}
      {%- set toks = part.strip().split() -%}
      ${{ loop.index }}::{{ toks[1] }} as {{ toks[0] }}{% if not loop.last %}, {% endif %}
    {%- endfor -%}
  {%- endset -%}
  (
    select {{ col_select }}
    from @{{ var('snowflake_stage', 'tpcdi_stage') }}/{{ tgt_db() }}/{{ var('batch_date') }}/{{ filename }}
      (file_format => (type => csv field_delimiter => '|'))
  )
{%- endmacro %}
