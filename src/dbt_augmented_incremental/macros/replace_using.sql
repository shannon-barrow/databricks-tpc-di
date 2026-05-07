{# Custom dbt incremental strategy: replace_using

   Generates Delta Lake's `INSERT INTO target REPLACE USING (cols) SELECT
   * FROM source` — atomic key-based selective overwrite. For each source
   row, target rows matching on the configured columns are replaced;
   non-matching target rows are untouched; source rows with no match are
   inserted. Same upsert semantic as a MERGE on those keys but simpler /
   faster in the optimizer.

   Empty source = no-op (unlike replace_where, which would delete rows
   matching the predicate).

   Model config:
     materialized = 'incremental'
     incremental_strategy = 'replace_using'
     unique_key = ['col_a', 'col_b']     -- list or string

   Works on SQL Warehouses, serverless, and classic compute (no
   spark.session config required).
#}

{% macro get_incremental_replace_using_sql(arg_dict) %}
  {{ return(adapter.dispatch('get_incremental_replace_using_sql', 'dbt_augmented_incremental')(arg_dict)) }}
{%- endmacro %}

{% macro databricks__get_incremental_replace_using_sql(arg_dict) %}
  {%- set target_relation = arg_dict["target_relation"] -%}
  {%- set source_relation = arg_dict["temp_relation"] -%}
  {%- set unique_key = arg_dict["unique_key"] -%}

  {%- if unique_key is none -%}
    {{ exceptions.raise_compiler_error("replace_using incremental strategy requires unique_key (string or list of column names)") }}
  {%- endif -%}

  {%- if unique_key is string -%}
    {%- set keys = [unique_key] -%}
  {%- else -%}
    {%- set keys = unique_key -%}
  {%- endif -%}

  insert into {{ target_relation }}
  replace using ({{ keys | join(', ') }})
  select * from {{ source_relation }}
{%- endmacro %}

{% macro snowflake__get_incremental_replace_using_sql(arg_dict) %}
  {{ exceptions.raise_compiler_error("replace_using is Delta Lake-specific; not available on Snowflake. Use 'merge' for the Snowflake adapter.") }}
{%- endmacro %}
