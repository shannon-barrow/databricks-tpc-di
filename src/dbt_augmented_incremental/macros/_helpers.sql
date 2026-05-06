{# Per-run identifiers and helpers. All macros use whitespace-stripping
   `{%-` / `-%}` so they expand to a clean single token suitable for use
   inline in SQL strings (e.g. `read_files('{{ daily_batch_dir() }}/...')`).
#}

{# `tgt_db` mirrors the wh_db_<sf> naming used by the augmented_incremental
   benchmark (so dbt-built schemas don't collide with the SDP / Classic
   schemas at the same SF). #}
{%- macro tgt_db() -%}
{{ var('wh_db') }}_{{ var('scale_factor') }}
{%- endmacro -%}

{%- macro fq(table_name) -%}
{{ var('catalog') }}.{{ tgt_db() }}.{{ table_name }}
{%- endmacro -%}

{# Path under the UC Volume where simulate_filedrops puts the day's part files. #}
{%- macro daily_batch_dir() -%}
{{ var('tpcdi_directory') }}augmented_incremental/_dailybatches/{{ tgt_db() }}/{{ var('batch_date') }}
{%- endmacro -%}

{# Bronze re-run guard. Mirrors the Auto Loader checkpoint behaviour: on
   incremental runs, only ingest rows whose <date_col> is strictly greater
   than the latest already stored. Idempotent across same-day re-runs. #}
{% macro since_last_load(date_col) -%}
  {% if is_incremental() -%}
  where {{ date_col }} > coalesce(
    (select max({{ date_col }}) from {{ this }}),
    cast('1900-01-01' as date)
  )
  {%- endif %}
{%- endmacro %}

{# The reference tables produced by Stage 0 live in
   `{catalog}.{staging_schema}_{sf}`. The dbt project does NOT re-ingest
   them — it CLONEs them into the run schema, then `source()`s them. #}
{%- macro staging_db() -%}
{{ var('catalog') }}.{{ var('staging_schema') }}_{{ var('scale_factor') }}
{%- endmacro -%}

{%- macro staging_fq(table_name) -%}
{{ staging_db() }}.{{ table_name }}
{%- endmacro -%}
