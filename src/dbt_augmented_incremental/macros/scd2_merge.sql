{# Custom dbt incremental strategy: scd2

   Implements the close-current-row + insert-new-row SCD2 pattern as a
   single MERGE. Mirrors the Classic build's `MERGE INTO ... USING (close
   UNION ALL insert) ON t.<key> = s.mergeKey AND t.iscurrent` shape.

   Model config required:
     materialized = 'incremental'
     incremental_strategy = 'scd2'
     unique_key = '<natural_key_column>'              -- e.g. 'customerid'

   Optional model config (with sensible defaults):
     scd2_current_col   = 'iscurrent'                 -- BOOLEAN column on target
     scd2_effective_col = 'effectivedate'             -- DATE on source rows; written into target.<end_col> on close
     scd2_end_col       = 'enddate'                   -- DATE on target

   Model body must produce candidate rows shaped like the target row,
   one per new entity event (not the dual close/insert form — this
   macro generates that). Source rows are inserted as iscurrent=true
   with effectivedate=<their effectivedate> and enddate=9999-12-31;
   any matching target row whose iscurrent=true gets closed out
   (iscurrent=false, enddate=<source effectivedate>).

   First-run behaviour (target table is empty or doesn't exist): falls
   back to a plain INSERT — every source row is a brand-new record. Same
   as Classic's INSERT OVERWRITE pattern in the historical phase.
#}

{% macro get_incremental_scd2_sql(arg_dict) %}
  {{ return(adapter.dispatch('get_incremental_scd2_sql', 'dbt_augmented_incremental')(arg_dict)) }}
{%- endmacro %}

{% macro databricks__get_incremental_scd2_sql(arg_dict) %}
  {%- set target_relation = arg_dict["target_relation"] -%}
  {%- set source_relation = arg_dict["temp_relation"] -%}
  {%- set unique_key = arg_dict["unique_key"] -%}
  {%- set dest_columns = arg_dict["dest_columns"] -%}

  {%- if unique_key is none -%}
    {{ exceptions.raise_compiler_error("scd2 incremental strategy requires unique_key") }}
  {%- endif -%}

  {%- set current_col = config.get('scd2_current_col', 'iscurrent') -%}
  {%- set effective_col = config.get('scd2_effective_col', 'effectivedate') -%}
  {%- set end_col = config.get('scd2_end_col', 'enddate') -%}

  {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute='name')) -%}

  merge into {{ target_relation }} t using (
    select s.{{ unique_key }} as __scd2_merge_key, s.*
    from {{ source_relation }} s
    join {{ target_relation }} t
      on s.{{ unique_key }} = t.{{ unique_key }}
     and t.{{ current_col }}

    union all

    select cast(null as bigint) as __scd2_merge_key, *
    from {{ source_relation }}
  ) s
  on t.{{ unique_key }} = s.__scd2_merge_key and t.{{ current_col }}
  when matched then update set
    t.{{ current_col }} = false,
    t.{{ end_col }} = s.{{ effective_col }}
  when not matched then insert ({{ dest_cols_csv }})
  values ({% for c in dest_columns %}s.{{ c.name }}{% if not loop.last %}, {% endif %}{% endfor %})
{%- endmacro %}

{% macro snowflake__get_incremental_scd2_sql(arg_dict) %}
  {# Same shape, Snowflake's MERGE syntax is identical for this case. #}
  {{ return(databricks__get_incremental_scd2_sql(arg_dict)) }}
{%- endmacro %}
