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

{% macro redshift__read_daily_csv(filename, schema_str) %}
  {# Redshift's bronze ingestion does NOT use this macro — bronze models in
     `redshift_models/rs_bronze/` use a `pre_hook` that issues a `COPY ...
     FROM 's3://...'` into a temp table, with the model body just selecting
     `* FROM <stg>`. See `dbt/macros/rs_bronze_copy_prehook.sql`.

     This stub exists only so dbt's `adapter.dispatch('read_daily_csv', ...)`
     resolves when the manifest is COMPILED against target.type='redshift'.
     The Databricks-tree bronze models (the callers) are gated by
     `+enabled: target.type == 'databricks'`, so this body should never
     execute on a Redshift run. But dbt still Jinja-compiles disabled
     models during parse, so the macro must emit valid SQL (not raise) —
     otherwise the parse phase fails before dbt ever decides what to skip.

     Emits a no-op `select` that's syntactically valid but returns zero
     rows. If a Databricks-only bronze model accidentally executes here
     it'll silently produce nothing; that's surfaced downstream as
     "silver got no rows from bronzeX" — clearer than a compile-time
     dead-end. #}
  (select null::varchar(1) as not_a_real_bronze_row where 1=0)
{%- endmacro %}

{% macro _fabric_tsql_type(spark_type) %}
  {#- Map the portable type token in schema_str to a Fabric DW T-SQL type for
     the OPENROWSET WITH clause. Unknown -> varchar(8000). -#}
  {%- set t = spark_type.strip().upper() -%}
  {%- if t == 'STRING' -%}varchar(8000)
  {%- elif t == 'BIGINT' or t == 'LONG' -%}bigint
  {%- elif t in ['INT','INTEGER'] -%}int
  {%- elif t == 'TINYINT' -%}tinyint
  {%- elif t == 'SMALLINT' -%}smallint
  {%- elif t == 'DATE' -%}date
  {%- elif t in ['TIMESTAMP','DATETIME'] -%}datetime2
  {%- elif t in ['DOUBLE','FLOAT'] -%}float
  {%- elif t == 'BOOLEAN' -%}bit
  {%- elif t.startswith('DECIMAL') or t.startswith('NUMERIC') -%}{{ t.lower() }}
  {%- else -%}varchar(8000)
  {%- endif -%}
{% endmacro %}

{% macro fabric__read_daily_csv(filename, schema_str) %}
  {# Fabric Data Warehouse reads the day's CSV in place via OPENROWSET(BULK).
     Fabric DW requires an ABSOLUTE path — var('fabric_files_url') is the
     OneLake Files base the fabric simulate_filedrops drops per-day files under.
     The WITH clause binds CSV columns BY ORDINAL, so we emit the declared names
     + T-SQL types in schema order (mirrors the Snowflake @stage $N::T
     projection). NOTE: OneLake OPENROWSET is in preview; schema_str must not
     contain scale-commas (e.g. decimal(10,2)) since it is comma-split. #}
  {%- set parts = schema_str.split(',') -%}
  {%- set with_cols -%}
    {%- for part in parts -%}
      {%- set toks = part.strip().split() -%}
      {{ toks[0] }} {{ dbt_augmented_incremental._fabric_tsql_type(toks[1]) }}{% if not loop.last %}, {% endif %}
    {%- endfor -%}
  {%- endset -%}
  {%- set col_names -%}
    {%- for part in parts -%}
      {%- set toks = part.strip().split() -%}
      {{ toks[0] }}{% if not loop.last %}, {% endif %}
    {%- endfor -%}
  {%- endset -%}
  (
    select {{ col_names }}
    from openrowset(
      bulk '{{ var("fabric_files_url") }}/{{ tgt_db() }}/{{ var("batch_date") }}/{{ filename }}',
      format = 'csv',
      fieldterminator = '|',
      rowterminator = '0x0a',
      firstrow = 1
    ) with ( {{ with_cols }} ) as data
  )
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
