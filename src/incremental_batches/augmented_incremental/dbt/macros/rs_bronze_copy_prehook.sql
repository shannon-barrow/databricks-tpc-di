{# Macros that emit the per-batch COPY pre-hook for Redshift bronze
   incremental models. Mirrors the SF/BQ bronze pattern but keeps the
   bronze ingestion INSIDE the dbt run on the same compute that runs
   silver/gold, so per-batch cost attribution stays apples-to-apples.

   The pre-hook (returned as a list of SQL statements):
     1. drops the per-batch staging temp table (defensive)
     2. creates a fresh TEMP TABLE matching the bronze model target schema
        (`LIKE {{ this }}`)
     3. COPYs the day's CSV
        (s3://.../_dailybatches/{wh_db}_{sf}/{batch}/{Dataset}.txt)
        into the temp table — CSV delimiter '|', auto-typed date/time

   The bronze model body then does `select * from <stg>`. Combined with
   `incremental_strategy='append'`, each batch INSERTs the day's rows into
   the persistent bronze table.

   On the first run the bronze table is created by setup_rs.py's CTAS
   step (staging → main schema) so the target has the right schema for
   `CREATE TEMP TABLE foo_stg (LIKE foo)`.

   Required vars (passed via `dbt run --vars` from run_dbt.py):
     - s3_volume_prefix (e.g. s3://tpcds-datasets/shannon_tpcdi/)
     - wh_db, scale_factor, batch_date
     - rs_iam_role
     - file_ext (default 'txt')
     - aws_region (default 'us-west-2')
#}

{%- macro rs_bronze_copy_prehook(dataset_name) -%}
  {%- set s3_prefix = var('s3_volume_prefix', 's3://tpcds-datasets/shannon_tpcdi/') -%}
  {%- set s3_uri = s3_prefix ~ 'augmented_incremental/_dailybatches/' ~ var('wh_db') ~ '_' ~ var('scale_factor') ~ '/' ~ var('batch_date') ~ '/' ~ dataset_name ~ '.' ~ var('file_ext', 'txt') -%}
  {%- set iam_role = var('rs_iam_role') -%}
  {%- set aws_region = var('aws_region', 'us-west-2') -%}
  {%- set stg = this.identifier ~ '_stg' -%}
  {%- do return([
    "DROP TABLE IF EXISTS " ~ stg,
    "CREATE TEMP TABLE " ~ stg ~ " (LIKE " ~ this ~ ")",
    "COPY " ~ stg ~ " FROM '" ~ s3_uri ~ "' IAM_ROLE '" ~ iam_role ~ "' FORMAT AS CSV DELIMITER '|' TIMEFORMAT 'auto' DATEFORMAT 'auto' EMPTYASNULL BLANKSASNULL ACCEPTINVCHARS COMPUPDATE OFF STATUPDATE OFF REGION '" ~ aws_region ~ "'"
  ]) -%}
{%- endmacro -%}


{%- macro rs_bronze_stg_table() -%}
{{ this.identifier }}_stg
{%- endmacro -%}
