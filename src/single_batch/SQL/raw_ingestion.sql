-- Databricks notebook source
CREATE OR REPLACE TABLE IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.' || :tbl) (
  ${raw_schema}
  ${constraints}
)
TBLPROPERTIES (${tbl_props});

-- COMMAND ----------

INSERT OVERWRITE IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.' || :tbl)
SELECT *
FROM read_files(
  "${tpcdi_directory}sf=${scale_factor}/Batch*",
  format => "csv",
  inferSchema => False, 
  header => False,
  sep => "|",
  schemaEvolutionMode => 'none',
  fileNamePattern => "${filename}", 
  schema => "${raw_schema}"
)
