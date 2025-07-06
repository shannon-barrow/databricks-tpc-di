-- Databricks notebook source
CREATE OR REPLACE TABLE IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.' || :tbl) (
  ${raw_schema},
  batchid INT COMMENT 'Batch ID when this record was inserted'
  ${constraints}
)
TBLPROPERTIES (${tblprops});

-- COMMAND ----------

INSERT OVERWRITE IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.' || :tbl)
SELECT 
  *,
  int(substring(_metadata.file_path FROM (position('/Batch', _metadata.file_path) + 6) FOR 1)) batchid
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
