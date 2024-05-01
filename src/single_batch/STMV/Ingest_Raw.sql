-- Databricks notebook source
CREATE OR REFRESH STREAMING TABLE ${catalog}.${tgt_db}.${table} (${raw_schema}${add_tgt_schema}) ${part} AS
SELECT ${tgt_query}
FROM STREAM read_files(
  "${tpcdi_directory}sf=${scale_factor}/${path}",
  format => "csv",
  inferSchema => False, 
  header => False,
  sep => "|",
  fileNamePattern => "${filename}", 
  schema => "${raw_schema}"
)
