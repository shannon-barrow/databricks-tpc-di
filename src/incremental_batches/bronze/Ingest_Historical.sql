-- Databricks notebook source
INSERT INTO ${catalog}.${tgt_db}.${table}
SELECT ${tgt_query}
FROM read_files(
  "${tpcdi_directory}sf=${scale_factor}/${path}",
  format => "csv",
  inferSchema => False, 
  header => False,
  sep => "|",
  fileNamePattern => "${filename}", 
  schema => "${raw_schema}"
)
