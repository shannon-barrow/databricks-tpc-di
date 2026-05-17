{{
    config(
        materialized = 'table'
    )
}}
SELECT
  batchdate,
  int(substring(_metadata.file_path FROM (position('/Batch', _metadata.file_path) + 6) FOR 1)) batchid
FROM read_files(
  '{{var("rawfilelocation")}}/sf={{var("scalefactor")}}/Batch*',
  format          => "csv",
  header          => "false",
  inferSchema     => false,
  sep             => "|",
  schemaEvolutionMode => 'none',
  fileNamePattern => "BatchDate\\.txt",
  schema          => "batchdate DATE"
) AS raw;