{{
    config(
        materialized = 'table'
    )
}}
SELECT *
FROM read_files(
  '{{var("rawfilelocation")}}/sf={{var("scalefactor")}}/Batch1',
  format          => "csv",
  header          => "false",
  inferSchema     => false,
  sep             => "|",
  schemaEvolutionMode => 'none',
  fileNamePattern => "Industry\\.txt",
  schema          => """
    in_id    STRING,
    in_name  STRING,
    in_sc_id STRING
  """
) AS raw;