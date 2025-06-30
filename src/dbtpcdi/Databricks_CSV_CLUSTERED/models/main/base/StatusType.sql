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
  fileNamePattern => "StatusType\\.txt",
  schema          => """
    st_id   STRING,
    st_name STRING
  """
) AS raw;