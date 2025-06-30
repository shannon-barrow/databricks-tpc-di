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
  fileNamePattern => "TaxRate\\.txt",
  schema          => """
    tx_id   STRING,
    tx_name STRING,
    tx_rate FLOAT
  """
) AS raw;