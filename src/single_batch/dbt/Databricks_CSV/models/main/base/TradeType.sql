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
  fileNamePattern => "TradeType\\.txt",
  schema          => """
    tt_id       STRING,
    tt_name     STRING,
    tt_is_sell  INT,
    tt_is_mrkt  INT
  """
) AS raw;