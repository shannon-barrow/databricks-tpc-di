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
  fileNamePattern => "Time\\.txt",
  schema          => """
    sk_timeid        BIGINT,
    timevalue        STRING,
    hourid           INT,
    hourdesc         STRING,
    minuteid         INT,
    minutedesc       STRING,
    secondid         INT,
    seconddesc       STRING,
    markethoursflag  BOOLEAN,
    officehoursflag  BOOLEAN
  """
) AS raw;