{{
    config(
        materialized = 'table'
    )
}}
SELECT
  *
FROM
  parquet.`{{var("rawfilelocation")}}/sf={{var("scalefactor")}}/Batch1/Time.parquet`;