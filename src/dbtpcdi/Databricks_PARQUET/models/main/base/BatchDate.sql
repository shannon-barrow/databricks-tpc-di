{{
    config(
        materialized = 'table'
    )
}}
SELECT
  batchdate,
  int(substring(_metadata.file_path FROM (position('/Batch', _metadata.file_path) + 6) FOR 1)) batchid
FROM
  parquet.`{{var("rawfilelocation")}}/sf={{var("scalefactor")}}/Batch*/BatchDate.parquet`;