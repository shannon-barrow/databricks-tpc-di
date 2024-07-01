-- Databricks notebook source
CREATE OR REPLACE VIEW ${catalog}.${wh_db}_${scale_factor}_stage.${{view}} AS
SELECT ${tgt_query}
FROM
  (
    SELECT
      split(value, "[|]") val,
      substring(_metadata.file_path FROM (position('/Batch', _metadata.file_path) + 6) FOR 1) batchid 
    FROM
      text.`${tpcdi_directory}sf=${scale_factor}/${path}/${filename}`
  );
