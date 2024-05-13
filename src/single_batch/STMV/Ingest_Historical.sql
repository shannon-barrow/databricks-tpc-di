-- Databricks notebook source
-- CREATE WIDGET DROPDOWN scale_factor DEFAULT "10" CHOICES SELECT * FROM (VALUES ("10"), ("100"), ("1000"), ("5000"), ("10000"));
-- CREATE WIDGET TEXT tpcdi_directory DEFAULT "/Volumes/tpcdi/tpcdi_raw_data/tpcdi_volume/";
-- CREATE WIDGET TEXT wh_db DEFAULT '';
-- CREATE WIDGET TEXT catalog DEFAULT 'tpcdi';
-- CREATE WIDGET TEXT path DEFAULT 'Batch1';
-- CREATE WIDGET TEXT tgt_query DEFAULT '';
-- CREATE WIDGET TEXT table DEFAULT '';
-- CREATE WIDGET TEXT filename DEFAULT '';

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE ${catalog}.${wh_db}_${scale_factor}.${table} AS
SELECT ${tgt_query}
FROM (
  SELECT
    split(value, "[|]") val
  FROM
    STREAM read_files(
      "${tpcdi_directory}sf=${scale_factor}/${path}",
      format => "text",
      inferSchema => False, 
      header => False,
      fileNamePattern => "${filename}"
    )
);
