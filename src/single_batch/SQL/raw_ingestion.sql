-- Databricks notebook source
-- CREATE WIDGET DROPDOWN scale_factor DEFAULT "10" CHOICES SELECT * FROM (VALUES ("10"), ("100"), ("1000"), ("5000"), ("10000"));
-- CREATE WIDGET TEXT catalog DEFAULT 'tpcdi';
-- CREATE WIDGET TEXT tbl DEFAULT '';
-- CREATE WIDGET TEXT tpcdi_directory DEFAULT "/Volumes/tpcdi/tpcdi_raw_data/tpcdi_volume/";
-- CREATE WIDGET TEXT filename DEFAULT '';
-- CREATE WIDGET TEXT raw_schema DEFAULT '';
-- CREATE WIDGET TEXT wh_db DEFAULT '';

-- COMMAND ----------

INSERT OVERWRITE ${catalog}.${wh_db}_${scale_factor}.${tbl}
SELECT *
FROM read_files(
  "${tpcdi_directory}sf=${scale_factor}/Batch*",
  format => "csv",
  inferSchema => False, 
  header => False,
  sep => "|",
  fileNamePattern => "${filename}", 
  schema => "${raw_schema}"
)
