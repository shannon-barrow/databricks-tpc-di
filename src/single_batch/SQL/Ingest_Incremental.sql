-- Databricks notebook source
-- CREATE WIDGET DROPDOWN scale_factor DEFAULT "10" CHOICES SELECT * FROM (VALUES ("10"), ("100"), ("1000"), ("5000"), ("10000"));
-- CREATE WIDGET TEXT catalog DEFAULT 'tpcdi';
-- CREATE WIDGET TEXT tbl DEFAULT '';
-- CREATE WIDGET TEXT tpcdi_directory DEFAULT "/Volumes/tpcdi/tpcdi_raw_data/tpcdi_volume/";
-- CREATE WIDGET TEXT filename DEFAULT '';
-- CREATE WIDGET TEXT raw_schema DEFAULT '';
-- CREATE WIDGET TEXT wh_db DEFAULT '';

-- COMMAND ----------

CREATE OR REPLACE TABLE ${catalog}.${wh_db}_${scale_factor}.${tbl} (
  ${raw_schema},
  batchid INT COMMENT 'Batch ID when this record was inserted'
  ${constraints}
)
TBLPROPERTIES (${tbl_props});

-- COMMAND ----------

INSERT OVERWRITE IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.' || :tbl)
SELECT 
  *,
  int(substring(_metadata.file_path FROM (position('/Batch', _metadata.file_path) + 6) FOR 1)) batchid
FROM read_files(
  "${tpcdi_directory}sf=${scale_factor}/Batch*",
  format => "csv",
  inferSchema => False, 
  header => False,
  sep => "|",
  fileNamePattern => "${filename}", 
  schema => "${raw_schema}"
)
