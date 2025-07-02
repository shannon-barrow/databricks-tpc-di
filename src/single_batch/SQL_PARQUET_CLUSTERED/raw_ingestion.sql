-- Databricks notebook source
CREATE OR REPLACE TABLE ${catalog}.${wh_db}_${scale_factor}.${tbl} (
  ${raw_schema}
  ${constraints}
)
TBLPROPERTIES (${tbl_props});

-- COMMAND ----------

INSERT OVERWRITE ${catalog}.${wh_db}_${scale_factor}.${tbl}
SELECT *
FROM parquet.`${tpcdi_directory}sf=${scale_factor}/Batch1/${filename}*.parquet`
