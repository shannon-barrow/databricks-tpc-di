-- Databricks notebook source
INSERT OVERWRITE ${catalog}.${wh_db}_${scale_factor}.${tbl}
SELECT *
FROM parquet.`${tpcdi_directory}sf=${scale_factor}/Batch1/${filename}*.parquet`
