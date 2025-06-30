-- Databricks notebook source
INSERT OVERWRITE ${catalog}.${wh_db}_${scale_factor}.FactHoldings 
WITH Holdings as (
  SELECT 
    *, 
    1 batchid
  FROM read_files(
    "${tpcdi_directory}sf=${scale_factor}/Batch1",
    format => "csv",
    inferSchema => False,
    header => False,
    sep => "|",
    fileNamePattern => "HoldingHistory.txt",
    schema => "hh_h_t_id INT, hh_t_id INT, hh_before_qty INT, hh_after_qty INT"
  )
  UNION ALL
  SELECT
    * except(cdc_flag, cdc_dsn),
    int(substring(_metadata.file_path FROM (position('/Batch', _metadata.file_path) + 6) FOR 1)) batchid
  FROM read_files(
    "${tpcdi_directory}sf=${scale_factor}/Batch{2,3}",
    format => "csv",
    inferSchema => False,
    header => False,
    sep => "|",
    fileNamePattern => "HoldingHistory.txt",
    schema => "cdc_flag STRING, cdc_dsn BIGINT, hh_h_t_id INT, hh_t_id INT, hh_before_qty INT, hh_after_qty INT"
  )
)
SELECT
  hh_h_t_id tradeid,
  hh_t_id currenttradeid,
  sk_customerid,
  sk_accountid,
  sk_securityid,
  sk_companyid,
  sk_closedateid sk_dateid,
  sk_closetimeid sk_timeid,
  tradeprice currentprice,
  hh_after_qty currentholding,
  h.batchid
FROM Holdings h
  JOIN ${catalog}.${wh_db}_${scale_factor}.DimTrade dt 
    ON tradeid = hh_t_id
