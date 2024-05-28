-- Databricks notebook source
INSERT INTO ${catalog}.${wh_db}_${scale_factor}.DIMessages
SELECT
  CURRENT_TIMESTAMP() as MessageDateAndTime,
  ${batch_id} AS BatchID,
  'Phase Complete Record' as MessageSource,
  'Batch Complete' as MessageText,
  'PCR' as MessageType,
  NULL as MessageData;
