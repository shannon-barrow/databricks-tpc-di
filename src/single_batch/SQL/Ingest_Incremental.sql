-- Databricks notebook source
-- TABLE:VIEW
-- ProspectIncremental:v_Prospect
-- FinWire:v_FinWire
-- BatchDate:v_BatchDate
INSERT INTO ${catalog}.${tgt_db}.${table}
SELECT * FROM ${catalog}.${wh_db}_${scale_factor}_stage.${view}
