-- Databricks notebook source
-- CREATE WIDGET DROPDOWN scale_factor DEFAULT "10" CHOICES SELECT * FROM (VALUES ("10"), ("100"), ("1000"), ("5000"), ("10000"));
-- CREATE WIDGET TEXT wh_db DEFAULT '';
-- CREATE WIDGET TEXT tgt_db DEFAULT '';
-- CREATE WIDGET TEXT catalog DEFAULT 'tpcdi';
-- CREATE WIDGET TEXT table DEFAULT '';
-- CREATE WIDGET TEXT view DEFAULT '';

-- COMMAND ----------

-- TABLE:VIEW
-- ProspectIncremental:v_Prospect
-- FinWire:v_FinWire
-- BatchDate:v_BatchDate
CREATE MATERIALIZED VIEW IF NOT EXISTS ${catalog}.${tgt_db}.${table} AS
SELECT * FROM ${catalog}.${wh_db}_${scale_factor}_stage.${view}
