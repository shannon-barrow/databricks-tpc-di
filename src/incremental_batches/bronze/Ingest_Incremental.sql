-- Databricks notebook source
-- TABLE:VIEW
-- ProspectIncremental:v_Prospect
-- CustomerIncremental:v_CustomerIncremental
-- TradeIncremental:v_TradeIncremental
-- CashTransactionIncremental:v_CashTransactionIncremental
-- DailyMarketIncremental:v_DailyMarketIncremental
-- FinWire:v_FinWire
-- BatchDate:v_BatchDate
INSERT INTO ${catalog}.${tgt_db}.${table}
SELECT * FROM ${catalog}.${wh_db}_${scale_factor}_stage.${view}
