-- Databricks notebook source
-- CREATE WIDGET DROPDOWN scale_factor DEFAULT "10" CHOICES SELECT * FROM (VALUES ("10"), ("100"), ("1000"), ("5000"), ("10000"));
-- CREATE WIDGET TEXT tpcdi_directory DEFAULT "/Volumes/tpcdi/tpcdi_raw_data/tpcdi_volume/";
-- CREATE WIDGET TEXT wh_db DEFAULT '';
-- CREATE WIDGET TEXT catalog DEFAULT 'tpcdi';
-- CREATE WIDGET DROPDOWN batch_id DEFAULT '0' CHOICES SELECT * FROM (VALUES ("0"), ("1"), ("2"), ("3"));

-- COMMAND ----------

INSERT INTO ${catalog}.${wh_db}_${scale_factor}.DIMessages
SELECT
  CURRENT_TIMESTAMP() AS MessageDateAndTime,
  ${batch_id} AS BatchID,
  MessageSource,
  MessageText,
  'Validation' AS MessageType,
  MessageData
FROM (
  SELECT
    'DimAccount' AS MessageSource,
    'Row count' AS MessageText,
    COUNT(1) AS MessageData
  FROM ${catalog}.${wh_db}_${scale_factor}.DimAccount
  UNION
  SELECT
    'DimBroker' AS MessageSource,
    'Row count' AS MessageText,
    COUNT(1) AS MessageData
  FROM ${catalog}.${wh_db}_${scale_factor}.DimBroker
  UNION
  SELECT
    'DimCompany' AS MessageSource,
    'Row count' AS MessageText,
    COUNT(1) AS MessageData
  FROM ${catalog}.${wh_db}_${scale_factor}.DimCompany
  UNION
  SELECT
    'DimCustomer' AS MessageSource,
    'Row count' AS MessageText,
    COUNT(1) AS MessageData
  FROM ${catalog}.${wh_db}_${scale_factor}.DimCustomer
  UNION
  SELECT
    'DimDate' AS MessageSource,
    'Row count' AS MessageText,
    COUNT(1) AS MessageData
  FROM ${catalog}.${wh_db}_${scale_factor}.DimDate
  UNION
  SELECT
    'DimSecurity' AS MessageSource,
    'Row count' AS MessageText,
    COUNT(1) AS MessageData
  FROM ${catalog}.${wh_db}_${scale_factor}.DimSecurity
  UNION
  SELECT
    'DimTime' AS MessageSource,
    'Row count' AS MessageText,
    COUNT(1) AS MessageData
  FROM ${catalog}.${wh_db}_${scale_factor}.DimTime
  UNION
  SELECT
    'DimTrade' AS MessageSource,
    'Row count' AS MessageText,
    COUNT(1) AS MessageData
  FROM ${catalog}.${wh_db}_${scale_factor}.DimTrade
  UNION
  SELECT
    'FactCashBalances' AS MessageSource,
    'Row count' AS MessageText,
    COUNT(1) AS MessageData
  FROM ${catalog}.${wh_db}_${scale_factor}.FactCashBalances
  UNION
  SELECT
    'FactHoldings' AS MessageSource,
    'Row count' AS MessageText,
    COUNT(1) AS MessageData
  FROM ${catalog}.${wh_db}_${scale_factor}.FactHoldings
  UNION
  SELECT
    'FactMarketHistory' AS MessageSource,
    'Row count' AS MessageText,
    COUNT(1) AS MessageData
  FROM ${catalog}.${wh_db}_${scale_factor}.FactMarketHistory
  UNION
  SELECT
    'FactWatches' AS MessageSource,
    'Row count' AS MessageText,
    COUNT(1) AS MessageData
  FROM ${catalog}.${wh_db}_${scale_factor}.FactWatches
  UNION
  SELECT
    'Financial' AS MessageSource,
    'Row count' AS MessageText,
    COUNT(1) AS MessageData
  FROM ${catalog}.${wh_db}_${scale_factor}.Financial
  UNION
  SELECT
    'Industry' AS MessageSource,
    'Row count' AS MessageText,
    COUNT(1) AS MessageData
  FROM ${catalog}.${wh_db}_${scale_factor}.Industry
  UNION
  SELECT
    'Prospect' AS MessageSource,
    'Row count' AS MessageText,
    COUNT(1) AS MessageData
  FROM ${catalog}.${wh_db}_${scale_factor}.Prospect
  UNION
  SELECT
    'StatusType' AS MessageSource,
    'Row count' AS MessageText,
    COUNT(1) AS MessageData
  FROM ${catalog}.${wh_db}_${scale_factor}.StatusType
  UNION
  SELECT
    'TaxRate' AS MessageSource,
    'Row count' AS MessageText,
    COUNT(1) AS MessageData
  FROM ${catalog}.${wh_db}_${scale_factor}.TaxRate
  UNION
  SELECT
    'TradeType' AS MessageSource,
    'Row count' AS MessageText,
    COUNT(1) AS MessageData
  FROM ${catalog}.${wh_db}_${scale_factor}.TradeType
  UNION
  SELECT
    'FactCashBalances' AS MessageSource,
    'Row count joined' AS MessageText,
    COUNT(1) AS MessageData
  FROM ${catalog}.${wh_db}_${scale_factor}.FactCashBalances f
    INNER JOIN ${catalog}.${wh_db}_${scale_factor}.DimAccount a ON f.SK_AccountID = a.SK_AccountID
    INNER JOIN ${catalog}.${wh_db}_${scale_factor}.DimCustomer c ON f.SK_CustomerID = c.SK_CustomerID
    INNER JOIN ${catalog}.${wh_db}_${scale_factor}.DimBroker b ON a.SK_BrokerID = b.SK_BrokerID
    INNER JOIN ${catalog}.${wh_db}_${scale_factor}.DimDate d ON f.SK_DateID = d.SK_DateID
  UNION
  SELECT
    'FactHoldings' AS MessageSource,
    'Row count joined' AS MessageText,
    COUNT(1) AS MessageData
  FROM ${catalog}.${wh_db}_${scale_factor}.FactHoldings f
    INNER JOIN ${catalog}.${wh_db}_${scale_factor}.DimAccount a ON f.SK_AccountID = a.SK_AccountID
    INNER JOIN ${catalog}.${wh_db}_${scale_factor}.DimCustomer c ON f.SK_CustomerID = c.SK_CustomerID
    INNER JOIN ${catalog}.${wh_db}_${scale_factor}.DimBroker b ON a.SK_BrokerID = b.SK_BrokerID
    INNER JOIN ${catalog}.${wh_db}_${scale_factor}.DimDate d ON f.SK_DateID = d.SK_DateID
    INNER JOIN ${catalog}.${wh_db}_${scale_factor}.DimTime t ON f.SK_TimeID = t.SK_TimeID
    INNER JOIN ${catalog}.${wh_db}_${scale_factor}.DimCompany m ON f.SK_CompanyID = m.SK_CompanyID
    INNER JOIN ${catalog}.${wh_db}_${scale_factor}.DimSecurity s ON f.SK_SecurityID = s.SK_SecurityID
  UNION
  SELECT
    'FactMarketHistory' AS MessageSource,
    'Row count joined' AS MessageText,
    COUNT(1) AS MessageData
  FROM ${catalog}.${wh_db}_${scale_factor}.FactMarketHistory f
    INNER JOIN ${catalog}.${wh_db}_${scale_factor}.DimDate d ON f.SK_DateID = d.SK_DateID
    INNER JOIN ${catalog}.${wh_db}_${scale_factor}.DimCompany m ON f.SK_CompanyID = m.SK_CompanyID
    INNER JOIN ${catalog}.${wh_db}_${scale_factor}.DimSecurity s ON f.SK_SecurityID = s.SK_SecurityID
  UNION
  SELECT
    'FactWatches' AS MessageSource,
    'Row count joined' AS MessageText,
    COUNT(1) AS MessageData
  FROM ${catalog}.${wh_db}_${scale_factor}.FactWatches f
    INNER JOIN ${catalog}.${wh_db}_${scale_factor}.DimCustomer c ON f.SK_CustomerID = c.SK_CustomerID
    INNER JOIN ${catalog}.${wh_db}_${scale_factor}.DimDate dp ON f.SK_DateID_DatePlaced = dp.SK_DateID
    INNER JOIN ${catalog}.${wh_db}_${scale_factor}.DimSecurity s ON f.SK_SecurityID = s.SK_SecurityID
  UNION
  SELECT
    'DimCustomer' AS MessageSource,
    'Inactive customers' AS MessageText,
    COUNT(1)
  FROM ${catalog}.${wh_db}_${scale_factor}.DimCustomer
  where
    IsCurrent
    and Status = 'Inactive'
  UNION
  SELECT
    'FactWatches' AS MessageSource,
    'Inactive watches' AS MessageText,
    COUNT(1)
  FROM ${catalog}.${wh_db}_${scale_factor}.FactWatches
  where
    SK_DATEID_DATEREMOVED is not null
)
