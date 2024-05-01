-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Notebook Purpose
-- MAGIC * Batch Validation Script that was refactored FROM the one found in AppENDix B

-- COMMAND ----------

-- CREATE WIDGET DROPDOWN scale_factor DEFAULT "10" CHOICES SELECT * FROM (VALUES ("10"), ("100"), ("1000"), ("5000"), ("10000"));
-- CREATE WIDGET TEXT tpcdi_directory DEFAULT "/Volumes/tpcdi/tpcdi_raw_data/tpcdi_volume/";
-- CREATE WIDGET TEXT wh_db DEFAULT '';
-- CREATE WIDGET TEXT catalog DEFAULT 'tpcdi';
-- CREATE WIDGET DROPDOWN batch_id DEFAULT '0' CHOICES SELECT * FROM (VALUES ("0"), ("1"), ("2"), ("3"));

-- COMMAND ----------

use catalog ${catalog};
use ${wh_db}_${scale_factor};

INSERT INTO DIMessages
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
  FROM
    DimAccount
  UNION
  SELECT
    'DimBroker' AS MessageSource,
    'Row count' AS MessageText,
    COUNT(1) AS MessageData
  FROM
    DimBroker
  UNION
  SELECT
    'DimCompany' AS MessageSource,
    'Row count' AS MessageText,
    COUNT(1) AS MessageData
  FROM
    DimCompany
  UNION
  SELECT
    'DimCustomer' AS MessageSource,
    'Row count' AS MessageText,
    COUNT(1) AS MessageData
  FROM
    DimCustomer
  UNION
  SELECT
    'DimDate' AS MessageSource,
    'Row count' AS MessageText,
    COUNT(1) AS MessageData
  FROM
    DimDate
  UNION
  SELECT
    'DimSecurity' AS MessageSource,
    'Row count' AS MessageText,
    COUNT(1) AS MessageData
  FROM
    DimSecurity
  UNION
  SELECT
    'DimTime' AS MessageSource,
    'Row count' AS MessageText,
    COUNT(1) AS MessageData
  FROM
    DimTime
  UNION
  SELECT
    'DimTrade' AS MessageSource,
    'Row count' AS MessageText,
    COUNT(1) AS MessageData
  FROM
    DimTrade
  UNION
  SELECT
    'FactCashBalances' AS MessageSource,
    'Row count' AS MessageText,
    COUNT(1) AS MessageData
  FROM
    FactCashBalances
  UNION
  SELECT
    'FactHoldings' AS MessageSource,
    'Row count' AS MessageText,
    COUNT(1) AS MessageData
  FROM
    FactHoldings
  UNION
  SELECT
    'FactMarketHistory' AS MessageSource,
    'Row count' AS MessageText,
    COUNT(1) AS MessageData
  FROM
    FactMarketHistory
  UNION
  SELECT
    'FactWatches' AS MessageSource,
    'Row count' AS MessageText,
    COUNT(1) AS MessageData
  FROM
    FactWatches
  UNION
  SELECT
    'Financial' AS MessageSource,
    'Row count' AS MessageText,
    COUNT(1) AS MessageData
  FROM
    Financial
  UNION
  SELECT
    'Industry' AS MessageSource,
    'Row count' AS MessageText,
    COUNT(1) AS MessageData
  FROM
    Industry
  UNION
  SELECT
    'Prospect' AS MessageSource,
    'Row count' AS MessageText,
    COUNT(1) AS MessageData
  FROM
    Prospect
  UNION
  SELECT
    'StatusType' AS MessageSource,
    'Row count' AS MessageText,
    COUNT(1) AS MessageData
  FROM
    StatusType
  UNION
  SELECT
    'TaxRate' AS MessageSource,
    'Row count' AS MessageText,
    COUNT(1) AS MessageData
  FROM
    TaxRate
  UNION
  SELECT
    'TradeType' AS MessageSource,
    'Row count' AS MessageText,
    COUNT(1) AS MessageData
  FROM
    TradeType
  UNION
  SELECT
    'FactCashBalances' AS MessageSource,
    'Row count joined' AS MessageText,
    COUNT(1) AS MessageData
  FROM
    FactCashBalances f
    INNER JOIN DimAccount a ON f.SK_AccountID = a.SK_AccountID
    INNER JOIN DimCustomer c ON f.SK_CustomerID = c.SK_CustomerID
    INNER JOIN DimBroker b ON a.SK_BrokerID = b.SK_BrokerID
    INNER JOIN DimDate d ON f.SK_DateID = d.SK_DateID
  UNION
  SELECT
    'FactHoldings' AS MessageSource,
    'Row count joined' AS MessageText,
    COUNT(1) AS MessageData
  FROM
    FactHoldings f
    INNER JOIN DimAccount a ON f.SK_AccountID = a.SK_AccountID
    INNER JOIN DimCustomer c ON f.SK_CustomerID = c.SK_CustomerID
    INNER JOIN DimBroker b ON a.SK_BrokerID = b.SK_BrokerID
    INNER JOIN DimDate d ON f.SK_DateID = d.SK_DateID
    INNER JOIN DimTime t ON f.SK_TimeID = t.SK_TimeID
    INNER JOIN DimCompany m ON f.SK_CompanyID = m.SK_CompanyID
    INNER JOIN DimSecurity s ON f.SK_SecurityID = s.SK_SecurityID
  UNION
  SELECT
    'FactMarketHistory' AS MessageSource,
    'Row count joined' AS MessageText,
    COUNT(1) AS MessageData
  FROM
    FactMarketHistory f
    INNER JOIN DimDate d ON f.SK_DateID = d.SK_DateID
    INNER JOIN DimCompany m ON f.SK_CompanyID = m.SK_CompanyID
    INNER JOIN DimSecurity s ON f.SK_SecurityID = s.SK_SecurityID
  UNION
  SELECT
    'FactWatches' AS MessageSource,
    'Row count joined' AS MessageText,
    COUNT(1) AS MessageData
  FROM
    FactWatches f
    INNER JOIN DimCustomer c ON f.SK_CustomerID = c.SK_CustomerID
    INNER JOIN DimDate dp ON f.SK_DateID_DatePlaced = dp.SK_DateID
    INNER JOIN DimSecurity s ON f.SK_SecurityID = s.SK_SecurityID
  UNION
  SELECT
    'DimCustomer' AS MessageSource,
    'Inactive customers' AS MessageText,
    COUNT(1)
  FROM
    DimCustomer
  where
    IsCurrent
    and Status = 'Inactive'
  UNION
  SELECT
    'FactWatches' AS MessageSource,
    'Inactive watches' AS MessageText,
    COUNT(1)
  FROM
    FactWatches
  where
    SK_DATEID_DATEREMOVED is not null
)
