# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook Purpose
# MAGIC * Batch Validation Script that was refactored from the one found in Appendix B

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

user_name = spark.sql("select current_user()").collect()[0][0].split("@")[0].replace(".","_")
dbutils.widgets.text("wh_db", f"{user_name}_TPCDI",'Root name of Target Warehouse')
dbutils.widgets.text("batch_id", "0", "Batch ID (1,2,3)")

wh_db = f"{dbutils.widgets.get('wh_db')}_wh"
staging_db = f"{dbutils.widgets.get('wh_db')}_stage"
batch_id = dbutils.widgets.get("batch_id")

# COMMAND ----------

spark.sql(f"""
  insert into {wh_db}.DIMessages (
    select
      CURRENT_TIMESTAMP() as MessageDateAndTime,
      case
        when BatchID is null then 0
        else BatchID
        end as BatchID,
      MessageSource,
      MessageText,
      'Validation' as MessageType,
      MessageData
    from (select { batch_id } as BatchID)
      cross join (
        select
          'DimAccount' as MessageSource,
          'Row count' as MessageText,
          count(*) as MessageData
        from
          {wh_db}.DimAccount
        union
        select
          'DimBroker' as MessageSource,
          'Row count' as MessageText,
          count(*) as MessageData
        from
          {wh_db}.DimBroker
        union
        select
          'DimCompany' as MessageSource,
          'Row count' as MessageText,
          count(*) as MessageData
        from
          {wh_db}.DimCompany
        union
        select
          'DimCustomer' as MessageSource,
          'Row count' as MessageText,
          count(*) as MessageData
        from
          {wh_db}.DimCustomer
        union
        select
          'DimDate' as MessageSource,
          'Row count' as MessageText,
          count(*) as MessageData
        from
          {wh_db}.DimDate
        union
        select
          'DimSecurity' as MessageSource,
          'Row count' as MessageText,
          count(*) as MessageData
        from
          {wh_db}.DimSecurity
        union
        select
          'DimTime' as MessageSource,
          'Row count' as MessageText,
          count(*) as MessageData
        from
          {wh_db}.DimTime
        union
        select
          'DimTrade' as MessageSource,
          'Row count' as MessageText,
          count(*) as MessageData
        from
          {wh_db}.DimTrade
        union
        select
          'FactCashBalances' as MessageSource,
          'Row count' as MessageText,
          count(*) as MessageData
        from
          {wh_db}.FactCashBalances
        union
        select
          'FactHoldings' as MessageSource,
          'Row count' as MessageText,
          count(*) as MessageData
        from
          {wh_db}.FactHoldings
        union
        select
          'FactMarketHistory' as MessageSource,
          'Row count' as MessageText,
          count(*) as MessageData
        from
          {wh_db}.FactMarketHistory
        union
        select
          'FactWatches' as MessageSource,
          'Row count' as MessageText,
          count(*) as MessageData
        from
          {wh_db}.FactWatches
        union
        select
          'Financial' as MessageSource,
          'Row count' as MessageText,
          count(*) as MessageData
        from
          {wh_db}.Financial
        union
        select
          'Industry' as MessageSource,
          'Row count' as MessageText,
          count(*) as MessageData
        from
          {wh_db}.Industry
        union
        select
          'Prospect' as MessageSource,
          'Row count' as MessageText,
          count(*) as MessageData
        from
          {wh_db}.Prospect
        union
        select
          'StatusType' as MessageSource,
          'Row count' as MessageText,
          count(*) as MessageData
        from
          {wh_db}.StatusType
        union
        select
          'TaxRate' as MessageSource,
          'Row count' as MessageText,
          count(*) as MessageData
        from
          {wh_db}.TaxRate
        union
        select
          'TradeType' as MessageSource,
          'Row count' as MessageText,
          count(*) as MessageData
        from
          {wh_db}.TradeType
        union
        select
          'FactCashBalances' as MessageSource,
          'Row count joined' as MessageText,
          count(*) as MessageData
        from
          {wh_db}.FactCashBalances f
          inner join {wh_db}.DimAccount a on f.SK_AccountID = a.SK_AccountID
          inner join {wh_db}.DimCustomer c on f.SK_CustomerID = c.SK_CustomerID
          inner join {wh_db}.DimBroker b on a.SK_BrokerID = b.SK_BrokerID
          inner join {wh_db}.DimDate d on f.SK_DateID = d.SK_DateID
        union
        select
          'FactHoldings' as MessageSource,
          'Row count joined' as MessageText,
          count(*) as MessageData
        from
          {wh_db}.FactHoldings f
          inner join {wh_db}.DimAccount a on f.SK_AccountID = a.SK_AccountID
          inner join {wh_db}.DimCustomer c on f.SK_CustomerID = c.SK_CustomerID
          inner join {wh_db}.DimBroker b on a.SK_BrokerID = b.SK_BrokerID
          inner join {wh_db}.DimDate d on f.SK_DateID = d.SK_DateID
          inner join {wh_db}.DimTime t on f.SK_TimeID = t.SK_TimeID
          inner join {wh_db}.DimCompany m on f.SK_CompanyID = m.SK_CompanyID
          inner join {wh_db}.DimSecurity s on f.SK_SecurityID = s.SK_SecurityID
        union
        select
          'FactMarketHistory' as MessageSource,
          'Row count joined' as MessageText,
          count(*) as MessageData
        from
          {wh_db}.FactMarketHistory f
          inner join {wh_db}.DimDate d on f.SK_DateID = d.SK_DateID
          inner join {wh_db}.DimCompany m on f.SK_CompanyID = m.SK_CompanyID
          inner join {wh_db}.DimSecurity s on f.SK_SecurityID = s.SK_SecurityID
        union
        select
          'FactWatches' as MessageSource,
          'Row count joined' as MessageText,
          count(*) as MessageData
        from
          {wh_db}.FactWatches f
          inner join {wh_db}.DimCustomer c on f.SK_CustomerID = c.SK_CustomerID
          inner join {wh_db}.DimDate dp on f.SK_DateID_DatePlaced = dp.SK_DateID
          inner join {wh_db}.DimSecurity s on f.SK_SecurityID = s.SK_SecurityID
          /* Additional information used at Audit time */
        union
        select
          'DimCustomer' as MessageSource,
          'Inactive customers' as MessageText,
          count(*)
        from
          {wh_db}.DimCustomer
        where
          IsCurrent = 1
          and Status = 'Inactive'
        union
        select
          'FactWatches' as MessageSource,
          'Inactive watches' as MessageText,
          count(*)
        from
          {wh_db}.FactWatches
        where
          SK_DATEID_DATEREMOVED is not null
      )
    ---------------------------------------------------
    -- ADDING ALERTS FROM HERE
    ---------------------------------------------------
    UNION
    SELECT
      CURRENT_TIMESTAMP() as MessageDateAndTime,
      case
        when BatchID is null then 0
        else BatchID
        end as BatchID,
      MessageSource,
      MessageText,
      'Alert' as MessageType,
      MessageData
    FROM (SELECT {batch_id} batchid)
    CROSS JOIN (
      SELECT 
        'DimCustomer' MessageSource,
        'Invalid customer tier' MessageText,
        concat('C_ID = ', customerid, ', C_TIER = ', nvl(cast(tier as string), 'null')) MessageData
      FROM (
        SELECT 
            customerid, 
            tier
        FROM {wh_db}.DimCustomer
        WHERE 
          batchid = {batch_id}
          AND (
            tier NOT IN (1,2,3)
            OR tier is null)
        QUALIFY ROW_NUMBER() OVER (PARTITION BY customerid ORDER BY enddate desc) = 1)
      UNION ALL
      SELECT DISTINCT
        'DimCustomer' MessageSource,
        'DOB out of range' MessageText,
        concat('C_ID = ', customerid, ', C_DOB = ', dob) MessageData
      FROM {wh_db}.DimCustomer dc
      JOIN {wh_db}.batchdate bd
        USING (batchid)
      WHERE
        dc.batchid = {batch_id}
        AND bd.batchid = {batch_id} 
        AND (
          datediff(YEAR, dob, batchdate) >= 100
          OR dob > batchdate)
      UNION ALL
      SELECT DISTINCT
        'DimTrade' MessageSource,
        'Invalid trade commission' MessageText,
        concat('T_ID = ', tradeid, ', T_COMM = ', commission) MessageData
      FROM {wh_db}.DimTrade
      WHERE
        batchid = {batch_id}
        AND commission IS NOT NULL
        AND commission > tradeprice * quantity
      UNION ALL
      SELECT DISTINCT
        'DimTrade' MessageSource,
        'Invalid trade fee' MessageText,
        concat('T_ID = ', tradeid, ', T_CHRG = ', fee) MessageData
      FROM {wh_db}.DimTrade
      WHERE
        batchid = {batch_id}
        AND fee IS NOT NULL
        AND fee > tradeprice * quantity
      UNION ALL
      SELECT DISTINCT
        'FactMarketHistory' MessageSource,
        'No earnings for company' MessageText,
        concat('DM_S_SYMB = ', symbol) MessageData
      FROM {wh_db}.FactMarketHistory fmh
      JOIN {wh_db}.DimSecurity ds 
        ON 
          ds.sk_securityid = fmh.sk_securityid
      WHERE
        fmh.batchid = {batch_id}
        AND PERatio IS NULL
      UNION ALL
      SELECT DISTINCT
        'DimCompany' MessageSource,
        'Invalid SPRating' MessageText,
        concat('CO_ID = ', cik, ', CO_SP_RATE = ', sprating) MessageData
      FROM (
        SELECT trim(substring(value, 79, 10)) CIK, trim(substring(value, 95, 4)) sprating 
        FROM {staging_db}.finwire
        WHERE rectype = 'CMP')
      WHERE sprating IN ('AAA','AA','A','BBB','BB','B','CCC','CC','C','D','AA+','A+','BBB+','BB+','B+','CCC+','AA-','A-','BBB-','BB-','B-','CCC-')
    )
  )
""")
