# Databricks notebook source
# MAGIC %md
# MAGIC # Audit Checks

# COMMAND ----------

user_name = spark.sql("select current_user()").collect()[0][0].split("@")[0].replace(".","_")
dbutils.widgets.text("wh_db", f"{user_name}_TPCDI", 'Root name of Target Warehouse')
wh_db = f"{dbutils.widgets.get('wh_db')}_wh"

# COMMAND ----------

spark.sql(f"use {wh_db}")

# COMMAND ----------

# MAGIC %sql
# MAGIC ANALYZE TABLE DimTrade COMPUTE STATISTICS FOR ALL COLUMNS;
# MAGIC ANALYZE TABLE DimCustomer COMPUTE STATISTICS FOR ALL COLUMNS;
# MAGIC ANALYZE TABLE FactHoldings COMPUTE STATISTICS FOR ALL COLUMNS;
# MAGIC ANALYZE TABLE Financial COMPUTE STATISTICS FOR ALL COLUMNS;
# MAGIC ANALYZE TABLE Prospect COMPUTE STATISTICS FOR ALL COLUMNS;
# MAGIC ANALYZE TABLE FactWatches COMPUTE STATISTICS FOR ALL COLUMNS;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE automated_audit_results
# MAGIC as
# MAGIC select * from (
# MAGIC select 'Audit table batches' as Test, NULL as Batch, case when 
# MAGIC       (select count(distinct BatchID) from Audit) = 3 and
# MAGIC       (select max(BatchID) from Audit) = 3
# MAGIC then 'OK' else 'Not 3 batches' end as Result, 'There must be audit data for 3 batches' as Description
# MAGIC union all
# MAGIC select 'Audit table sources' as Test, NULL as Batch, case when 
# MAGIC       (select count(distinct DataSet) 
# MAGIC        from Audit 
# MAGIC        where DataSet in (   
# MAGIC             'Batch',
# MAGIC             'DimAccount',
# MAGIC             'DimBroker',
# MAGIC             'DimCompany',
# MAGIC             'DimCustomer',
# MAGIC             'DimSecurity',
# MAGIC             'DimTrade',
# MAGIC             'FactHoldings',     
# MAGIC             'FactMarketHistory',
# MAGIC             'FactWatches',
# MAGIC             'Financial',
# MAGIC             'Generator',
# MAGIC             'Prospect'
# MAGIC       ) ) = 13
# MAGIC then 'OK' else 'Mismatch' end as Result, 'There must be audit data for all data sets' as Description
# MAGIC  
# MAGIC -- Checks against the DImessages table.
# MAGIC union all
# MAGIC select 'DImessages validation reports', BatchID, Result, 'Every batch must have a full set of validation reports' from (
# MAGIC       select distinct BatchID, (
# MAGIC             case when 
# MAGIC                   (select count(*) from DImessages where BatchID = a.BatchID and MessageType = 'Validation') = 24
# MAGIC             then 'OK' else 'Validation checks not fully reported' end
# MAGIC       ) as Result
# MAGIC       from DImessages a
# MAGIC ) o
# MAGIC 
# MAGIC union all
# MAGIC select 'DImessages batches', NULL, case when 
# MAGIC       (select count(distinct BatchID) from DImessages) = 4 and
# MAGIC       (select max(BatchID) from DImessages) = 3
# MAGIC then 'OK' else 'Not 3 batches plus batch 0' end, 'Must have 3 distinct batches reported in DImessages'
# MAGIC 
# MAGIC union all
# MAGIC select 'DImessages Phase complete records', NULL, case when 
# MAGIC       (select count(distinct BatchID) from DImessages where MessageType = 'PCR') = 4 and
# MAGIC       (select max(BatchID) from DImessages where MessageType = 'PCR') = 3
# MAGIC then 'OK' else 'Not 4 Phase Complete Records' end, 'Must have 4 Phase Complete records'
# MAGIC 
# MAGIC union all
# MAGIC select 'DImessages sources', NULL, case when 
# MAGIC       (select count(*) from (
# MAGIC             select distinct MessageSource from DImessages where MessageType = 'Validation' and MessageSource in 
# MAGIC                   ('DimAccount','DimBroker','DimCustomer','DimDate','DimSecurity','DimTime','DimTrade','FactCashBalances','FactHoldings',
# MAGIC                    'FactMarketHistory','FactWatches','Financial','Industry','Prospect','StatusType','TaxRate','TradeType')
# MAGIC       ) a ) = 17
# MAGIC then 'OK' else 'Mismatch' end, 'Messages must be present for all tables/transforms'
# MAGIC 
# MAGIC union all
# MAGIC select 'DImessages initial condition', NULL, case when 
# MAGIC       (select count(*) from DImessages where BatchID = 0 and MessageType = 'Validation' and MessageData <> '0') = 0
# MAGIC then 'OK' else 'Non-empty table in before Batch1' end, 'All DW tables must be empty before Batch1'
# MAGIC 
# MAGIC -- Checks against the DimBroker table. 
# MAGIC union all
# MAGIC select 'DimBroker row count', NULL, case when 
# MAGIC       (select count(*) from DimBroker) = 
# MAGIC       (select Value from Audit where DataSet = 'DimBroker' and Attribute = 'HR_BROKERS') 
# MAGIC then 'OK' else 'Mismatch' end, 'Actual row count matches Audit table'
# MAGIC  
# MAGIC union all
# MAGIC select 'DimBroker distinct keys', NULL, case when 
# MAGIC       (select count(distinct SK_BrokerID) from DimBroker) = 
# MAGIC       (select Value from Audit where DataSet = 'DimBroker' and Attribute = 'HR_BROKERS') 
# MAGIC then 'OK' else 'Not unique' end, 'All SKs are distinct'
# MAGIC 
# MAGIC union all
# MAGIC select 'DimBroker BatchID', 1, case when 
# MAGIC       (select count(*) from DimBroker where BatchID <> 1) = 0 
# MAGIC then 'OK' else 'Not batch 1' end, 'All rows report BatchID = 1'
# MAGIC 
# MAGIC union all
# MAGIC select 'DimBroker IsCurrent', NULL, case when 
# MAGIC       (select count(*) from DimBroker where !IsCurrent) = 0 
# MAGIC then 'OK' else 'Not current' end, 'All rows have IsCurrent = 1'
# MAGIC 
# MAGIC union all
# MAGIC select 'DimBroker EffectiveDate', NULL, case when 
# MAGIC       (select count(*) from DimBroker where EffectiveDate <> '1950-01-01') = 0 
# MAGIC then 'OK' else 'Wrong date' end, 'All rows have Batch1 BatchDate as EffectiveDate'
# MAGIC 
# MAGIC union all
# MAGIC select 'DimBroker EndDate', NULL, case when 
# MAGIC       (select count(*) from DimBroker where EndDate <> '9999-12-31') = 0 
# MAGIC then 'OK' else 'Wrong date' end, 'All rows have end of time as EndDate'
# MAGIC  
# MAGIC --  
# MAGIC -- Checks against the DimAccount table.
# MAGIC --  
# MAGIC union all
# MAGIC select 'DimAccount row count', 1, case when                                   
# MAGIC       (select count(*) from DimAccount where BatchID = 1) >
# MAGIC       (select Value from Audit where DataSet = 'DimCustomer' and Attribute = 'C_NEW' and BatchID = 1) + 
# MAGIC       (select Value from Audit where DataSet = 'DimAccount' and Attribute = 'CA_ADDACCT' and BatchID = 1) +
# MAGIC       (select Value from Audit where DataSet = 'DimAccount' and Attribute = 'CA_CLOSEACCT' and BatchID = 1) +
# MAGIC       (select Value from Audit where DataSet = 'DimAccount' and Attribute = 'CA_UPDACCT' and BatchID = 1) +
# MAGIC       (select Value from Audit where DataSet = 'DimCustomer' and Attribute = 'C_UPDCUST' and BatchID = 1) +
# MAGIC       (select Value from Audit where DataSet = 'DimCustomer' and Attribute = 'C_INACT' and BatchID = 1) -
# MAGIC       (select Value from Audit where DataSet = 'DimCustomer' and Attribute = 'C_ID_HIST' and BatchID = 1) -
# MAGIC       (select Value from Audit where DataSet = 'DimAccount' and Attribute = 'CA_ID_HIST' and BatchID = 1)
# MAGIC then 'OK' else 'Too few rows' end, 'Actual row count matches or exceeds Audit table minimum'
# MAGIC 
# MAGIC -- had to change to sum(value) instead of just value because of correlated subquery issue
# MAGIC union all
# MAGIC select 'DimAccount row count', BatchID, Result, 'Actual row count matches or exceeds Audit table minimum' from (
# MAGIC       select distinct BatchID, (
# MAGIC             case when 
# MAGIC                   (select count(*) from DimAccount where BatchID = a.BatchID) >= 
# MAGIC                   (select sum(Value) from Audit where DataSet = 'DimAccount' and Attribute = 'CA_ADDACCT' and BatchID = a.BatchID) +
# MAGIC                   (select sum(Value) from Audit where DataSet = 'DimAccount' and Attribute = 'CA_CLOSEACCT' and BatchID = a.BatchID) +
# MAGIC                   (select sum(Value) from Audit where DataSet = 'DimAccount' and Attribute = 'CA_UPDACCT' and BatchID = a.BatchID) -
# MAGIC                   (select sum(Value) from Audit where DataSet = 'DimAccount' and Attribute = 'CA_ID_HIST' and BatchID = a.BatchID)
# MAGIC             then 'OK' else 'Too few rows' end
# MAGIC       ) as Result
# MAGIC       from Audit a
# MAGIC       where BatchID in (2, 3)
# MAGIC ) o
# MAGIC 
# MAGIC union all
# MAGIC select 'DimAccount distinct keys', NULL, case when 
# MAGIC       (select count(distinct SK_AccountID) from DimAccount) = 
# MAGIC       (select count(*) from DimAccount) 
# MAGIC then 'OK' else 'Not unique' end, 'All SKs are distinct'
# MAGIC 
# MAGIC -- Three tests together check for validity of the EffectiveDate and EndDate handling:
# MAGIC --   'DimAccount EndDate' checks that effective and end dates line up
# MAGIC --   'DimAccount Overlap' checks that there are not records that overlap in time
# MAGIC --   'DimAccount End of Time' checks that every company has a final record that goes to 9999-12-31
# MAGIC 
# MAGIC union all
# MAGIC select 'DimAccount EndDate', NULL, case when
# MAGIC       (select count(*) from DimAccount) =
# MAGIC       (select count(*) from DimAccount a join DimAccount b on a.AccountID = b.AccountID and a.EndDate = b.EffectiveDate) +
# MAGIC       (select count(*) from DimAccount where EndDate = '9999-12-31')
# MAGIC then 'OK' else 'Dates not aligned' end, 'EndDate of one record matches EffectiveDate of another, or the end of time'
# MAGIC  
# MAGIC union all
# MAGIC select 'DimAccount Overlap', NULL, case when (
# MAGIC       select count(*)
# MAGIC       from DimAccount a 
# MAGIC       join DimAccount b on a.AccountID = b.AccountID and a.SK_AccountID <> b.SK_AccountID and a.EffectiveDate >= b.EffectiveDate and a.EffectiveDate < b.EndDate
# MAGIC ) = 0
# MAGIC then 'OK' else 'Dates overlap' end, 'Date ranges do not overlap for a given Account'
# MAGIC  
# MAGIC union all
# MAGIC select 'DimAccount End of Time', NULL, case when
# MAGIC       (select count(distinct AccountID) from DimAccount) =
# MAGIC       (select count(*) from DimAccount where EndDate = '9999-12-31')
# MAGIC then 'OK' else 'End of tome not reached' end, 'Every Account has one record with a date range reaching the end of time'
# MAGIC 
# MAGIC union all
# MAGIC select 'DimAccount consolidation', NULL, case when 
# MAGIC       (select count(*) from DimAccount where EffectiveDate = EndDate) = 0     
# MAGIC then 'OK' else 'Not consolidated' end, 'No records become effective and end on the same day'
# MAGIC  
# MAGIC union all
# MAGIC select 'DimAccount batches', NULL, case when 
# MAGIC       (select count(distinct BatchID) from DimAccount) = 3  and
# MAGIC       (select max(BatchID) from DimAccount) = 3 
# MAGIC then 'OK' else 'Mismatch' end, 'BatchID values must match Audit table'
# MAGIC 
# MAGIC union all
# MAGIC --This Code wasn't working with such a nested correlated subquery
# MAGIC --select 'DimAccount EffectiveDate', BatchID, Result, 'All records from a batch have an EffectiveDate in the batch time window' from (
# MAGIC --    select distinct BatchID, (
# MAGIC --          case when (
# MAGIC --                select count(*) from DimAccount
# MAGIC --                where BatchID = a.BatchID and (
# MAGIC --                      EffectiveDate < (select Date from Audit where DataSet = 'Batch' and Attribute = 'FirstDay' and BatchID = a.BatchID) or
# MAGIC --                      EffectiveDate > (select Date from Audit where DataSet = 'Batch' and Attribute = 'LastDay' and BatchID = a.BatchID) )
# MAGIC --          ) = 0 
# MAGIC --          then 'OK' else 'Data out of range - see ticket #71' end
# MAGIC --    ) as Result
# MAGIC --    from Audit a where BatchID in (1, 2, 3)
# MAGIC -- ) o
# MAGIC select 'DimAccount EffectiveDate', BatchID, Result, 'All records from a batch have an EffectiveDate in the batch time window' from (
# MAGIC       select distinct BatchID, (
# MAGIC             case when (
# MAGIC                   select count(*) 
# MAGIC                   from (select BatchID, Date, Attribute from Audit where DataSet = 'Batch' and Attribute in ('FirstDay', 'LastDay')) aud
# MAGIC                   join DimAccount da
# MAGIC                         ON 
# MAGIC                               da.batchid = aud.batchid 
# MAGIC                               AND (
# MAGIC                                     (da.EffectiveDate < aud.date AND Attribute = 'FirstDay')
# MAGIC                                     OR (da.EffectiveDate > aud.date AND Attribute = 'LastDay')
# MAGIC                               )
# MAGIC                   where da.batchid = a.batchid AND aud.BatchID = a.BatchID
# MAGIC             ) = 0 
# MAGIC             then 'OK' else 'Data out of range - see ticket #71' end
# MAGIC       ) as Result
# MAGIC       from Audit a where BatchID in (1, 2, 3)
# MAGIC ) o
# MAGIC 
# MAGIC union all
# MAGIC select 'DimAccount IsCurrent', NULL, case when
# MAGIC   (select count(*) from DimAccount) =
# MAGIC   (select count(*) from DimAccount where EndDate = '9999-12-31' and IsCurrent) +
# MAGIC   (select count(*) from DimAccount where EndDate < '9999-12-31' and !IsCurrent)
# MAGIC   then 'OK' 
# MAGIC   else 'Not current' end, 
# MAGIC   'IsCurrent is 1 if EndDate is the end of time, else Iscurrent is 0'
# MAGIC  
# MAGIC 
# MAGIC union all
# MAGIC select 'DimAccount Status', NULL, case when
# MAGIC       (select count(*) from DimAccount where Status not in ('Active', 'Inactive')) = 0
# MAGIC then 'OK' else 'Bad value' end, 'All Status values are valid'
# MAGIC  
# MAGIC 
# MAGIC union all
# MAGIC select 'DimAccount TaxStatus', NULL, case when
# MAGIC       (select count(*) from DimAccount where BatchID = 1 and TaxStatus not in (0, 1, 2)) = 0
# MAGIC then 'OK' else 'Bad value' end, 'All TaxStatus values are valid'
# MAGIC 
# MAGIC 
# MAGIC union all
# MAGIC select 'DimAccount SK_CustomerID', NULL, case when
# MAGIC       (select count(*) from DimAccount) = 
# MAGIC       (select count(*) from DimAccount a
# MAGIC        join DimCustomer c on a.SK_CustomerID = c.SK_CustomerID and c.EffectiveDate <= a.EffectiveDate and a.EndDate <= c.EndDate)
# MAGIC then 'OK' else 'Bad join' end, 'All SK_CustomerIDs match a DimCustomer record with a valid date range'
# MAGIC 
# MAGIC 
# MAGIC union all
# MAGIC select 'DimAccount SK_BrokerID', NULL, case when
# MAGIC       (select count(*) from DimAccount) = 
# MAGIC       (select count(*) from DimAccount a   join DimBroker c on a.SK_BrokerID = c.SK_BrokerID and c.EffectiveDate <= a.EffectiveDate and a.EndDate <= c.EndDate)
# MAGIC then 'OK' else 'Bad join - spec problem with DimBroker EffectiveDate values' end, 'All SK_BrokerIDs match a broker record with a valid date range'
# MAGIC  
# MAGIC 
# MAGIC union all
# MAGIC select 'DimAccount inactive customers', NULL, case when (
# MAGIC        select count(*) from 
# MAGIC        (select count(*) from (select * from DimCustomer where Status='Inactive') c left join DimAccount a on a.SK_CustomerID = c.SK_CustomerID 
# MAGIC           where a.Status = 'Inactive' group by c.SK_CustomerID having count(*) < 1) z
# MAGIC ) = 0
# MAGIC then 'OK' else 'Bad value' end, 'If a customer is inactive, the corresponding accounts must also have been inactive'
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC --  
# MAGIC -- Checks against the DimCustomer table.
# MAGIC --  
# MAGIC --had to add sum for scalar subquery
# MAGIC union all
# MAGIC select 'DimCustomer row count', BatchID, Result, 'Actual row count matches or exceeds Audit table minimum' from (
# MAGIC       select distinct BatchID, case when
# MAGIC             (select count(*) from DimCustomer where BatchID = a.BatchID) >=
# MAGIC             (select sum(Value) from Audit where DataSet = 'DimCustomer' and Attribute = 'C_NEW' and BatchID = a.BatchID) + 
# MAGIC             (select sum(Value) from Audit where DataSet = 'DimCustomer' and Attribute = 'C_INACT' and BatchID = a.BatchID) +
# MAGIC             (select sum(Value) from Audit where DataSet = 'DimCustomer' and Attribute = 'C_UPDCUST' and BatchID = a.BatchID) -
# MAGIC             (select sum(Value) from Audit where DataSet = 'DimCustomer' and Attribute = 'C_ID_HIST' and BatchID = a.BatchID)
# MAGIC       then 'OK' else 'Too few rows' end as Result
# MAGIC       from Audit a where BatchID in (1, 2, 3)
# MAGIC ) o
# MAGIC 
# MAGIC union all
# MAGIC select 'DimCustomer distinct keys', NULL, case when 
# MAGIC       (select count(distinct SK_CustomerID) from DimCustomer) = 
# MAGIC       (select count(*) from DimCustomer) 
# MAGIC then 'OK' else 'Not unique' end, 'All SKs are distinct'
# MAGIC  
# MAGIC 
# MAGIC 
# MAGIC -- Three tests together check for validity of the EffectiveDate and EndDate handling:
# MAGIC --   'DimCustomer EndDate' checks that effective and end dates line up
# MAGIC --   'DimCustomer Overlap' checks that there are not records that overlap in time
# MAGIC --   'DimCustomer End of Time' checks that every company has a final record that goes to 9999-12-31
# MAGIC 
# MAGIC union all
# MAGIC select 'DimCustomer EndDate', NULL, case when
# MAGIC       (select count(*) from DimCustomer) =
# MAGIC       (select count(*) from DimCustomer a join DimCustomer b on a.CustomerID = b.CustomerID and a.EndDate = b.EffectiveDate) +
# MAGIC       (select count(*) from DimCustomer where EndDate = '9999-12-31')
# MAGIC then 'OK' else 'Dates not aligned' end, 'EndDate of one record matches EffectiveDate of another, or the end of time'
# MAGIC  
# MAGIC 
# MAGIC union all
# MAGIC select 'DimCustomer Overlap', NULL, case when (
# MAGIC       select count(*)
# MAGIC       from DimCustomer a 
# MAGIC       join DimCustomer b on a.CustomerID = b.CustomerID and a.SK_CustomerID <> b.SK_CustomerID and a.EffectiveDate >= b.EffectiveDate and a.EffectiveDate < b.EndDate
# MAGIC ) = 0
# MAGIC then 'OK' else 'Dates overlap' end, 'Date ranges do not overlap for a given Customer'
# MAGIC  
# MAGIC 
# MAGIC union all
# MAGIC select 'DimCustomer End of Time', NULL, case when
# MAGIC       (select count(distinct CustomerID) from DimCustomer) =
# MAGIC       (select count(*) from DimCustomer where EndDate = '9999-12-31')
# MAGIC then 'OK' else 'End of time not reached' end, 'Every Customer has one record with a date range reaching the end of time'
# MAGIC  
# MAGIC 
# MAGIC union all
# MAGIC select 'DimCustomer consolidation', NULL, case when 
# MAGIC       (select count(*) from DimCustomer where EffectiveDate = EndDate) = 0    
# MAGIC then 'OK' else 'Not consolidated' end, 'No records become effective and end on the same day'
# MAGIC  
# MAGIC 
# MAGIC union all
# MAGIC select 'DimCustomer batches', NULL, case when 
# MAGIC       (select count(distinct BatchID) from DimCustomer) = 3 and
# MAGIC       (select max(BatchID) from DimCustomer) = 3
# MAGIC then 'OK' else 'Mismatch' end, 'BatchID values must match Audit table'
# MAGIC  
# MAGIC 
# MAGIC union all
# MAGIC select 'DimCustomer IsCurrent', NULL, case when 
# MAGIC       (select count(*) from DimCustomer) =
# MAGIC       (select count(*) from DimCustomer where EndDate = '9999-12-31' and IsCurrent) +
# MAGIC       (select count(*) from DimCustomer where EndDate < '9999-12-31' and !IsCurrent)
# MAGIC then 'OK' else 'Not current' end, 'IsCurrent is 1 if EndDate is the end of time, else Iscurrent is 0'
# MAGIC  
# MAGIC 
# MAGIC union all
# MAGIC --This Code wasn't working with such a nested correlated subquery and/or was returning more than one record in case statement.  Had to get it grouped together and counted in total
# MAGIC -- select 'DimCustomer EffectiveDate', BatchID, Result, 'All records from a batch have an EffectiveDate in the batch time window' from (
# MAGIC --       select distinct BatchID, (
# MAGIC --             case when (
# MAGIC --                   select count(*) from DimCustomer
# MAGIC --                   where BatchID = a.BatchID and (
# MAGIC --                         EffectiveDate < (select Date from Audit a where DataSet = 'Batch' and Attribute = 'FirstDay' and BatchID = a.BatchID) or
# MAGIC --                         EffectiveDate > (select Date from Audit a where DataSet = 'Batch' and Attribute = 'LastDay' and BatchID = a.BatchID) )
# MAGIC --             ) = 0 
# MAGIC --             then 'OK' else 'Data out of range' end
# MAGIC --       ) as Result
# MAGIC --       from Audit a where BatchID in (1, 2, 3)
# MAGIC -- ) o
# MAGIC select 'DimCustomer EffectiveDate', BatchID, Result, 'All records from a batch have an EffectiveDate in the batch time window' from (
# MAGIC       select distinct BatchID, (
# MAGIC             case when (
# MAGIC                   select count(*) 
# MAGIC                   from (select BatchID, Date, Attribute from Audit where DataSet = 'Batch' and Attribute in ('FirstDay', 'LastDay')) aud
# MAGIC                   join DimCustomer dc
# MAGIC                         ON 
# MAGIC                               dc.batchid = aud.batchid 
# MAGIC                               AND (
# MAGIC                                     (dc.EffectiveDate < aud.date AND Attribute = 'FirstDay')
# MAGIC                                     OR (dc.EffectiveDate > aud.date AND Attribute = 'LastDay'))
# MAGIC                   where dc.batchid = a.batchid AND aud.BatchID = a.BatchID
# MAGIC             ) = 0 then 'OK' else 'Data out of range' end) as Result
# MAGIC       from Audit a where BatchID in (1, 2, 3)
# MAGIC ) o
# MAGIC 
# MAGIC union all
# MAGIC select 'DimCustomer Status', NULL, case when
# MAGIC       (select count(*) from DimCustomer where Status not in ('Active', 'Inactive')) = 0
# MAGIC then 'OK' else 'Bad value' end, 'All Status values are valid'
# MAGIC  
# MAGIC union all
# MAGIC -- HEAVILY modified this one since correlated subqueries 1) Have to be an aggregate and 2) Cannot use a non-equijoin
# MAGIC -- select 'DimCustomer inactive customers', BatchID, Result, 'Inactive customer count matches Audit table' from (
# MAGIC --       select distinct BatchID, case when
# MAGIC --             (select messageData from DImessages where MessageType = 'Validation' and BatchID = a.BatchID and 'DimCustomer' = MessageSource and 'Inactive customers' = MessageText) = 
# MAGIC --             (select sum(Value) from Audit where DataSet = 'DimCustomer' and BatchID <= a.BatchID and Attribute = 'C_INACT')         
# MAGIC --       then 'OK' else 'Mismatch' end as Result
# MAGIC --       from Audit a where BatchID in (1, 2, 3)
# MAGIC -- ) o
# MAGIC select 'DimCustomer inactive customers', BatchID, Result, 'Inactive customer count matches Audit table' from (
# MAGIC       select distinct BatchID, case when
# MAGIC             (
# MAGIC                   select sum(MessageData) from DImessages where MessageSource = 'DimCustomer' and MessageType = 'Validation' and MessageText = 'Inactive customers' and BatchID = a.BatchID) =
# MAGIC             (
# MAGIC                   select sum(audit_batch_total)
# MAGIC                   from (
# MAGIC                         select batchid, sum(sum(Value)) over (order by batchid) as audit_batch_total 
# MAGIC                         from Audit 
# MAGIC                         where BatchID in (1, 2, 3) and DataSet = 'DimCustomer' and Attribute = 'C_INACT' 
# MAGIC                         group by batchid)
# MAGIC                   where BatchID = a.BatchID)
# MAGIC       then 'OK' else 'Mismatch' end as Result
# MAGIC       from Audit a where BatchID in (1, 2, 3)
# MAGIC ) o
# MAGIC 
# MAGIC union all
# MAGIC select 'DimCustomer Gender', NULL, case when
# MAGIC       (select count(*) from DimCustomer where Gender not in ('M', 'F', 'U')) = 0
# MAGIC then 'OK' else 'Bad value' end, 'All Gender values are valid'
# MAGIC  
# MAGIC 
# MAGIC union all
# MAGIC --adding sum for value
# MAGIC select 'DimCustomer age range alerts', BatchID, Result, 'Count of age range alerts matches audit table' from (
# MAGIC       select distinct BatchID, case when
# MAGIC             (select count(*) from DImessages where MessageType = 'Alert' and BatchID = a.BatchID and MessageText = 'DOB out of range') = 
# MAGIC             (select sum(Value) from Audit where DataSet = 'DimCustomer' and BatchID = a.BatchID and Attribute = 'C_DOB_TO') +
# MAGIC             (select sum(Value) from Audit where DataSet = 'DimCustomer' and BatchID = a.BatchID and Attribute = 'C_DOB_TY')
# MAGIC       then 'OK' else 'Mismatch' end as Result
# MAGIC       from Audit a where BatchID in (1, 2, 3)
# MAGIC ) o
# MAGIC 
# MAGIC union all
# MAGIC --adding sum for value
# MAGIC select 'DimCustomer customer tier alerts', BatchID, Result, 'Count of customer tier alerts matches audit table' from (
# MAGIC       select distinct BatchID, case when
# MAGIC             (select count(*) from DImessages where MessageType = 'Alert' and BatchID = a.BatchID and MessageText = 'Invalid customer tier') = 
# MAGIC             (select sum(Value) from Audit where DataSet = 'DimCustomer' and BatchID = a.BatchID and Attribute = 'C_TIER_INV')
# MAGIC       then 'OK' else 'Mismatch' end as Result
# MAGIC       from Audit a where BatchID in (1, 2, 3)
# MAGIC ) o
# MAGIC 
# MAGIC union all                     
# MAGIC select 'DimCustomer TaxID', NULL, case when (
# MAGIC       select count(*) from DimCustomer where TaxID not like '___-__-____'
# MAGIC       ) = 0
# MAGIC then 'OK' else 'Mismatch' end, 'TaxID values are properly formatted'
# MAGIC 
# MAGIC union all                       
# MAGIC select 'DimCustomer Phone1', NULL, case when (
# MAGIC       select count(*) from DimCustomer 
# MAGIC       where Phone1 not like '+1 (___) ___-____%'
# MAGIC         and Phone1 not like '(___) ___-____%'
# MAGIC         and Phone1 not like '___-____%'
# MAGIC         and Phone1 <> ''
# MAGIC         and Phone1 is not null
# MAGIC       ) = 0
# MAGIC then 'OK' else 'Mismatch' end, 'Phone1 values are properly formatted'
# MAGIC  
# MAGIC 
# MAGIC union all                       
# MAGIC select 'DimCustomer Phone2', NULL, case when (
# MAGIC       select count(*) from DimCustomer 
# MAGIC       where Phone2 not like '+1 (___) ___-____%'
# MAGIC         and Phone2 not like '(___) ___-____%'
# MAGIC         and Phone2 not like '___-____%'
# MAGIC         and Phone2 <> ''
# MAGIC         and Phone2 is not null
# MAGIC       ) = 0
# MAGIC then 'OK' else 'Mismatch' end, 'Phone2 values are properly formatted'
# MAGIC  
# MAGIC 
# MAGIC union all                       
# MAGIC select 'DimCustomer Phone3', NULL, case when (
# MAGIC       select count(*) from DimCustomer 
# MAGIC       where Phone3 not like '+1 (___) ___-____%'
# MAGIC         and Phone3 not like '(___) ___-____%'
# MAGIC         and Phone3 not like '___-____%'
# MAGIC         and Phone3 <> ''
# MAGIC         and Phone3 is not null
# MAGIC       ) = 0
# MAGIC then 'OK' else 'Mismatch' end, 'Phone3 values are properly formatted'
# MAGIC  
# MAGIC 
# MAGIC union all                     
# MAGIC select 'DimCustomer Email1', NULL, case when (
# MAGIC       select count(*) from DimCustomer 
# MAGIC       where Email1 not like '_%.%@%.%'
# MAGIC         and Email1 is not null
# MAGIC       ) = 0
# MAGIC then 'OK' else 'Mismatch' end, 'Email1 values are properly formatted'
# MAGIC  
# MAGIC 
# MAGIC union all                    
# MAGIC select 'DimCustomer Email2', NULL, case when (
# MAGIC       select count(*) from DimCustomer 
# MAGIC       where Email2 not like '_%.%@%.%'
# MAGIC         and Email2 <> ''
# MAGIC         and Email2 is not null
# MAGIC       ) = 0
# MAGIC then 'OK' else 'Mismatch' end, 'Email2 values are properly formatted'
# MAGIC  
# MAGIC 
# MAGIC 
# MAGIC union all                    
# MAGIC select 'DimCustomer LocalTaxRate', NULL, case when 
# MAGIC       (select count(*) from DimCustomer) = 
# MAGIC       (select count(*) from DimCustomer c join TaxRate t on c.LocalTaxRateDesc = t.TX_NAME and c.LocalTaxRate = t.TX_RATE) and
# MAGIC       (select count(distinct LocalTaxRateDesc) from DimCustomer) > 300
# MAGIC then 'OK' else 'Mismatch' end, 'LocalTaxRateDesc and LocalTaxRate values are from TaxRate table'
# MAGIC  
# MAGIC 
# MAGIC union all                 
# MAGIC select 'DimCustomer NationalTaxRate', NULL, case when 
# MAGIC       (select count(*) from DimCustomer) = 
# MAGIC       (select count(*) from DimCustomer c join TaxRate t on c.NationalTaxRateDesc = t.TX_NAME and c.NationalTaxRate = t.TX_RATE) and
# MAGIC       (select count(distinct NationalTaxRateDesc) from DimCustomer) >= 9   -- Including the inequality for now, because the generated data is not sticking to national tax rates
# MAGIC then 'OK' else 'Mismatch' end, 'NationalTaxRateDesc and NationalTaxRate values are from TaxRate table'
# MAGIC  
# MAGIC 
# MAGIC union all                                
# MAGIC select 'DimCustomer demographic fields', NULL, case when
# MAGIC       (
# MAGIC             select count(*) from DimCustomer c 
# MAGIC             join Prospect p on upper(c.FirstName || c.LastName || c.AddressLine1 || COALESCE(c.AddressLine2,'') || c.PostalCode)
# MAGIC                                      = upper(p.FirstName || p.LastName || p.AddressLine1 || COALESCE(p.AddressLine2,'') || p.PostalCode)
# MAGIC                           and COALESCE(c.CreditRating,0) = COALESCE(p.CreditRating,0) and COALESCE(c.NetWorth,0) = COALESCE(p.NetWorth,0) and COALESCE(c.MarketingNameplate, '') = COALESCE(p.MarketingNameplate,'')
# MAGIC                                   and c.IsCurrent
# MAGIC       ) = (
# MAGIC             select count(*) from DimCustomer where AgencyID is not null and IsCurrent
# MAGIC       )
# MAGIC then 'OK' else 'Mismatch' end, 'For current customer records that match Prospect records, the demographic fields also match'
# MAGIC  
# MAGIC 
# MAGIC --  
# MAGIC -- Checks against the DimSecurity table.
# MAGIC --  
# MAGIC 
# MAGIC union all
# MAGIC -- Modified this one since correlated subqueries 1) Have to be an aggregate and 2) Cannot use a non-equijoin
# MAGIC -- select 'DimSecurity row count', BatchID, Result, 'Actual row count matches or exceeds Audit table minimum' from (
# MAGIC --       select distinct BatchID, case when
# MAGIC --             cast((select MessageData from DImessages where MessageType = 'Validation' and BatchID = a.BatchID and MessageSource = 'DimSecurity' and MessageText = 'Row count') as bigint) >= 
# MAGIC --             (select sum(Value) from Audit where DataSet = 'DimSecurity' and Attribute = 'FW_SEC' and BatchID <= a.BatchID)
# MAGIC --       then 'OK' else 'Too few rows' end as Result
# MAGIC --       from Audit a where BatchID in (1)
# MAGIC -- ) o
# MAGIC select 'DimSecurity row count', BatchID, Result, 'Actual row count matches or exceeds Audit table minimum' from (
# MAGIC   select distinct BatchID, case when
# MAGIC     cast((select sum(MessageData) from DImessages where MessageType = 'Validation' and MessageSource = 'DimSecurity' and MessageText = 'Row count' and BatchID = a.BatchID) as bigint) >= 
# MAGIC     (select sum(Value) from Audit where DataSet = 'DimSecurity' and Attribute = 'FW_SEC' and BatchID = a.BatchID) -- This one was simpler since BatchID in (1) means only one batchid to compare to
# MAGIC     then 'OK' else 'Too few rows' end as Result
# MAGIC   from Audit a where BatchID in (1)
# MAGIC ) o
# MAGIC 
# MAGIC union all
# MAGIC select 'DimSecurity distinct keys', NULL, case when 
# MAGIC       (select count(distinct SK_SecurityID) from DimSecurity) = 
# MAGIC       (select count(*) from DimSecurity) 
# MAGIC then 'OK' else 'Not unique' end, 'All SKs are distinct'
# MAGIC 
# MAGIC 
# MAGIC -- Three tests together check for validity of the EffectiveDate and EndDate handling:
# MAGIC --   'DimSecurity EndDate' checks that effective and end dates line up
# MAGIC --   'DimSecurity Overlap' checks that there are not records that overlap in time
# MAGIC --   'DimSecurity End of Time' checks that every company has a final record that goes to 9999-12-31
# MAGIC 
# MAGIC union all
# MAGIC select 'DimSecurity EndDate', NULL, case when
# MAGIC       (select count(*) from DimSecurity) =
# MAGIC       (select count(*) from DimSecurity a join DimSecurity b on a.Symbol = b.Symbol and a.EndDate = b.EffectiveDate) +
# MAGIC       (select count(*) from DimSecurity where EndDate = '9999-12-31')
# MAGIC then 'OK' else 'Dates not aligned' end, 'EndDate of one record matches EffectiveDate of another, or the end of time'
# MAGIC  
# MAGIC 
# MAGIC union all
# MAGIC select 'DimSecurity Overlap', NULL, case when (
# MAGIC       select count(*)
# MAGIC       from DimSecurity a 
# MAGIC       join DimSecurity b on a.Symbol = b.Symbol and a.SK_SecurityID <> b.SK_SecurityID and a.EffectiveDate >= b.EffectiveDate and a.EffectiveDate < b.EndDate
# MAGIC ) = 0
# MAGIC then 'OK' else 'Dates overlap' end, 'Date ranges do not overlap for a given company'
# MAGIC  
# MAGIC 
# MAGIC union all
# MAGIC select 'DimSecurity End of Time', NULL, case when
# MAGIC       (select count(distinct Symbol) from DimSecurity) =
# MAGIC       (select count(*) from DimSecurity where EndDate = '9999-12-31')
# MAGIC then 'OK' else 'End of tome not reached' end, 'Every company has one record with a date range reaching the end of time'
# MAGIC  
# MAGIC 
# MAGIC union all
# MAGIC select 'DimSecurity consolidation', NULL, case when 
# MAGIC       (select count(*) from DimSecurity where EffectiveDate = EndDate) = 0    
# MAGIC then 'OK' else 'Not consolidated' end, 'No records become effective and end on the same day'
# MAGIC  
# MAGIC 
# MAGIC union all
# MAGIC select 'DimSecurity batches', NULL, case when 
# MAGIC       (select count(distinct BatchID) from DimSecurity) = 1 and
# MAGIC       (select max(BatchID) from DimSecurity) = 1 
# MAGIC then 'OK' else 'Mismatch' end, 'BatchID values must match Audit table'
# MAGIC  
# MAGIC 
# MAGIC union all
# MAGIC select 'DimSecurity IsCurrent', NULL, case when 
# MAGIC       (select count(*) from DimSecurity) =
# MAGIC       (select count(*) from DimSecurity where EndDate = '9999-12-31' and IsCurrent) +
# MAGIC       (select count(*) from DimSecurity where EndDate < '9999-12-31' and !IsCurrent)
# MAGIC then 'OK' else 'Not current' end, 'IsCurrent is 1 if EndDate is the end of time, else Iscurrent is 0'
# MAGIC  
# MAGIC 
# MAGIC union all
# MAGIC --This Code wasn't working with such a nested correlated subquery and/or was returning more than one record in case statement.  Had to get it grouped together and counted in total
# MAGIC -- select 'DimSecurity EffectiveDate', BatchID, Result, 'All records from a batch have an EffectiveDate in the batch time window' from (
# MAGIC --       select distinct BatchID, (
# MAGIC --             case when (
# MAGIC --                   select count(*) from DimSecurity
# MAGIC --                   where BatchID = a.BatchID and (
# MAGIC --                 --added alias for audit and filter the same
# MAGIC --                         EffectiveDate < (select Date from Audit a where DataSet = 'Batch' and Attribute = 'FirstDay' and BatchID = a.BatchID) or
# MAGIC --                         EffectiveDate > (select Date from Audit a where DataSet = 'Batch' and Attribute = 'LastDay' and BatchID = a.BatchID))
# MAGIC --             ) = 0 
# MAGIC --             then 'OK' else 'Data out of range' end
# MAGIC --       ) as Result
# MAGIC --       from Audit a where BatchID in (1, 2, 3)
# MAGIC -- ) o
# MAGIC select 'DimSecurity EffectiveDate', BatchID, Result, 'All records from a batch have an EffectiveDate in the batch time window' from (
# MAGIC       select distinct BatchID, (
# MAGIC             case when (
# MAGIC                   select count(*) 
# MAGIC                   from (select BatchID, Date, Attribute from Audit where DataSet = 'Batch' and Attribute in ('FirstDay', 'LastDay')) aud
# MAGIC                   join DimSecurity dc
# MAGIC                         ON 
# MAGIC                               dc.batchid = aud.batchid 
# MAGIC                               AND (
# MAGIC                                     (dc.EffectiveDate < aud.date AND Attribute = 'FirstDay')
# MAGIC                                     OR (dc.EffectiveDate > aud.date AND Attribute = 'LastDay'))
# MAGIC                   where dc.batchid = a.batchid AND aud.BatchID = a.BatchID
# MAGIC             ) = 0 then 'OK' else 'Data out of range' end) as Result
# MAGIC       from Audit a where BatchID in (1, 2, 3)
# MAGIC ) o
# MAGIC 
# MAGIC 
# MAGIC union all
# MAGIC select 'DimSecurity Status', NULL, case when
# MAGIC       (select count(*) from DimSecurity where Status not in ('Active', 'Inactive')) = 0
# MAGIC then 'OK' else 'Bad value' end, 'All Status values are valid'
# MAGIC  
# MAGIC 
# MAGIC union all
# MAGIC select 'DimSecurity SK_CompanyID', NULL, case when
# MAGIC       (select count(*) from DimSecurity) = 
# MAGIC       (select count(*) from DimSecurity a
# MAGIC        join DimCompany c on a.SK_CompanyID = c.SK_CompanyID and c.EffectiveDate <= a.EffectiveDate and a.EndDate <= c.EndDate)
# MAGIC then 'OK' else 'Bad join' end, 'All SK_CompanyIDs match a DimCompany record with a valid date range'
# MAGIC  
# MAGIC 
# MAGIC union all
# MAGIC select 'DimSecurity ExchangeID', NULL, case when
# MAGIC       (select count(*) from DimSecurity where ExchangeID not in ('NYSE', 'NASDAQ', 'AMEX', 'PCX')) = 0
# MAGIC then 'OK' else 'Bad value - see ticket #65' end, 'All ExchangeID values are valid'
# MAGIC  
# MAGIC 
# MAGIC union all
# MAGIC select 'DimSecurity Issue', NULL, case when
# MAGIC       (select count(*) from DimSecurity where Issue not in ('COMMON', 'PREF_A', 'PREF_B', 'PREF_C', 'PREF_D')) = 0
# MAGIC then 'OK' else 'Bad value - see ticket #65' end, 'All Issue values are valid'
# MAGIC  
# MAGIC 
# MAGIC --  
# MAGIC -- Checks against the DimCompany table.
# MAGIC --  
# MAGIC union all
# MAGIC select 'DimCompany row count', BatchID, Result, 'Actual row count matches or exceeds Audit table minimum' from (
# MAGIC       select distinct BatchID, case when
# MAGIC     --added sum(messagedata)
# MAGIC             cast((select sum(MessageData) from DImessages where MessageType='Validation' and BatchID = a.BatchID and MessageSource = 'DimCompany' and MessageText = 'Row count') as bigint) <=
# MAGIC         --added audit alias and filter
# MAGIC             (select sum(Value) from Audit a where DataSet = 'DimCompany' and Attribute = 'FW_CMP' and BatchID <= a.BatchID)
# MAGIC       then 'OK' else 'Too few rows' end as Result
# MAGIC       from Audit a where BatchID in (1, 2, 3)
# MAGIC ) o
# MAGIC 
# MAGIC  
# MAGIC union all
# MAGIC select 'DimCompany distinct keys', NULL, case when 
# MAGIC       (select count(distinct SK_CompanyID) from DimCompany) = 
# MAGIC       (select count(*) from DimCompany) 
# MAGIC then 'OK' else 'Not unique' end, 'All SKs are distinct'
# MAGIC  
# MAGIC 
# MAGIC 
# MAGIC -- Three tests together check for validity of the EffectiveDate and EndDate handling:
# MAGIC --   'DimCompany EndDate' checks that effective and end dates line up
# MAGIC --   'DimCompany Overlap' checks that there are not records that overlap in time
# MAGIC --   'DimCompany End of Time' checks that every company has a final record that goes to 9999-12-31
# MAGIC 
# MAGIC union all
# MAGIC select 'DimCompany EndDate', NULL, case when
# MAGIC       (select count(*) from DimCompany) =
# MAGIC       (select count(*) from DimCompany a join DimCompany b on a.CompanyID = b.CompanyID and a.EndDate = b.EffectiveDate) +
# MAGIC       (select count(*) from DimCompany where EndDate = '9999-12-31')
# MAGIC then 'OK' else 'Dates not aligned' end, 'EndDate of one record matches EffectiveDate of another, or the end of time'
# MAGIC  
# MAGIC 
# MAGIC union all
# MAGIC select 'DimCompany Overlap', NULL, case when (
# MAGIC       select count(*)
# MAGIC       from DimCompany a 
# MAGIC       join DimCompany b on a.CompanyID = b.CompanyID and a.SK_CompanyID <> b.SK_CompanyID and a.EffectiveDate >= b.EffectiveDate and a.EffectiveDate < b.EndDate
# MAGIC ) = 0
# MAGIC then 'OK' else 'Dates overlap' end, 'Date ranges do not overlap for a given company'
# MAGIC  
# MAGIC 
# MAGIC union all
# MAGIC select 'DimCompany End of Time', NULL, case when
# MAGIC       (select count(distinct CompanyID) from DimCompany) =
# MAGIC       (select count(*) from DimCompany where EndDate = '9999-12-31')
# MAGIC then 'OK' else 'End of tome not reached' end, 'Every company has one record with a date range reaching the end of time'
# MAGIC  
# MAGIC 
# MAGIC union all
# MAGIC select 'DimCompany consolidation', NULL, case when 
# MAGIC       (select count(*) from DimCompany where EffectiveDate = EndDate) = 0     
# MAGIC then 'OK' else 'Not consolidated' end, 'No records become effective and end on the same day'
# MAGIC 
# MAGIC 
# MAGIC union all
# MAGIC select 'DimCompany batches', NULL, case when 
# MAGIC       (select count(distinct BatchID) from DimCompany) = 1 and
# MAGIC       (select max(BatchID) from DimCompany) = 1 
# MAGIC then 'OK' else 'Mismatch' end, 'BatchID values must match Audit table'
# MAGIC 
# MAGIC 
# MAGIC union all
# MAGIC --This Code wasn't working with such a nested correlated subquery and/or was returning more than one record in case statement.  Had to get it grouped together and counted in total
# MAGIC -- select 'DimCompany EffectiveDate', BatchID, Result, 'All records from a batch have an EffectiveDate in the batch time window' from (
# MAGIC --       select distinct BatchID, (
# MAGIC --             case when (
# MAGIC --                   select count(*) from DimCompany
# MAGIC --                   where BatchID = a.BatchID and (
# MAGIC --             --added audit alias 
# MAGIC --                         EffectiveDate < (select Date from Audit a where DataSet = 'Batch' and Attribute = 'FirstDay' and BatchID = a.BatchID) or
# MAGIC --                         EffectiveDate > (select Date from Audit a where DataSet = 'Batch' and Attribute = 'LastDay' and BatchID = a.BatchID))
# MAGIC --             ) = 0 
# MAGIC --             then 'OK' else 'Data out of range - see ticket #71' end
# MAGIC --       ) as Result
# MAGIC --       from Audit a where BatchID in (1, 2, 3)
# MAGIC -- ) o
# MAGIC select 'DimCompany EffectiveDate', BatchID, Result, 'All records from a batch have an EffectiveDate in the batch time window' from (
# MAGIC       select distinct BatchID, (
# MAGIC             case when (
# MAGIC                   select count(*) 
# MAGIC                   from (select BatchID, Date, Attribute from Audit where DataSet = 'Batch' and Attribute in ('FirstDay', 'LastDay')) aud
# MAGIC                   join DimCompany dc
# MAGIC                         ON 
# MAGIC                               dc.batchid = aud.batchid 
# MAGIC                               AND (
# MAGIC                                     (dc.EffectiveDate < aud.date AND Attribute = 'FirstDay')
# MAGIC                                     OR (dc.EffectiveDate > aud.date AND Attribute = 'LastDay'))
# MAGIC                   where dc.batchid = a.batchid AND aud.BatchID = a.BatchID
# MAGIC             ) = 0 then 'OK' else 'Data out of range' end) as Result
# MAGIC       from Audit a where BatchID in (1, 2, 3)
# MAGIC ) o
# MAGIC 
# MAGIC union all
# MAGIC select 'DimCompany Status', NULL, case when
# MAGIC       (select count(*) from DimCompany where Status not in ('Active', 'Inactive')) = 0
# MAGIC then 'OK' else 'Bad value' end, 'All Status values are valid'
# MAGIC 
# MAGIC 
# MAGIC union all
# MAGIC select 'DimCompany distinct names', NULL, case when   (
# MAGIC       select count(*)
# MAGIC       from DimCompany a
# MAGIC       join DimCompany b on a.Name = b.Name and a.CompanyID <> b.CompanyID
# MAGIC ) = 0 
# MAGIC then 'OK' else 'Mismatch' end, 'Every company has a unique name'
# MAGIC 
# MAGIC 
# MAGIC union all                     -- Curious, there are duplicate industry names in Industry table.  Should there be?  That's why the distinct stuff...
# MAGIC select 'DimCompany Industry', NULL, case when
# MAGIC       (select count(*) from DimCompany) = 
# MAGIC       (select count(*) from DimCompany where Industry in (select distinct IN_NAME from Industry))
# MAGIC then 'OK' else 'Bad value' end, 'Industry values are from the Industry table'
# MAGIC 
# MAGIC 
# MAGIC union all
# MAGIC select 'DimCompany SPrating', NULL, case when (
# MAGIC       select count(*) from DimCompany 
# MAGIC       where SPrating not in ( 'AAA','AA','A','BBB','BB','B','CCC','CC','C','D','AA+','A+','BBB+','BB+','B+','CCC+','AA-','A-','BBB-','BB-','B-','CCC-' )
# MAGIC         and SPrating is not null
# MAGIC ) = 0
# MAGIC then 'OK' else 'Bad value' end, 'All SPrating values are valid'
# MAGIC 
# MAGIC 
# MAGIC union all               -- Right now we have blank (but not null) country names.  Should there be?
# MAGIC select 'DimCompany Country', NULL, case when (
# MAGIC       select count(*) from DimCompany 
# MAGIC       where Country not in ( 'Canada', 'United States of America', '' )
# MAGIC         and Country is not null
# MAGIC ) = 0
# MAGIC then 'OK' else 'Bad value' end, 'All Country values are valid'
# MAGIC 
# MAGIC 
# MAGIC --  
# MAGIC -- Checks against the Prospect table.
# MAGIC --  
# MAGIC union all
# MAGIC select 'Prospect SK_UpdateDateID', NULL, case when 
# MAGIC       (select count(*) from Prospect where SK_RecordDateID < SK_UpdateDateID) = 0   
# MAGIC then 'OK' else 'Mismatch' end, 'SK_RecordDateID must be newer or same as SK_UpdateDateID'
# MAGIC 
# MAGIC 
# MAGIC union all
# MAGIC select 'Prospect SK_RecordDateID', BatchID, Result, 'All records from batch have SK_RecordDateID in or after the batch time window' from (
# MAGIC       select distinct BatchID, (
# MAGIC             case when (
# MAGIC                   select count(*) from Prospect p
# MAGIC             join DimDate on SK_DateId=P.SK_RecordDateID
# MAGIC             join Audit _A on DataSet='Batch' and Attribute='FirstDay' and DateValue < Date and P.BatchID=_A.BatchID
# MAGIC --                where p.BatchID = a.BatchID and (
# MAGIC --                      (select DateValue from DimDate where SK_DateID = p.SK_RecordDateID) < 
# MAGIC --                      (select Date from Audit where DataSet = 'Batch' and Attribute = 'FirstDay' and BatchID = a.BatchID)
# MAGIC --                )
# MAGIC             ) = 0
# MAGIC             then 'OK' else 'Mismatch' end
# MAGIC       ) as Result
# MAGIC       from Audit a where BatchID in (1, 2, 3)
# MAGIC ) o
# MAGIC  
# MAGIC union all
# MAGIC select 'Prospect batches', NULL, case when 
# MAGIC       (select count(distinct BatchID) from Prospect) = 3 and
# MAGIC       (select max(BatchID) from Prospect) = 3 
# MAGIC then 'OK' else 'Mismatch' end, 'BatchID values must match Audit table'
# MAGIC  
# MAGIC 
# MAGIC  union all
# MAGIC select 'Prospect Country', NULL, case when (
# MAGIC       select count(*) from Prospect 
# MAGIC       where Country not in ( 'Canada', 'United States of America' ) -- For the tiny sample data it would be ( 'CANADA', 'USA' )
# MAGIC         and Country is not null
# MAGIC ) = 0
# MAGIC then 'OK' else 'Bad value' end, 'All Country values are valid'
# MAGIC  
# MAGIC 
# MAGIC union all                                                                                             
# MAGIC select 'Prospect MarketingNameplate', NULL, case when (
# MAGIC       select sum(case when (COALESCE(NetWorth,0) > 1000000 or COALESCE(Income,0) > 200000) and MarketingNameplate not like '%HighValue%' then 1 else 0 end) +
# MAGIC              sum(case when (COALESCE(NumberChildren,0) > 3 or COALESCE(NumberCreditCards,0) > 5) and MarketingNameplate not like '%Expenses%' then 1 else 0 end) +
# MAGIC              sum(case when (COALESCE(Age,0) > 45) and MarketingNameplate not like '%Boomer%' then 1 else 0 end) +
# MAGIC              sum(case when (COALESCE(Income,50000) < 50000 or COALESCE(CreditRating,600) < 600 or COALESCE(NetWorth,100000) < 100000) and MarketingNameplate not like '%MoneyAlert%' then 1 else 0 end) +
# MAGIC              sum(case when (COALESCE(NumberCars,0) > 3 or COALESCE(NumberCreditCards,0) > 7) and MarketingNameplate not like '%Spender%' then 1 else 0 end) +
# MAGIC              sum(case when (COALESCE(Age,25) < 25 and COALESCE(NetWorth,0) > 1000000) and MarketingNameplate not like '%Inherited%' then 1 else 0 end) +
# MAGIC                sum(case when COALESCE(MarketingNameplate, '') not in (   -- Technically, a few of these combinations cannot really happen
# MAGIC                         '','HighValue','Expenses','HighValue+Expenses','Boomer','HighValue+Boomer','Expenses+Boomer','HighValue+Expenses+Boomer','MoneyAlert','HighValue+MoneyAlert',
# MAGIC                         'Expenses+MoneyAlert','HighValue+Expenses+MoneyAlert','Boomer+MoneyAlert','HighValue+Boomer+MoneyAlert','Expenses+Boomer+MoneyAlert','HighValue+Expenses+Boomer+MoneyAlert',
# MAGIC                         'Spender','HighValue+Spender','Expenses+Spender','HighValue+Expenses+Spender','Boomer+Spender','HighValue+Boomer+Spender','Expenses+Boomer+Spender',
# MAGIC                         'HighValue+Expenses+Boomer+Spender','MoneyAlert+Spender','HighValue+MoneyAlert+Spender','Expenses+MoneyAlert+Spender','HighValue+Expenses+MoneyAlert+Spender',
# MAGIC                         'Boomer+MoneyAlert+Spender','HighValue+Boomer+MoneyAlert+Spender','Expenses+Boomer+MoneyAlert+Spender','HighValue+Expenses+Boomer+MoneyAlert+Spender','Inherited',
# MAGIC                         'HighValue+Inherited','Expenses+Inherited','HighValue+Expenses+Inherited','Boomer+Inherited','HighValue+Boomer+Inherited','Expenses+Boomer+Inherited',
# MAGIC                         'HighValue+Expenses+Boomer+Inherited','MoneyAlert+Inherited','HighValue+MoneyAlert+Inherited','Expenses+MoneyAlert+Inherited','HighValue+Expenses+MoneyAlert+Inherited',
# MAGIC                         'Boomer+MoneyAlert+Inherited','HighValue+Boomer+MoneyAlert+Inherited','Expenses+Boomer+MoneyAlert+Inherited','HighValue+Expenses+Boomer+MoneyAlert+Inherited',
# MAGIC                         'Spender+Inherited','HighValue+Spender+Inherited','Expenses+Spender+Inherited','HighValue+Expenses+Spender+Inherited','Boomer+Spender+Inherited',
# MAGIC                         'HighValue+Boomer+Spender+Inherited','Expenses+Boomer+Spender+Inherited','HighValue+Expenses+Boomer+Spender+Inherited','MoneyAlert+Spender+Inherited',
# MAGIC                         'HighValue+MoneyAlert+Spender+Inherited','Expenses+MoneyAlert+Spender+Inherited','HighValue+Expenses+MoneyAlert+Spender+Inherited','Boomer+MoneyAlert+Spender+Inherited',
# MAGIC                         'HighValue+Boomer+MoneyAlert+Spender+Inherited','Expenses+Boomer+MoneyAlert+Spender+Inherited','HighValue+Expenses+Boomer+MoneyAlert+Spender+Inherited'
# MAGIC                         ) then 1 else 0  end)
# MAGIC       from Prospect
# MAGIC ) = 0
# MAGIC then 'OK' else 'Bad value' end, 'All MarketingNameplate values match the data'
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC --
# MAGIC -- Checks against the FactWatches table.
# MAGIC --
# MAGIC 
# MAGIC union all
# MAGIC select 'FactWatches row count', BatchID, Result, 'Actual row count matches Audit table' from (
# MAGIC       select distinct BatchID, (
# MAGIC             case when 
# MAGIC         --add sum message data and sum value
# MAGIC                   cast((select sum(MessageData) from DImessages where MessageSource = 'FactWatches' and MessageType = 'Validation' and MessageText = 'Row count' and BatchID = a.BatchID) as int) -
# MAGIC                   cast((select sum(MessageData) from DImessages where MessageSource = 'FactWatches' and MessageType = 'Validation' and MessageText = 'Row count' and BatchID = a.BatchID-1) as int) =
# MAGIC                   (select sum(Value) from Audit where DataSet = 'FactWatches' and Attribute = 'WH_ACTIVE' and BatchID = a.BatchID)
# MAGIC             then 'OK' else 'Mismatch' end
# MAGIC       ) as Result
# MAGIC       from Audit a where BatchID in (1, 2, 3)
# MAGIC ) o
# MAGIC 
# MAGIC union all
# MAGIC select 'FactWatches batches', NULL, case when 
# MAGIC       (select count(distinct BatchID) from FactWatches) = 3  and
# MAGIC       (select max(BatchID) from FactWatches) = 3
# MAGIC then 'OK' else 'Mismatch' end, 'BatchID values must match Audit table'
# MAGIC  
# MAGIC 
# MAGIC union all
# MAGIC -- HEAVILY modified this one since correlated subqueries 1) Have to be an aggregate and 2) Cannot use a non-equijoin
# MAGIC -- select 'FactWatches active watches', BatchID, Result, 'Actual total matches Audit table' from (
# MAGIC --       select distinct BatchID, case when
# MAGIC --             (select cast(MessageData as bigint) from DImessages where MessageSource = 'FactWatches' and MessageType = 'Validation' and MessageText = 'Row count' and BatchID = a.BatchID) +
# MAGIC --             (select cast(MessageData as bigint) from DImessages where MessageSource = 'FactWatches' and MessageType = 'Validation' and MessageText = 'Inactive watches' and BatchID = a.BatchID) =
# MAGIC --             (select sum(Value) from Audit where DataSet = 'FactWatches' and Attribute = 'WH_RECORDS' and BatchID <= a.BatchID)
# MAGIC --       then 'OK' else 'Mismatch' end as Result
# MAGIC --       from Audit a where BatchID in (1, 2, 3)
# MAGIC -- ) o
# MAGIC select 'FactWatches active watches', BatchID, Result, 'Actual total matches Audit table' from (
# MAGIC       select 
# MAGIC             distinct BatchID, 
# MAGIC             case when
# MAGIC                   (select cast(sum(MessageData) as bigint) from DImessages where MessageSource = 'FactWatches' and MessageType = 'Validation' and MessageText = 'Row count' and BatchID = a.BatchID) +
# MAGIC                   (select cast(sum(MessageData) as bigint) from DImessages where MessageSource = 'FactWatches' and MessageType = 'Validation' and MessageText = 'Inactive watches' and BatchID = a.BatchID) =
# MAGIC                   (
# MAGIC                         select sum(audit_batch_total)
# MAGIC                         from (
# MAGIC                               select batchid, sum(sum(Value)) over (order by batchid) as audit_batch_total 
# MAGIC                               from Audit 
# MAGIC                               where BatchID in (1, 2, 3) and DataSet = 'FactWatches' and Attribute = 'WH_RECORDS' 
# MAGIC                               group by batchid)
# MAGIC                               where BatchID = a.BatchID)
# MAGIC       then 'OK' else 'Mismatch' end as Result
# MAGIC       from Audit a where BatchID in (1, 2, 3)
# MAGIC ) o
# MAGIC  
# MAGIC union all
# MAGIC select 'FactWatches SK_CustomerID', NULL, case when
# MAGIC       (select count(*) from FactWatches) = 
# MAGIC       (select count(*) from FactWatches a
# MAGIC     join DimDate _d on _d.SK_DateID= A.SK_DateID_DatePlaced
# MAGIC        join DimCustomer c on a.SK_CustomerID = c.SK_CustomerID
# MAGIC         and c.EffectiveDate <= _d.DateValue --(select DateValue from DimDate where SK_DateID = a.SK_DateID_DatePlaced)
# MAGIC         and _d.DateValue <=c.EndDate)--(select DateValue from DimDate where SK_DateID = a.SK_DateID_DatePlaced) <= c.EndDate )
# MAGIC then 'OK' else 'Bad join' end, 'All SK_CustomerIDs match a DimCustomer record with a valid date range'
# MAGIC  
# MAGIC 
# MAGIC union all
# MAGIC select 'FactWatches SK_SecurityID', NULL, case when
# MAGIC       (select count(*) from FactWatches) = 
# MAGIC       (select count(*) from FactWatches a
# MAGIC     join DimDate _d on _d.SK_DateID=a.SK_DateID_DatePlaced
# MAGIC        join DimSecurity c on a.SK_SecurityID = c.SK_SecurityID
# MAGIC         and c.EffectiveDate <= _d.DateValue --(select DateValue from DimDate where SK_DateID = a.SK_DateID_DatePlaced)
# MAGIC         and _d.DateValue <=c.EndDate)--(select DateValue from DimDate where SK_DateID = a.SK_DateID_DatePlaced) <= c.EndDate )
# MAGIC then 'OK' else 'Bad join' end, 'All SK_SecurityIDs match a DimSecurity record with a valid date range'
# MAGIC  
# MAGIC 
# MAGIC union all 
# MAGIC -- Modified because of 1) too far nested correlated subqueries and 2) ANY correlated subqueries need aggregation (added first())
# MAGIC -- select 'FactWatches date check', BatchID, Result, 'All SK_DateID_ values are in the correct batch time window' from (
# MAGIC --       select distinct BatchID, (
# MAGIC --             case when (
# MAGIC --                   select count(*) from FactWatches w
# MAGIC --                   where w.BatchID = a.BatchID and (
# MAGIC --                         w.SK_DateID_DateRemoved is null and (
# MAGIC --                               (select DateValue from DimDate where SK_DateID = w.SK_DateID_DatePlaced) > 
# MAGIC --                               (select Date from Audit where DataSet = 'Batch' and Attribute = 'LastDay' and BatchID = a.BatchID)
# MAGIC --                               or
# MAGIC --                               (select DateValue from DimDate where SK_DateID = w.SK_DateID_DatePlaced) < 
# MAGIC --                               (select Date from Audit where DataSet = 'Batch' and Attribute = 'FirstDay' and BatchID = a.BatchID)
# MAGIC --                         ) or 
# MAGIC --                         w.SK_DateID_DateRemoved is not null and (
# MAGIC --                               (select DateValue from DimDate where SK_DateID = w.SK_DateID_DateRemoved) > 
# MAGIC --                               (select Date from Audit where DataSet = 'Batch' and Attribute = 'LastDay' and BatchID = a.BatchID)
# MAGIC --                               or
# MAGIC --                               (select DateValue from DimDate where SK_DateID = w.SK_DateID_DateRemoved) < 
# MAGIC --                               (select Date from Audit where DataSet = 'Batch' and Attribute = 'FirstDay' and BatchID = a.BatchID)
# MAGIC --                               or
# MAGIC --                               SK_DateID_DatePlaced > SK_DateID_DateRemoved
# MAGIC --                         )
# MAGIC --                   )
# MAGIC --             ) = 0
# MAGIC --             then 'OK' else 'Mismatch' end
# MAGIC --       ) as Result
# MAGIC --       from Audit a where BatchID in (1, 2, 3)
# MAGIC -- ) o
# MAGIC select 'FactWatches date check', BatchID, Result, 'All SK_DateID_ values are in the correct batch time window' from (
# MAGIC       select distinct BatchID, (
# MAGIC             case when (
# MAGIC                   select count(*) from FactWatches w
# MAGIC                   where w.BatchID = a.BatchID and (
# MAGIC                         w.SK_DateID_DateRemoved is null and (
# MAGIC                               (select first(DateValue) from DimDate where SK_DateID = w.SK_DateID_DatePlaced) > 
# MAGIC                               (select first(Date) from Audit where DataSet = 'Batch' and Attribute = 'LastDay' and BatchID = w.BatchID)
# MAGIC                               or
# MAGIC                               (select first(DateValue) from DimDate where SK_DateID = w.SK_DateID_DatePlaced) < 
# MAGIC                               (select first(Date) from Audit where DataSet = 'Batch' and Attribute = 'FirstDay' and BatchID = w.BatchID)
# MAGIC                         ) or 
# MAGIC                         w.SK_DateID_DateRemoved is not null and (
# MAGIC                               (select first(DateValue) from DimDate where SK_DateID = w.SK_DateID_DateRemoved) > 
# MAGIC                               (select first(Date) from Audit where DataSet = 'Batch' and Attribute = 'LastDay' and BatchID = w.BatchID)
# MAGIC                               or
# MAGIC                               (select first(DateValue) from DimDate where SK_DateID = w.SK_DateID_DateRemoved) < 
# MAGIC                               (select first(Date) from Audit where DataSet = 'Batch' and Attribute = 'FirstDay' and BatchID = w.BatchID)
# MAGIC                               or
# MAGIC                               SK_DateID_DatePlaced > SK_DateID_DateRemoved
# MAGIC                         )
# MAGIC                   )
# MAGIC             ) = 0
# MAGIC             then 'OK' else 'Mismatch' end
# MAGIC       ) as Result
# MAGIC       from Audit a where BatchID in (1, 2, 3)
# MAGIC ) o
# MAGIC 
# MAGIC --  
# MAGIC -- Checks against the DimTrade table.
# MAGIC --  
# MAGIC 
# MAGIC union all
# MAGIC -- HEAVILY modified this one since correlated subqueries 1) Have to be an aggregate and 2) Cannot use a non-equijoin
# MAGIC -- select 'DimTrade row count', BatchID, Result, 'Actual total matches Audit table' from (
# MAGIC --       select distinct BatchID, case when
# MAGIC --             (select MessageData from DImessages where MessageSource = 'DimTrade' and MessageType = 'Validation' and MessageText = 'Row count' and BatchID = a.BatchID)  =
# MAGIC --             (select sum(Value) from Audit where DataSet = 'DimTrade' and Attribute = 'T_NEW' and BatchID <= a.BatchID)
# MAGIC --       then 'OK' else 'Mismatch' end as Result
# MAGIC --       from Audit a where BatchID in (1, 2, 3)
# MAGIC -- ) o
# MAGIC select 'DimTrade row count', BatchID, Result, 'Actual total matches Audit table' from (
# MAGIC       select distinct BatchID, case when
# MAGIC             (
# MAGIC                   select sum(MessageData) from DImessages where MessageSource = 'DimTrade' and MessageType = 'Validation' and MessageText = 'Row count' and BatchID = a.BatchID) =
# MAGIC             (
# MAGIC                   select sum(audit_batch_total)
# MAGIC                   from (
# MAGIC                         select batchid, sum(sum(Value)) over (order by batchid) as audit_batch_total 
# MAGIC                         from Audit 
# MAGIC                         where BatchID in (1, 2, 3) and DataSet = 'DimTrade' and Attribute = 'T_NEW' 
# MAGIC                         group by batchid)
# MAGIC                   where BatchID = a.BatchID)
# MAGIC       then 'OK' else 'Mismatch' end as Result
# MAGIC       from Audit a where BatchID in (1, 2, 3)
# MAGIC ) o
# MAGIC 
# MAGIC union all 
# MAGIC select 'DimTrade canceled trades', NULL, case when 
# MAGIC             (select count(*) from DimTrade where Status = 'Canceled')  =
# MAGIC             (select sum(Value) from Audit where DataSet = 'DimTrade' and Attribute = 'T_CanceledTrades')
# MAGIC then 'OK' else 'Mismatch' end, 'Actual row counts matches Audit table'
# MAGIC  
# MAGIC 
# MAGIC union all 
# MAGIC select 'DimTrade commission alerts', NULL, case when 
# MAGIC             (select count(*) from DImessages where MessageType = 'Alert' and messageText = 'Invalid trade commission')  =
# MAGIC             (select sum(Value) from Audit where DataSet = 'DimTrade' and Attribute = 'T_InvalidCommision')
# MAGIC then 'OK' else 'Mismatch' end, 'Actual row counts matches Audit table'
# MAGIC  
# MAGIC 
# MAGIC union all 
# MAGIC select 'DimTrade charge alerts', NULL, case when 
# MAGIC             (select count(*) from DImessages where MessageType = 'Alert' and messageText = 'Invalid trade fee')  =
# MAGIC             (select sum(Value) from Audit where DataSet = 'DimTrade' and Attribute = 'T_InvalidCharge')
# MAGIC then 'OK' else 'Mismatch' end, 'Actual row counts matches Audit table'
# MAGIC  
# MAGIC 
# MAGIC union all
# MAGIC select 'DimTrade batches', NULL, case when 
# MAGIC       (select count(distinct BatchID) from DimTrade) = 3 and
# MAGIC       (select max(BatchID) from DimTrade) = 3 
# MAGIC then 'OK' else 'Mismatch' end, 'BatchID values must match Audit table'
# MAGIC 
# MAGIC union all
# MAGIC select 'DimTrade distinct keys', NULL, case when 
# MAGIC       (select count(distinct TradeID) from DimTrade) = 
# MAGIC       (select count(*) from DimTrade) 
# MAGIC then 'OK' else 'Not unique' end, 'All keys are distinct'
# MAGIC  
# MAGIC 
# MAGIC union all
# MAGIC select 'DimTrade SK_BrokerID', NULL, case when
# MAGIC       (select count(*) from DimTrade) = 
# MAGIC       (select count(*) from DimTrade a
# MAGIC     join DimDate _d on _d.SK_DateID=a.SK_CreateDateID
# MAGIC        join DimBroker c on a.SK_BrokerID = c.SK_BrokerID and c.EffectiveDate <= _d.DateValue and _d.DateValue <=c.EndDate)--(select DateValue from DimDate where SK_DateID = a.SK_CreateDateID) and (select DateValue from DimDate where SK_DateID = a.SK_CreateDateID) <= c.EndDate)
# MAGIC then 'OK' else 'Bad join' end, 'All SK_BrokerIDs match a DimBroker record with a valid date range'
# MAGIC  
# MAGIC 
# MAGIC union all
# MAGIC select 'DimTrade SK_CompanyID', NULL, case when
# MAGIC       (select count(*) from DimTrade) = 
# MAGIC       (select count(*) from DimTrade a
# MAGIC     join DimDate _d on _d.SK_DateID=a.SK_CreateDateID
# MAGIC        join DimCompany c on a.SK_CompanyID = c.SK_CompanyID and c.EffectiveDate <= _d.DateValue and _d.DateValue<=c.EndDate)--(select DateValue from DimDate where SK_DateID = a.SK_CreateDateID) and (select DateValue from DimDate where SK_DateID = a.SK_CreateDateID) <= c.EndDate)
# MAGIC then 'OK' else 'Bad join' end, 'All SK_CompanyIDs match a DimCompany record with a valid date range'
# MAGIC  
# MAGIC 
# MAGIC union all
# MAGIC select 'DimTrade SK_SecurityID', NULL, case when
# MAGIC       (select count(*) from DimTrade) = 
# MAGIC       (select count(*) from DimTrade a
# MAGIC     join DimDate _d on _d.SK_DateID=a.SK_CreateDateID
# MAGIC        join DimSecurity c on a.SK_SecurityID = c.SK_SecurityID and c.EffectiveDate <= _d.DateValue and _d.DateValue<=c.EndDate)--(select DateValue from DimDate where SK_DateID = a.SK_CreateDateID) and (select DateValue from DimDate where SK_DateID = a.SK_CreateDateID) <= c.EndDate)
# MAGIC then 'OK' else 'Bad join' end, 'All SK_SecurityIDs match a DimSecurity record with a valid date range'
# MAGIC  
# MAGIC 
# MAGIC union all
# MAGIC select 'DimTrade SK_CustomerID', NULL, case when
# MAGIC       (select count(*) from DimTrade) = 
# MAGIC       (select count(*) from DimTrade a
# MAGIC     join DimDate _d on _d.SK_DateID=a.SK_CreateDateID
# MAGIC        join DimCustomer c on a.SK_CustomerID = c.SK_CustomerID and c.EffectiveDate <= _d.DateValue and _d.DateValue<=c.EndDate)--(select DateValue from DimDate where SK_DateID = a.SK_CreateDateID) and (select DateValue from DimDate where SK_DateID = a.SK_CreateDateID) <= c.EndDate)
# MAGIC then 'OK' else 'Bad join' end, 'All SK_CustomerIDs match a DimCustomer record with a valid date range'
# MAGIC  
# MAGIC 
# MAGIC union all
# MAGIC select 'DimTrade SK_AccountID', NULL, case when
# MAGIC       (select count(*) from DimTrade) = 
# MAGIC       (select count(*) from DimTrade a
# MAGIC     join DimDate _d on _d.SK_DateID=a.SK_CreateDateID
# MAGIC        join DimAccount c on a.SK_AccountID = c.SK_AccountID and c.EffectiveDate <= _d.DateValue and _d.DateValue<=c.EndDate)--(select DateValue from DimDate where SK_DateID = a.SK_CreateDateID) and (select DateValue from DimDate where SK_DateID = a.SK_CreateDateID) <= c.EndDate)
# MAGIC then 'OK' else 'Bad join' end, 'All SK_AccountIDs match a DimAccount record with a valid date range'
# MAGIC 
# MAGIC union all
# MAGIC -- Modified because of 1) too far nested correlated subqueries and 2) ANY correlated subqueries need aggregation (added first())
# MAGIC -- select 'DimTrade date check', BatchID, Result, 'All SK_DateID values are in the correct batch time window' from (
# MAGIC --       select distinct BatchID, (
# MAGIC --             case when (
# MAGIC --                   select count(*) from DimTrade w
# MAGIC --                   where w.BatchID = a.BatchID and (
# MAGIC --                         w.SK_CloseDateID is null and (
# MAGIC --                               (select DateValue from DimDate where SK_DateID = w.SK_CreateDateID) > 
# MAGIC --                               (select Date from Audit where DataSet = 'Batch' and Attribute = 'LastDay' and BatchID = a.BatchID)
# MAGIC --                               or
# MAGIC --                               (select DateValue from DimDate where SK_DateID = w.SK_CreateDateID) < 
# MAGIC --                               (case when w.Type like 'Limit%'  /* Limit trades can have create dates earlier than the current Batch date, but not earlier than Batch1's first date */
# MAGIC --                                               then (select Date from Audit where DataSet = 'Batch' and Attribute = 'FirstDay' and BatchID = 1) 
# MAGIC --                                               else (select Date from Audit where DataSet = 'Batch' and Attribute = 'FirstDay' and BatchID = a.BatchID) end)
# MAGIC --                         ) or 
# MAGIC --                         w.SK_CloseDateID is not null and (
# MAGIC --                               (select DateValue from DimDate where SK_DateID = w.SK_CloseDateID) > 
# MAGIC --                               (select Date from Audit where DataSet = 'Batch' and Attribute = 'LastDay' and BatchID = a.BatchID)
# MAGIC --                               or
# MAGIC --                               (select DateValue from DimDate where SK_DateID = w.SK_CloseDateID) < 
# MAGIC --                               (select Date from Audit where DataSet = 'Batch' and Attribute = 'FirstDay' and BatchID = a.BatchID)
# MAGIC --                               or
# MAGIC --                               SK_CloseDateID < SK_CreateDateID
# MAGIC --                         )
# MAGIC --                   )
# MAGIC --             ) = 0
# MAGIC --             then 'OK' else 'Mismatch' end
# MAGIC --       ) as Result
# MAGIC --       from Audit a where BatchID in (1, 2, 3)
# MAGIC -- ) o
# MAGIC select 'DimTrade date check', BatchID, Result, 'All SK_DateID values are in the correct batch time window' from (
# MAGIC       select distinct BatchID, (
# MAGIC             case when (
# MAGIC                   select count(*) from DimTrade w
# MAGIC                   where w.BatchID = a.BatchID and (
# MAGIC                         w.SK_CloseDateID is null and (
# MAGIC                               (select first(DateValue) from DimDate where SK_DateID = w.SK_CreateDateID) > 
# MAGIC                               (select first(Date) from Audit where DataSet = 'Batch' and Attribute = 'LastDay' and BatchID = w.BatchID)
# MAGIC                               or
# MAGIC                               (select first(DateValue) from DimDate where SK_DateID = w.SK_CreateDateID) < 
# MAGIC                               (case when w.Type like 'Limit%'  /* Limit trades can have create dates earlier than the current Batch date, but not earlier than Batch1's first date */
# MAGIC                                               then (select first(Date) from Audit where DataSet = 'Batch' and Attribute = 'FirstDay' and BatchID = 1) 
# MAGIC                                               else (select first(Date) from Audit where DataSet = 'Batch' and Attribute = 'FirstDay' and BatchID = w.BatchID) end)
# MAGIC                         ) or 
# MAGIC                         w.SK_CloseDateID is not null and (
# MAGIC                               (select first(DateValue) from DimDate where SK_DateID = w.SK_CloseDateID) > 
# MAGIC                               (select first(Date) from Audit where DataSet = 'Batch' and Attribute = 'LastDay' and BatchID = w.BatchID)
# MAGIC                               or
# MAGIC                               (select first(DateValue) from DimDate where SK_DateID = w.SK_CloseDateID) < 
# MAGIC                               (select first(Date) from Audit where DataSet = 'Batch' and Attribute = 'FirstDay' and BatchID = w.BatchID)
# MAGIC                               or
# MAGIC                               SK_CloseDateID < SK_CreateDateID
# MAGIC                         )
# MAGIC                   )
# MAGIC             ) = 0
# MAGIC             then 'OK' else 'Mismatch' end
# MAGIC       ) as Result
# MAGIC       from Audit a where BatchID in (1, 2, 3)
# MAGIC ) o
# MAGIC 
# MAGIC union all
# MAGIC select 'DimTrade Status', NULL, case when (
# MAGIC       select count(*) from DimTrade 
# MAGIC       where Status not in ( 'Canceled', 'Pending', 'Submitted', 'Active', 'Completed' )
# MAGIC ) = 0
# MAGIC then 'OK' else 'Bad value' end, 'All Trade Status values are valid'
# MAGIC  
# MAGIC 
# MAGIC union all
# MAGIC select 'DimTrade Type', NULL, case when (
# MAGIC       select count(*) from DimTrade 
# MAGIC       where Type not in ( 'Market Buy', 'Market Sell', 'Stop Loss', 'Limit Sell', 'Limit Buy' )
# MAGIC ) = 0
# MAGIC then 'OK' else 'Bad value' end, 'All Trade Type values are valid'
# MAGIC  
# MAGIC 
# MAGIC 
# MAGIC --  
# MAGIC -- Checks against the Financial table.
# MAGIC --  
# MAGIC 
# MAGIC union all 
# MAGIC select 'Financial row count', NULL, case when 
# MAGIC             (select MessageData from DImessages where MessageSource = 'Financial' and MessageType = 'Validation' and MessageText = 'Row count' and BatchID = 1)  =
# MAGIC             (select sum(Value) from Audit where DataSet = 'Financial' and Attribute = 'FW_FIN')
# MAGIC then 'OK' else 'Mismatch' end, 'Actual row count matches Audit table'
# MAGIC  
# MAGIC 
# MAGIC union all
# MAGIC select 'Financial SK_CompanyID', NULL, case when
# MAGIC       (select count(*) from Financial) = 
# MAGIC       (select count(*) from Financial a join DimCompany c on a.SK_CompanyID = c.SK_CompanyID )
# MAGIC then 'OK' else 'Bad join' end, 'All SK_CompanyIDs match a DimCompany record'
# MAGIC  
# MAGIC 
# MAGIC union all
# MAGIC select 'Financial FI_YEAR', NULL, case when (
# MAGIC       (select count(*) from Financial where FI_YEAR < year((select Date from Audit where DataSet = 'Batch' and BatchID = 1 and Attribute = 'FirstDay'))) + 
# MAGIC       (select count(*) from Financial where FI_YEAR > year((select Date from Audit where DataSet = 'Batch' and BatchID = 1 and Attribute = 'LastDay')))
# MAGIC ) = 0
# MAGIC then 'OK' else 'Bad Year' end, 'All Years are within Batch1 range'
# MAGIC  
# MAGIC 
# MAGIC union all
# MAGIC select 'Financial FI_QTR', NULL, case when (
# MAGIC       select count(*) from Financial where FI_QTR not in ( 1, 2, 3, 4 )
# MAGIC ) = 0
# MAGIC then 'OK' else 'Bad Qtr' end, 'All quarters are in ( 1, 2, 3, 4 )'
# MAGIC  
# MAGIC 
# MAGIC union all
# MAGIC select 'Financial FI_QTR_START_DATE', NULL, case when (
# MAGIC       select count(*) from Financial
# MAGIC       where FI_YEAR <> year(FI_QTR_START_DATE)
# MAGIC          or month(FI_QTR_START_DATE) <> (FI_QTR-1)*3+1
# MAGIC          or day(FI_QTR_START_DATE) <> 1
# MAGIC ) = 0
# MAGIC then 'OK' else 'Bad date' end, 'All quarters start on correct date'
# MAGIC  
# MAGIC 
# MAGIC union all                                                               
# MAGIC select 'Financial EPS', NULL, case when (
# MAGIC       select count(*) from Financial
# MAGIC       where Round(FI_NET_EARN/FI_OUT_BASIC,2)- FI_BASIC_EPS not between -0.4 and 0.4
# MAGIC          or  Round(FI_NET_EARN/FI_OUT_DILUT,2) - FI_DILUT_EPS not between -0.4 and 0.4
# MAGIC          or  Round(FI_NET_EARN/FI_REVENUE,2) - FI_MARGIN not between -0.4 and 0.4
# MAGIC ) = 0
# MAGIC then 'OK' else 'Bad EPS' end, 'Earnings calculations are valid'
# MAGIC  
# MAGIC 
# MAGIC 
# MAGIC --  
# MAGIC -- Checks against the FactMarketHistory table.
# MAGIC --  
# MAGIC 
# MAGIC union all
# MAGIC select 'FactMarketHistory row count', BatchID, Result, 'Actual row count matches Audit table' from (
# MAGIC       select distinct BatchID, (
# MAGIC             case when 
# MAGIC         --added sum(messageData) and sum(value)
# MAGIC                   cast((select sum(MessageData) from DImessages where MessageSource = 'FactMarketHistory' and MessageType = 'Validation' and MessageText = 'Row count' and BatchID = a.BatchID) as int) -
# MAGIC                   cast((select sum(MessageData) from DImessages where MessageSource = 'FactMarketHistory' and MessageType = 'Validation' and MessageText = 'Row count' and BatchID = a.BatchID-1) as int) =
# MAGIC                   (select sum(Value) from Audit where DataSet = 'FactMarketHistory' and Attribute = 'DM_RECORDS' and BatchID = a.BatchID)
# MAGIC             then 'OK' else 'Mismatch' end
# MAGIC       ) as Result
# MAGIC       from Audit a where BatchID in (1, 2, 3)
# MAGIC ) o
# MAGIC 
# MAGIC union all
# MAGIC select 'FactMarketHistory batches', NULL, case when 
# MAGIC       (select count(distinct BatchID) from FactMarketHistory) = 3 and
# MAGIC       (select max(BatchID) from FactMarketHistory) = 3 
# MAGIC then 'OK' else 'Mismatch' end, 'BatchID values must match Audit table'
# MAGIC 
# MAGIC 
# MAGIC union all
# MAGIC select 'FactMarketHistory SK_CompanyID', NULL, case when
# MAGIC       (select count(*) from FactMarketHistory) = 
# MAGIC       (select count(*) from FactMarketHistory a join DimDate _d on _d.SK_DateID=a.SK_DateID join DimCompany c on a.SK_CompanyID = c.SK_CompanyID and c.EffectiveDate <= _d.DateValue and _d.DateValue <=c.EndDate)--(select DateValue from DimDate where SK_DateID = a.SK_DateID) and (select DateValue from DimDate where SK_DateID = a.SK_DateID) <= c.EndDate)
# MAGIC then 'OK' else 'Bad join' end, 'All SK_CompanyIDs match a DimCompany record with a valid date range'
# MAGIC 
# MAGIC 
# MAGIC union all
# MAGIC select 'FactMarketHistory SK_SecurityID', NULL, case when
# MAGIC       (select count(*) from FactMarketHistory) = 
# MAGIC       (select count(*) from FactMarketHistory a join DimDate _d on _d.SK_DateID=a.SK_DateID join DimSecurity c on a.SK_SecurityID = c.SK_SecurityID and c.EffectiveDate <= _d.DateValue and _d.DateValue <= c.EndDate)--(select DateValue from DimDate where SK_DateID = a.SK_DateID) and (select DateValue from DimDate where SK_DateID = a.SK_DateID) <= c.EndDate)
# MAGIC then 'OK' else 'Bad join' end, 'All SK_SecurityIDs match a DimSecurity record with a valid date range'
# MAGIC 
# MAGIC 
# MAGIC union all
# MAGIC -- Modified because of 1) too far nested correlated subqueries and 2) ANY correlated subqueries need aggregation (added first())
# MAGIC -- select 'FactMarketHistory SK_DateID', BatchID, Result, 'All dates are within batch date range' from (
# MAGIC --       select distinct BatchID, (
# MAGIC --             case when (
# MAGIC --                   (select count(*) from FactMarketHistory m where m.BatchID = a.BatchID and (select DateValue from DimDate where SK_DateID = m.SK_DateID) < (select Date-1 day from Audit where DataSet = 'Batch' and BatchID = a.BatchID and Attribute = 'FirstDay')) + 
# MAGIC --                   (select count(*) from FactMarketHistory m where m.BatchID = a.BatchID and (select DateValue from DimDate where SK_DateID = m.SK_DateID) >= (select Date from Audit where DataSet = 'Batch' and BatchID = a.BatchID and Attribute = 'LastDay'))
# MAGIC --             ) = 0
# MAGIC --             then 'OK' else 'Bad Date' end
# MAGIC --       ) as Result
# MAGIC --       from Audit a where BatchID in (1, 2, 3)
# MAGIC -- ) o
# MAGIC select 'FactMarketHistory SK_DateID', BatchID, Result, 'All dates are within batch date range' from (
# MAGIC       select distinct BatchID, (
# MAGIC             case when (
# MAGIC                   (select count(*) from FactMarketHistory m where m.BatchID = a.BatchID and (select first(DateValue) from DimDate where SK_DateID = m.SK_DateID) < (select first(Date) -1 day from Audit where DataSet = 'Batch' and BatchID = m.BatchID and Attribute = 'FirstDay')) + 
# MAGIC                   (select count(*) from FactMarketHistory m where m.BatchID = a.BatchID and (select first(DateValue) from DimDate where SK_DateID = m.SK_DateID) >= (select first(Date) from Audit where DataSet = 'Batch' and BatchID = m.BatchID and Attribute = 'LastDay'))
# MAGIC             ) = 0
# MAGIC             then 'OK' else 'Bad Date' end
# MAGIC       ) as Result
# MAGIC       from Audit a where BatchID in (1, 2, 3)
# MAGIC ) o
# MAGIC 
# MAGIC union all
# MAGIC select 'FactMarketHistory relative dates', NULL, case when (
# MAGIC       select count(*) from FactMarketHistory
# MAGIC       where FiftyTwoWeekLow > DayLow
# MAGIC          or DayLow > ClosePrice
# MAGIC          or ClosePrice > DayHigh
# MAGIC          or DayHigh > FiftyTwoWeekHigh
# MAGIC ) = 0
# MAGIC then 'OK' else 'Bad Date' end, '52-week-low <= day_low <= close_price <= day_high <= 52-week-high'
# MAGIC 
# MAGIC 
# MAGIC --  
# MAGIC -- Checks against the FactHoldings table.
# MAGIC --  
# MAGIC union all
# MAGIC select 'FactHoldings row count', BatchID, Result, 'Actual row count matches Audit table' from (
# MAGIC       select distinct BatchID, (
# MAGIC             case when 
# MAGIC         --added sum for messagedata and value
# MAGIC                   cast((select sum(MessageData) from DImessages where MessageSource = 'FactHoldings' and MessageType = 'Validation' and MessageText = 'Row count' and BatchID = a.BatchID) as int) -
# MAGIC                   cast((select sum(MessageData) from DImessages where MessageSource = 'FactHoldings' and MessageType = 'Validation' and MessageText = 'Row count' and BatchID = a.BatchID-1) as int) =
# MAGIC                   (select sum(Value) from Audit where DataSet = 'FactHoldings' and Attribute = 'HH_RECORDS' and BatchID = a.BatchID)
# MAGIC             then 'OK' else 'Mismatch' end
# MAGIC       ) as Result
# MAGIC       from Audit a where BatchID in (1, 2, 3)
# MAGIC ) o
# MAGIC   
# MAGIC union all
# MAGIC select 'FactHoldings batches', NULL, case when 
# MAGIC       (select count(distinct BatchID) from FactHoldings) = 3 and
# MAGIC       (select max(BatchID) from FactHoldings) = 3 
# MAGIC then 'OK' else 'Mismatch' end, 'BatchID values must match Audit table'
# MAGIC 
# MAGIC 
# MAGIC union all
# MAGIC /* It is possible that the dimension record has changed between orgination of the trade and the completion of the trade. *
# MAGIC  * So, we can check that the Effective Date of the dimension record is older than the the completion date, but the end date could be earlier or later than the completion date
# MAGIC  */
# MAGIC select 'FactHoldings SK_CustomerID', NULL, case when
# MAGIC       (select count(*) from FactHoldings) = 
# MAGIC       (select count(*) from FactHoldings a join DimDate _d on _d.SK_DateID=a.SK_DateID join DimCustomer c on a.SK_CustomerID = c.SK_CustomerID and c.EffectiveDate <= _d.DateValue --(select DateValue from DimDate where SK_DateID = a.SK_DateID) 
# MAGIC        )
# MAGIC then 'OK' else 'Bad join' end, 'All SK_CustomerIDs match a DimCustomer record with a valid date range'
# MAGIC 
# MAGIC 
# MAGIC union all
# MAGIC select 'FactHoldings SK_AccountID', NULL, case when
# MAGIC       (select count(*) from FactHoldings) = 
# MAGIC       (select count(*) from FactHoldings a join DimDate _d on _d.SK_DateID=a.SK_DateID join DimAccount c on a.SK_AccountID = c.SK_AccountID and c.EffectiveDate <= _d.DateValue--(select DateValue from DimDate where SK_DateID = a.SK_DateID) 
# MAGIC        )
# MAGIC then 'OK' else 'Bad join' end, 'All SK_AccountIDs match a DimAccount record with a valid date range'
# MAGIC --good till here
# MAGIC 
# MAGIC union all
# MAGIC select 'FactHoldings SK_CompanyID', NULL, case when
# MAGIC       (select count(*) from FactHoldings) = 
# MAGIC       (select count(*) from FactHoldings a join DimDate _d on _d.SK_DateID=a.SK_DateID join DimCompany c on a.SK_CompanyID = c.SK_CompanyID and c.EffectiveDate <= _d.DateValue--(select DateValue from DimDate where SK_DateID = a.SK_DateID) 
# MAGIC        )
# MAGIC then 'OK' else 'Bad join' end, 'All SK_CompanyIDs match a DimCompany record with a valid date range'
# MAGIC 
# MAGIC 
# MAGIC union all
# MAGIC select 'FactHoldings SK_SecurityID', NULL, case when
# MAGIC       (select count(*) from FactHoldings) = 
# MAGIC       (select count(*) from FactHoldings a join DimDate _d on _d.SK_DateID=a.SK_DateID join DimSecurity c on a.SK_SecurityID = c.SK_SecurityID and c.EffectiveDate <= _d.DateValue--(select DateValue from DimDate where SK_DateID = a.SK_DateID) 
# MAGIC        )
# MAGIC then 'OK' else 'Bad join' end, 'All SK_SecurityIDs match a DimSecurity record with a valid date range'
# MAGIC 
# MAGIC 
# MAGIC union all
# MAGIC select 'FactHoldings CurrentTradeID', NULL, case when
# MAGIC       (select count(*) from FactHoldings) = 
# MAGIC       (select count(*) from FactHoldings a join DimTrade t on a.CurrentTradeID = t.TradeID and a.SK_DateID = t.SK_CloseDateID and a.SK_TimeID = t.SK_CloseTimeID) 
# MAGIC then 'OK' else 'Failed' end, 'CurrentTradeID matches a DimTrade record with and Close Date and Time are values are used as the holdings date and time'
# MAGIC 
# MAGIC 
# MAGIC union all
# MAGIC -- Modified because of 1) too far nested correlated subqueries and 2) ANY correlated subqueries need aggregation (added first())
# MAGIC -- select 'FactHoldings SK_DateID', BatchID, Result, 'All dates are within batch date range' from (
# MAGIC --       select distinct BatchID, (
# MAGIC --             case when (
# MAGIC --                   (select count(*) from FactHoldings m where BatchID = a.BatchID and (select DateValue from DimDate where SK_DateID = m.SK_DateID) < (select Date from Audit where DataSet = 'Batch' and BatchID = a.BatchID and Attribute = 'FirstDay')) + 
# MAGIC --                   (select count(*) from FactHoldings m where BatchID = a.BatchID and (select DateValue from DimDate where SK_DateID = m.SK_DateID) > (select Date from Audit where DataSet = 'Batch' and BatchID = a.BatchID and Attribute = 'LastDay'))
# MAGIC --             ) = 0
# MAGIC --             then 'OK' else 'Bad Date' end
# MAGIC --       ) as Result
# MAGIC --       from Audit a where BatchID in (1, 2, 3)
# MAGIC -- ) o
# MAGIC select 'FactHoldings SK_DateID', BatchID, Result, 'All dates are within batch date range' from (
# MAGIC       select distinct BatchID, (
# MAGIC             case when (
# MAGIC                   (select count(*) from FactHoldings m where BatchID = a.BatchID and (select first(DateValue) from DimDate where SK_DateID = m.SK_DateID) < (select first(Date) from Audit where DataSet = 'Batch' and BatchID = m.BatchID and Attribute = 'FirstDay')) + 
# MAGIC                   (select count(*) from FactHoldings m where BatchID = a.BatchID and (select first(DateValue) from DimDate where SK_DateID = m.SK_DateID) > (select first(Date) from Audit where DataSet = 'Batch' and BatchID = m.BatchID and Attribute = 'LastDay'))
# MAGIC             ) = 0
# MAGIC             then 'OK' else 'Bad Date' end
# MAGIC       ) as Result
# MAGIC       from Audit a where BatchID in (1, 2, 3)
# MAGIC ) o
# MAGIC 
# MAGIC --  
# MAGIC -- Checks against the FactCashBalances table.
# MAGIC --  
# MAGIC union all
# MAGIC select 'FactCashBalances batches', NULL, case when 
# MAGIC       (select count(distinct BatchID) from FactCashBalances) = 3 and
# MAGIC       (select max(BatchID) from FactCashBalances) = 3 
# MAGIC then 'OK' else 'Mismatch' end, 'BatchID values must match Audit table'
# MAGIC 
# MAGIC 
# MAGIC union all
# MAGIC select 'FactCashBalances SK_CustomerID', NULL, case when
# MAGIC       (select count(*) from FactCashBalances) = 
# MAGIC       (select count(*) from FactCashBalances a join DimDate _d on _d.SK_DateID=a.SK_DateID join DimCustomer c on a.SK_CustomerID = c.SK_CustomerID and c.EffectiveDate <= _d.DateValue and _d.DateValue <=c.EndDate)--(select DateValue from DimDate where SK_DateID = a.SK_DateID) and (select DateValue from DimDate where SK_DateID = a.SK_DateID) <= c.EndDate)
# MAGIC then 'OK' else 'Bad join' end, 'All SK_CustomerIDs match a DimCustomer record with a valid date range'
# MAGIC 
# MAGIC 
# MAGIC union all
# MAGIC select 'FactCashBalances SK_AccountID', NULL, case when
# MAGIC       (select count(*) from FactCashBalances) = 
# MAGIC       (select count(*) from FactCashBalances a join DimDate _d on _d.SK_DateID=a.SK_DateID join DimAccount c on a.SK_AccountID = c.SK_AccountID and c.EffectiveDate <= _d.DateValue and _d.DateValue <= c.EndDate)--(select DateValue from DimDate where SK_DateID = a.SK_DateID) and (select DateValue from DimDate where SK_DateID = a.SK_DateID) <= c.EndDate)
# MAGIC then 'OK' else 'Bad join' end, 'All SK_AccountIDs match a DimAccount record with a valid date range'
# MAGIC 
# MAGIC 
# MAGIC union all
# MAGIC -- Modified because of 1) too far nested correlated subqueries and 2) ANY correlated subqueries need aggregation (added first())
# MAGIC -- select 'FactCashBalances SK_DateID', BatchID, Result, 'All dates are within batch date range' from (
# MAGIC --    select distinct BatchID, (
# MAGIC --          case when (
# MAGIC --                (select count(*) from FactCashBalances m where BatchID = a.BatchID and (select DateValue from DimDate where SK_DateID = m.SK_DateID) < (select Date from Audit where DataSet = 'Batch' and BatchID = a.BatchID and Attribute = 'FirstDay')) + 
# MAGIC --                (select count(*) from FactCashBalances m where BatchID = a.BatchID and (select DateValue from DimDate where SK_DateID = m.SK_DateID) > (select Date from Audit where DataSet = 'Batch' and BatchID = a.BatchID and Attribute = 'LastDay'))
# MAGIC --          ) = 0
# MAGIC --          then 'OK' else 'Bad Date' end
# MAGIC --    ) as Result
# MAGIC --    from Audit a where BatchID in (1, 2, 3)
# MAGIC -- ) o
# MAGIC select 'FactCashBalances SK_DateID', BatchID, Result, 'All dates are within batch date range' from (
# MAGIC       select distinct 
# MAGIC             BatchID, (
# MAGIC             case when ((
# MAGIC                   select count(*) 
# MAGIC                   from FactCashBalances m 
# MAGIC                   where 
# MAGIC                         BatchID = a.BatchID 
# MAGIC                         and (select first(DateValue) from DimDate where SK_DateID = m.SK_DateID) < (select first(Date) from Audit where DataSet = 'Batch' and BatchID = m.BatchID and Attribute = 'FirstDay')) + (
# MAGIC                   select count(*) 
# MAGIC                   from FactCashBalances m 
# MAGIC                   where 
# MAGIC                         BatchID = a.BatchID 
# MAGIC                         and (select first(DateValue) from DimDate where SK_DateID = m.SK_DateID) > (select first(Date) from Audit where DataSet = 'Batch' and BatchID = m.BatchID and Attribute = 'LastDay'))
# MAGIC                   ) = 0 then 'OK' else 'Bad Date' end) as Result
# MAGIC       from Audit a where BatchID in (1, 2, 3)
# MAGIC ) o
# MAGIC 
# MAGIC /*
# MAGIC  *  Checks against the Batch Validation Query row counts
# MAGIC  */
# MAGIC 
# MAGIC union all
# MAGIC select 'Batch row count: ' || MessageSource, BatchID,
# MAGIC       case when RowsLastBatch > RowsThisBatch then 'Row count decreased' else 'OK' end,
# MAGIC       'Row counts do not decrease between successive batches'
# MAGIC from (
# MAGIC       select distinct(a.BatchID), m.MessageSource, cast(m1.MessageData as bigint) as RowsThisBatch, cast(m2.MessageData as bigint) as RowsLastBatch
# MAGIC       from Audit a 
# MAGIC       full join DImessages m on m.BatchID = 0 and m.MessageText = 'Row count' and m.MessageType = 'Validation'
# MAGIC       join DImessages m1 on m1.BatchID = a.BatchID   and m1.MessageSource = m.MessageSource and m1.MessageText = 'Row count' and m1.MessageType = 'Validation'
# MAGIC       join DImessages m2 on m2.BatchID = a.BatchID-1 and m2.MessageSource = m.MessageSource and m2.MessageText = 'Row count' and m2.MessageType = 'Validation'
# MAGIC       where a.BatchID in (1, 2, 3)
# MAGIC ) o
# MAGIC 
# MAGIC union all
# MAGIC select 'Batch joined row count: ' || MessageSource, BatchID,
# MAGIC       case when RowsJoined = RowsUnjoined then 'OK' else 'No match' end,
# MAGIC       'Row counts match when joined to dimensions'
# MAGIC from (
# MAGIC       select distinct(a.BatchID), m.MessageSource, cast(m1.MessageData as bigint) as RowsUnjoined, cast(m2.MessageData as bigint) as RowsJoined
# MAGIC       from Audit a 
# MAGIC       full join DImessages m on m.BatchID = 0 and m.MessageText = 'Row count' and m.MessageType = 'Validation'
# MAGIC       join DImessages m1 on m1.BatchID = a.BatchID and m1.MessageSource = m.MessageSource and m1.MessageText = 'Row count' and m1.MessageType = 'Validation'
# MAGIC       join DImessages m2 on m2.BatchID = a.BatchID and m2.MessageSource = m.MessageSource and m2.MessageText = 'Row count joined' and m2.MessageType = 'Validation'
# MAGIC       where a.BatchID in (1, 2, 3)
# MAGIC ) o
# MAGIC 
# MAGIC 
# MAGIC /*
# MAGIC  *  Checks against the Data Visibility Query row counts
# MAGIC  */
# MAGIC 
# MAGIC union all
# MAGIC select 'Data visibility row counts: ' || MessageSource , NULL as BatchID,
# MAGIC       case when regressions = 0 then 'OK' else 'Row count decreased' end,
# MAGIC       'Row counts must be non-decreasing over time'
# MAGIC from (
# MAGIC       select m1.MessageSource, sum( case when cast(m1.MessageData as bigint) > cast(m2.MessageData as bigint) then 1 else 0 end ) as regressions
# MAGIC       from DImessages m1 
# MAGIC       join DImessages m2 on 
# MAGIC             m2.MessageType IN ('Visibility_1', 'Visibility_2') and 
# MAGIC             m2.MessageText = 'Row count' and
# MAGIC             m2.MessageSource = m1.MessageSource and
# MAGIC             m2.MessageDateAndTime > m1.MessageDateAndTime
# MAGIC       where m1.MessageType IN ('Visibility_1', 'Visibility_2') and m1.MessageText = 'Row count'
# MAGIC       group by m1.MessageSource
# MAGIC ) o
# MAGIC 
# MAGIC union all
# MAGIC select 'Data visibility joined row counts: ' || MessageSource , NULL as BatchID,
# MAGIC       case when regressions = 0 then 'OK' else 'No match' end,
# MAGIC       'Row counts match when joined to dimensions'
# MAGIC from (
# MAGIC       select 
# MAGIC         m1.MessageSource, 
# MAGIC         sum(
# MAGIC           case 
# MAGIC             when cast(m1.MessageData as bigint) > cast(m2.MessageData as bigint) then 1
# MAGIC             else 0 end) as regressions
# MAGIC       from DImessages m1 
# MAGIC       join DImessages m2 on 
# MAGIC             m2.MessageType = 'Visibility_1' and 
# MAGIC             m2.MessageText = 'Row count joined' and
# MAGIC             m2.MessageSource = m1.MessageSource and
# MAGIC             m2.MessageDateAndTime = m1.MessageDateAndTime
# MAGIC       where m1.MessageType = 'Visibility_1' and m1.MessageText = 'Row count'
# MAGIC       group by m1.MessageSource
# MAGIC ) o
# MAGIC 
# MAGIC /* close the outer query */
# MAGIC ) q
# MAGIC order by Test, Batch;
