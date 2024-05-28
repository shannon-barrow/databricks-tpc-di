-- Databricks notebook source
-- CREATE WIDGET DROPDOWN scale_factor DEFAULT "10" CHOICES SELECT * FROM (VALUES ("10"), ("100"), ("1000"), ("5000"), ("10000"));
-- CREATE WIDGET TEXT wh_db DEFAULT '';
-- CREATE WIDGET TEXT catalog DEFAULT 'tpcdi';

-- COMMAND ----------

CREATE OR REPLACE TABLE ${catalog}.${wh_db}_${scale_factor}.automated_Audit_results as
select *
from (
    select 'Audit table batches' as Test, NULL as Batch, case
        when (
          select count(distinct BatchID)
          from ${catalog}.${wh_db}_${scale_factor}.Audit
        ) = 3
        and (
          select max(BatchID)
          from ${catalog}.${wh_db}_${scale_factor}.Audit
        ) = 3 then 'OK'
        else 'Not 3 batches'
      end as Result, 'There must be Audit data for 3 batches' as Description
    union all
    select 'Audit table sources' as Test, NULL as Batch, case
        when (
          select count(distinct DataSet)
          from ${catalog}.${wh_db}_${scale_factor}.Audit
          where DataSet in (
              'Batch', 'DimAccount', 'DimBroker', 'DimCompany', 'DimCustomer', 'DimSecurity', 'DimTrade', 'FactHoldings', 'FactMarketHistory', 'FactWatches', 'Financial', 'Generator', 'Prospect'
            )
        ) = 13 then 'OK'
        else 'Mismatch'
      end as Result, 'There must be Audit data for all data sets' as Description -- Checks against the DImessages table.
    union all
    select 'DImessages validation reports', BatchID, Result, 'Every batch must have a full set of validation reports'
    from (
        select distinct BatchID, (
            case
              when (
                select count(*)
                from ${catalog}.${wh_db}_${scale_factor}.dimessages
                where BatchID = a.BatchID
                  and MessageType = 'Validation'
              ) = 24 then 'OK'
              else 'Validation checks not fully reported'
            end
          ) as Result
        from ${catalog}.${wh_db}_${scale_factor}.dimessages a
      ) o
    union all
    select 'DImessages batches', NULL, case
        when (
          select count(distinct BatchID)
          from ${catalog}.${wh_db}_${scale_factor}.dimessages
        ) = 4
        and (
          select max(BatchID)
          from ${catalog}.${wh_db}_${scale_factor}.dimessages
        ) = 3 then 'OK'
        else 'Not 3 batches plus batch 0'
      end, 'Must have 3 distinct batches reported in DImessages'
    union all
    select 'DImessages Phase complete records', NULL, case
        when (
          select count(distinct BatchID)
          from ${catalog}.${wh_db}_${scale_factor}.dimessages
          where MessageType = 'PCR'
        ) = 4
        and (
          select max(BatchID)
          from ${catalog}.${wh_db}_${scale_factor}.dimessages
          where MessageType = 'PCR'
        ) = 3 then 'OK'
        else 'Not 4 Phase Complete Records'
      end, 'Must have 4 Phase Complete records'
    union all
    select 'DImessages sources', NULL, case
        when (
          select count(*)
          from (
              select distinct MessageSource
              from ${catalog}.${wh_db}_${scale_factor}.dimessages
              where MessageType = 'Validation'
                and MessageSource in (
                  'DimAccount', 'DimBroker', 'DimCustomer', 'DimDate', 'DimSecurity', 'DimTime', 'DimTrade', 'FactCashBalances', 'FactHoldings', 'FactMarketHistory', 'FactWatches', 'Financial', 'Industry', 'Prospect', 'StatusType', 'TaxRate', 'TradeType'
                )
            ) a
        ) = 17 then 'OK'
        else 'Mismatch'
      end, 'Messages must be present for all tables/transforms'
    union all
    select 'DImessages initial condition', NULL, case
        when (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.dimessages
          where BatchID = 0
            and MessageType = 'Validation'
            and MessageData <> '0'
        ) = 0 then 'OK'
        else 'Non-empty table in before Batch1'
      end, 'All DW tables must be empty before Batch1' -- Checks against the DimBroker table.
    union all
    select 'DimBroker row count', NULL, case
        when (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.DimBroker
        ) = (
          select Value
          from ${catalog}.${wh_db}_${scale_factor}.Audit
          where DataSet = 'DimBroker'
            and Attribute = 'HR_BROKERS'
        ) then 'OK'
        else 'Mismatch'
      end, 'Actual row count matches Audit table'
    union all
    select 'DimBroker distinct keys', NULL, case
        when (
          select count(distinct SK_BrokerID)
          from ${catalog}.${wh_db}_${scale_factor}.DimBroker
        ) = (
          select Value
          from ${catalog}.${wh_db}_${scale_factor}.Audit
          where DataSet = 'DimBroker'
            and Attribute = 'HR_BROKERS'
        ) then 'OK'
        else 'Not unique'
      end, 'All SKs are distinct'
    union all
    select 'DimBroker BatchID', 1, case
        when (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.DimBroker
          where BatchID <> 1
        ) = 0 then 'OK'
        else 'Not batch 1'
      end, 'All rows report BatchID = 1'
    union all
    select 'DimBroker IsCurrent', NULL, case
        when (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.DimBroker
          where ! IsCurrent
        ) = 0 then 'OK'
        else 'Not current'
      end, 'All rows have IsCurrent = 1'
    union all
    select 'DimBroker EffectiveDate', NULL, case
        when (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.DimBroker
          where EffectiveDate <> '1950-01-01'
        ) = 0 then 'OK'
        else 'Wrong date'
      end, 'All rows have Batch1 BatchDate as EffectiveDate'
    union all
    select 'DimBroker EndDate', NULL, case
        when (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.DimBroker
          where EndDate <> '9999-12-31'
        ) = 0 then 'OK'
        else 'Wrong date'
      end, 'All rows have end of time as EndDate' --
      -- Checks against the DimAccount table.
      --
    union all
    select 'DimAccount row count', 1, case
        when (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.DimAccount
          where BatchID = 1
        ) > (
          select Value
          from ${catalog}.${wh_db}_${scale_factor}.Audit
          where DataSet = 'DimCustomer'
            and Attribute = 'C_NEW'
            and BatchID = 1
        ) + (
          select Value
          from ${catalog}.${wh_db}_${scale_factor}.Audit
          where DataSet = 'DimAccount'
            and Attribute = 'CA_ADDACCT'
            and BatchID = 1
        ) + (
          select Value
          from ${catalog}.${wh_db}_${scale_factor}.Audit
          where DataSet = 'DimAccount'
            and Attribute = 'CA_CLOSEACCT'
            and BatchID = 1
        ) + (
          select Value
          from ${catalog}.${wh_db}_${scale_factor}.Audit
          where DataSet = 'DimAccount'
            and Attribute = 'CA_UPDACCT'
            and BatchID = 1
        ) + (
          select Value
          from ${catalog}.${wh_db}_${scale_factor}.Audit
          where DataSet = 'DimCustomer'
            and Attribute = 'C_UPDCUST'
            and BatchID = 1
        ) + (
          select Value
          from ${catalog}.${wh_db}_${scale_factor}.Audit
          where DataSet = 'DimCustomer'
            and Attribute = 'C_INACT'
            and BatchID = 1
        ) - (
          select Value
          from ${catalog}.${wh_db}_${scale_factor}.Audit
          where DataSet = 'DimCustomer'
            and Attribute = 'C_ID_HIST'
            and BatchID = 1
        ) - (
          select Value
          from ${catalog}.${wh_db}_${scale_factor}.Audit
          where DataSet = 'DimAccount'
            and Attribute = 'CA_ID_HIST'
            and BatchID = 1
        ) then 'OK'
        else 'Too few rows'
      end, 'Actual row count matches or exceeds Audit table minimum' -- had to change to sum(value) instead of just value because of correlated subquery issue
    union all
    select 'DimAccount row count', BatchID, Result, 'Actual row count matches or exceeds Audit table minimum'
    from (
        select distinct BatchID, (
            case
              when (
                select count(*)
                from ${catalog}.${wh_db}_${scale_factor}.DimAccount
                where BatchID = a.BatchID
              ) >= (
                select sum(Value)
                from ${catalog}.${wh_db}_${scale_factor}.Audit
                where DataSet = 'DimAccount'
                  and Attribute = 'CA_ADDACCT'
                  and BatchID = a.BatchID
              ) + (
                select sum(Value)
                from ${catalog}.${wh_db}_${scale_factor}.Audit
                where DataSet = 'DimAccount'
                  and Attribute = 'CA_CLOSEACCT'
                  and BatchID = a.BatchID
              ) + (
                select sum(Value)
                from ${catalog}.${wh_db}_${scale_factor}.Audit
                where DataSet = 'DimAccount'
                  and Attribute = 'CA_UPDACCT'
                  and BatchID = a.BatchID
              ) - (
                select sum(Value)
                from ${catalog}.${wh_db}_${scale_factor}.Audit
                where DataSet = 'DimAccount'
                  and Attribute = 'CA_ID_HIST'
                  and BatchID = a.BatchID
              ) then 'OK'
              else 'Too few rows'
            end
          ) as Result
        from ${catalog}.${wh_db}_${scale_factor}.Audit a
        where BatchID in (2, 3)
      ) o
    union all
    select 'DimAccount distinct keys', NULL, case
        when (
          select count(distinct SK_AccountID)
          from ${catalog}.${wh_db}_${scale_factor}.DimAccount
        ) = (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.DimAccount
        ) then 'OK'
        else 'Not unique'
      end, 'All SKs are distinct' 
      -- Three tests together check for validity of the EffectiveDate and EndDate handling:
      --   'DimAccount EndDate' checks that effective and end dates line up
      --   'DimAccount Overlap' checks that there are not records that overlap in time
      --   'DimAccount End of Time' checks that every company has a final record that goes to 9999-12-31
    union all
    select 'DimAccount EndDate', NULL, case
        when (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.DimAccount
        ) = (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.DimAccount a
            join ${catalog}.${wh_db}_${scale_factor}.DimAccount b on a.AccountID = b.AccountID
            and a.EndDate = b.EffectiveDate
        ) + (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.DimAccount
          where EndDate = '9999-12-31'
        ) then 'OK'
        else 'Dates not aligned'
      end, 'EndDate of one record matches EffectiveDate of another, or the end of time'
    union all
    select 'DimAccount Overlap', NULL, case
        when (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.DimAccount a
            join ${catalog}.${wh_db}_${scale_factor}.DimAccount b on a.AccountID = b.AccountID
            and a.SK_AccountID <> b.SK_AccountID
            and a.EffectiveDate >= b.EffectiveDate
            and a.EffectiveDate < b.EndDate
        ) = 0 then 'OK'
        else 'Dates overlap'
      end, 'Date ranges do not overlap for a given Account'
    union all
    select 'DimAccount End of Time', NULL, case
        when (
          select count(distinct AccountID)
          from ${catalog}.${wh_db}_${scale_factor}.DimAccount
        ) = (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.DimAccount
          where EndDate = '9999-12-31'
        ) then 'OK'
        else 'End of tome not reached'
      end, 'Every Account has one record with a date range reaching the end of time'
    union all
    select 'DimAccount consolidation', NULL, case
        when (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.DimAccount
          where EffectiveDate = EndDate
        ) = 0 then 'OK'
        else 'Not consolidated'
      end, 'No records become effective and end on the same day'
    union all
    select 'DimAccount batches', NULL, case
        when (
          select count(distinct BatchID)
          from ${catalog}.${wh_db}_${scale_factor}.DimAccount
        ) = 3
        and (
          select max(BatchID)
          from ${catalog}.${wh_db}_${scale_factor}.DimAccount
        ) = 3 then 'OK'
        else 'Mismatch'
      end, 'BatchID values must match Audit table'
    union all
    select 'DimAccount EffectiveDate', BatchID, Result, 'All records from a batch have an EffectiveDate in the batch time window'
    from (
        select distinct BatchID, (
            case
              when (
                select count(*)
                from (
                    select BatchID, Date, Attribute
                    from ${catalog}.${wh_db}_${scale_factor}.Audit
                    where DataSet = 'Batch'
                      and Attribute in ('FirstDay', 'LastDay')
                  ) aud
                  join ${catalog}.${wh_db}_${scale_factor}.DimAccount da ON da.batchid = aud.batchid
                  AND (
                    (
                      da.EffectiveDate < aud.date
                      AND Attribute = 'FirstDay'
                    )
                    OR (
                      da.EffectiveDate > aud.date
                      AND Attribute = 'LastDay'
                    )
                  )
                where da.batchid = a.batchid
                  AND aud.BatchID = a.BatchID
              ) = 0 then 'OK'
              else 'Data out of range - see ticket #71'
            end
          ) as Result
        from ${catalog}.${wh_db}_${scale_factor}.Audit a
        where BatchID in (1, 2, 3)
      ) o
    union all
    select 'DimAccount IsCurrent', NULL, case
        when (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.DimAccount
        ) = (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.DimAccount
          where EndDate = '9999-12-31'
            and IsCurrent
        ) + (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.DimAccount
          where EndDate < '9999-12-31'
            and ! IsCurrent
        ) then 'OK'
        else 'Not current'
      end, 'IsCurrent is 1 if EndDate is the end of time, else Iscurrent is 0'
    union all
    select 'DimAccount Status', NULL, case
        when (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.DimAccount
          where Status not in ('Active', 'Inactive')
        ) = 0 then 'OK'
        else 'Bad value'
      end, 'All Status values are valid'
    union all
    select 'DimAccount TaxStatus', NULL, case
        when (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.DimAccount
          where BatchID = 1
            and TaxStatus not in (0, 1, 2)
        ) = 0 then 'OK'
        else 'Bad value'
      end, 'All TaxStatus values are valid'
    union all
    select 'DimAccount SK_CustomerID', NULL, case
        when (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.DimAccount
        ) = (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.DimAccount a
            join ${catalog}.${wh_db}_${scale_factor}.DimCustomer c on a.SK_CustomerID = c.SK_CustomerID
            and c.EffectiveDate <= a.EffectiveDate
            and a.EndDate <= c.EndDate
        ) then 'OK'
        else 'Bad join'
      end, 'All SK_CustomerIDs match a DimCustomer record with a valid date range'
    union all
    select 'DimAccount SK_BrokerID', NULL, case
        when (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.DimAccount
        ) = (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.DimAccount a
            join ${catalog}.${wh_db}_${scale_factor}.DimBroker c on a.SK_BrokerID = c.SK_BrokerID
            and c.EffectiveDate <= a.EffectiveDate
            and a.EndDate <= c.EndDate
        ) then 'OK'
        else 'Bad join - spec problem with DimBroker EffectiveDate values'
      end, 'All SK_BrokerIDs match a broker record with a valid date range'
    union all
    select 'DimAccount inactive customers', NULL, case
        when (
          select count(*)
          from (
              select count(*)
              from (
                  select *
                  from ${catalog}.${wh_db}_${scale_factor}.DimCustomer
                  where Status = 'Inactive'
                ) c
                left join ${catalog}.${wh_db}_${scale_factor}.DimAccount a on a.SK_CustomerID = c.SK_CustomerID
              where a.Status = 'Inactive'
              group by
                c.SK_CustomerID
              having
                count(*) < 1
            ) z
        ) = 0 then 'OK'
        else 'Bad value'
      end, 'If a customer is inactive, the corresponding accounts must also have been inactive' --
    union all
    select 'DimCustomer row count', BatchID, Result, 'Actual row count matches or exceeds Audit table minimum'
    from (
        select distinct BatchID, case
            when (
              select count(*)
              from ${catalog}.${wh_db}_${scale_factor}.DimCustomer
              where BatchID = a.BatchID
            ) >= (
              select sum(Value)
              from ${catalog}.${wh_db}_${scale_factor}.Audit
              where DataSet = 'DimCustomer'
                and Attribute = 'C_NEW'
                and BatchID = a.BatchID
            ) + (
              select sum(Value)
              from ${catalog}.${wh_db}_${scale_factor}.Audit
              where DataSet = 'DimCustomer'
                and Attribute = 'C_INACT'
                and BatchID = a.BatchID
            ) + (
              select sum(Value)
              from ${catalog}.${wh_db}_${scale_factor}.Audit
              where DataSet = 'DimCustomer'
                and Attribute = 'C_UPDCUST'
                and BatchID = a.BatchID
            ) - (
              select sum(Value)
              from ${catalog}.${wh_db}_${scale_factor}.Audit
              where DataSet = 'DimCustomer'
                and Attribute = 'C_ID_HIST'
                and BatchID = a.BatchID
            ) then 'OK'
            else 'Too few rows'
          end as Result
        from ${catalog}.${wh_db}_${scale_factor}.Audit a
        where BatchID in (1, 2, 3)
      ) o
    union all
    select 'DimCustomer distinct keys', NULL, case
        when (
          select count(distinct SK_CustomerID)
          from ${catalog}.${wh_db}_${scale_factor}.DimCustomer
        ) = (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.DimCustomer
        ) then 'OK'
        else 'Not unique'
      end, 'All SKs are distinct' -- Three tests together check for validity of the EffectiveDate and EndDate handling:
      --   'DimCustomer EndDate' checks that effective and end dates line up
      --   'DimCustomer Overlap' checks that there are not records that overlap in time
      --   'DimCustomer End of Time' checks that every company has a final record that goes to 9999-12-31
    union all
    select 'DimCustomer EndDate', NULL, case
        when (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.DimCustomer
        ) = (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.DimCustomer a
            join ${catalog}.${wh_db}_${scale_factor}.DimCustomer b on a.CustomerID = b.CustomerID
            and a.EndDate = b.EffectiveDate
        ) + (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.DimCustomer
          where EndDate = '9999-12-31'
        ) then 'OK'
        else 'Dates not aligned'
      end, 'EndDate of one record matches EffectiveDate of another, or the end of time'
    union all
    select 'DimCustomer Overlap', NULL, case
        when (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.DimCustomer a
            join ${catalog}.${wh_db}_${scale_factor}.DimCustomer b on a.CustomerID = b.CustomerID
            and a.SK_CustomerID <> b.SK_CustomerID
            and a.EffectiveDate >= b.EffectiveDate
            and a.EffectiveDate < b.EndDate
        ) = 0 then 'OK'
        else 'Dates overlap'
      end, 'Date ranges do not overlap for a given Customer'
    union all
    select 'DimCustomer End of Time', NULL, case
        when (
          select count(distinct CustomerID)
          from ${catalog}.${wh_db}_${scale_factor}.DimCustomer
        ) = (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.DimCustomer
          where EndDate = '9999-12-31'
        ) then 'OK'
        else 'End of time not reached'
      end, 'Every Customer has one record with a date range reaching the end of time'
    union all
    select 'DimCustomer consolidation', NULL, case
        when (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.DimCustomer
          where EffectiveDate = EndDate
        ) = 0 then 'OK'
        else 'Not consolidated'
      end, 'No records become effective and end on the same day'
    union all
    select 'DimCustomer batches', NULL, case
        when (
          select count(distinct BatchID)
          from ${catalog}.${wh_db}_${scale_factor}.DimCustomer
        ) = 3
        and (
          select max(BatchID)
          from ${catalog}.${wh_db}_${scale_factor}.DimCustomer
        ) = 3 then 'OK'
        else 'Mismatch'
      end, 'BatchID values must match Audit table'
    union all
    select 'DimCustomer IsCurrent', NULL, case
        when (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.DimCustomer
        ) = (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.DimCustomer
          where EndDate = '9999-12-31'
            and IsCurrent
        ) + (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.DimCustomer
          where EndDate < '9999-12-31'
            and ! IsCurrent
        ) then 'OK'
        else 'Not current'
      end, 'IsCurrent is 1 if EndDate is the end of time, else Iscurrent is 0'
    union all
    select 'DimCustomer EffectiveDate', BatchID, Result, 'All records from a batch have an EffectiveDate in the batch time window'
    from (
        select distinct BatchID, (
            case
              when (
                select count(*)
                from (
                    select BatchID, Date, Attribute
                    from ${catalog}.${wh_db}_${scale_factor}.Audit
                    where DataSet = 'Batch'
                      and Attribute in ('FirstDay', 'LastDay')
                  ) aud
                  join ${catalog}.${wh_db}_${scale_factor}.DimCustomer dc ON dc.batchid = aud.batchid
                  AND (
                    (
                      dc.EffectiveDate < aud.date
                      AND Attribute = 'FirstDay'
                    )
                    OR (
                      dc.EffectiveDate > aud.date
                      AND Attribute = 'LastDay'
                    )
                  )
                where dc.batchid = a.batchid
                  AND aud.BatchID = a.BatchID
              ) = 0 then 'OK'
              else 'Data out of range'
            end
          ) as Result
        from ${catalog}.${wh_db}_${scale_factor}.Audit a
        where BatchID in (1, 2, 3)
      ) o
    union all
    select 'DimCustomer Status', NULL, case
        when (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.DimCustomer
          where Status not in ('Active', 'Inactive')
        ) = 0 then 'OK'
        else 'Bad value'
      end, 'All Status values are valid'
    union all
    select 'DimCustomer inactive customers', BatchID, Result, 'Inactive customer count matches Audit table'
    from (
        select distinct BatchID, case
            when (
              select sum(MessageData)
              from ${catalog}.${wh_db}_${scale_factor}.dimessages
              where MessageSource = 'DimCustomer'
                and MessageType = 'Validation'
                and MessageText = 'Inactive customers'
                and BatchID = a.BatchID
            ) = (
              select sum(Audit_batch_total)
              from (
                  select batchid, sum(sum(Value)) over (
                      order by
                        batchid
                    ) as Audit_batch_total
                  from ${catalog}.${wh_db}_${scale_factor}.Audit
                  where BatchID in (1, 2, 3)
                    and DataSet = 'DimCustomer'
                    and Attribute = 'C_INACT'
                  group by
                    batchid
                )
              where BatchID = a.BatchID
            ) then 'OK'
            else 'Mismatch'
          end as Result
        from ${catalog}.${wh_db}_${scale_factor}.Audit a
        where BatchID in (1, 2, 3)
      ) o
    union all
    select 'DimCustomer Gender', NULL, case
        when (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.DimCustomer
          where Gender not in ('M', 'F', 'U')
        ) = 0 then 'OK'
        else 'Bad value'
      end, 'All Gender values are valid'
    union all
      --adding sum for value
    select 'DimCustomer age range alerts', BatchID, Result, 'Count of age range alerts matches Audit table'
    from (
        select distinct BatchID, case
            when (
              select count(*)
              from ${catalog}.${wh_db}_${scale_factor}.dimessages
              where MessageType = 'Alert'
                and BatchID = a.BatchID
                and MessageText = 'DOB out of range'
            ) = (
              select sum(Value)
              from ${catalog}.${wh_db}_${scale_factor}.Audit
              where DataSet = 'DimCustomer'
                and BatchID = a.BatchID
                and Attribute = 'C_DOB_TO'
            ) + (
              select sum(Value)
              from ${catalog}.${wh_db}_${scale_factor}.Audit
              where DataSet = 'DimCustomer'
                and BatchID = a.BatchID
                and Attribute = 'C_DOB_TY'
            ) then 'OK'
            else 'Mismatch'
          end as Result
        from ${catalog}.${wh_db}_${scale_factor}.Audit a
        where BatchID in (1, 2, 3)
      ) o
    union all
      --adding sum for value
    select 'DimCustomer customer tier alerts', BatchID, Result, 'Count of customer tier alerts matches Audit table'
    from (
        select distinct BatchID, case
            when (
              select count(*)
              from ${catalog}.${wh_db}_${scale_factor}.dimessages
              where MessageType = 'Alert'
                and BatchID = a.BatchID
                and MessageText = 'Invalid customer tier'
            ) = (
              select sum(Value)
              from ${catalog}.${wh_db}_${scale_factor}.Audit
              where DataSet = 'DimCustomer'
                and BatchID = a.BatchID
                and Attribute = 'C_TIER_INV'
            ) then 'OK'
            else 'Mismatch'
          end as Result
        from ${catalog}.${wh_db}_${scale_factor}.Audit a
        where BatchID in (1, 2, 3)
      ) o
    union all
    select 'DimCustomer TaxID', NULL, case
        when (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.DimCustomer
          where TaxID not like '___-__-____'
        ) = 0 then 'OK'
        else 'Mismatch'
      end, 'TaxID values are properly formatted'
    union all
    select 'DimCustomer Phone1', NULL, case
        when (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.DimCustomer
          where Phone1 not like '+1 (___) ___-____%'
            and Phone1 not like '(___) ___-____%'
            and Phone1 not like '___-____%'
            and Phone1 <> ''
            and Phone1 is not null
        ) = 0 then 'OK'
        else 'Mismatch'
      end, 'Phone1 values are properly formatted'
    union all
    select 'DimCustomer Phone2', NULL, case
        when (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.DimCustomer
          where Phone2 not like '+1 (___) ___-____%'
            and Phone2 not like '(___) ___-____%'
            and Phone2 not like '___-____%'
            and Phone2 <> ''
            and Phone2 is not null
        ) = 0 then 'OK'
        else 'Mismatch'
      end, 'Phone2 values are properly formatted'
    union all
    select 'DimCustomer Phone3', NULL, case
        when (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.DimCustomer
          where Phone3 not like '+1 (___) ___-____%'
            and Phone3 not like '(___) ___-____%'
            and Phone3 not like '___-____%'
            and Phone3 <> ''
            and Phone3 is not null
        ) = 0 then 'OK'
        else 'Mismatch'
      end, 'Phone3 values are properly formatted'
    union all
    select 'DimCustomer Email1', NULL, case
        when (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.DimCustomer
          where Email1 not like '_%.%@%.%'
            and Email1 is not null
        ) = 0 then 'OK'
        else 'Mismatch'
      end, 'Email1 values are properly formatted'
    union all
    select 'DimCustomer Email2', NULL, case
        when (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.DimCustomer
          where Email2 not like '_%.%@%.%'
            and Email2 <> ''
            and Email2 is not null
        ) = 0 then 'OK'
        else 'Mismatch'
      end, 'Email2 values are properly formatted'
    union all
    select 'DimCustomer LocalTaxRate', NULL, case
        when (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.DimCustomer
        ) = (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.DimCustomer c
            join ${catalog}.${wh_db}_${scale_factor}.taxrate t on c.LocalTaxRateDesc = t.TX_NAME
            and c.LocalTaxRate = t.TX_RATE
        )
        and (
          select count(distinct LocalTaxRateDesc)
          from ${catalog}.${wh_db}_${scale_factor}.DimCustomer
        ) > 300 then 'OK'
        else 'Mismatch'
      end, 'LocalTaxRateDesc and LocalTaxRate values are from TaxRate table'
    union all
    select 'DimCustomer NationalTaxRate', NULL, case
        when (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.DimCustomer
        ) = (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.DimCustomer c
            join ${catalog}.${wh_db}_${scale_factor}.taxrate t on c.NationalTaxRateDesc = t.TX_NAME
            and c.NationalTaxRate = t.TX_RATE
        )
        and (
          select count(distinct NationalTaxRateDesc)
          from ${catalog}.${wh_db}_${scale_factor}.DimCustomer
        ) >= 9 -- Including the inequality for now, because the generated data is not sticking to national tax rates
        then 'OK'
        else 'Mismatch'
      end, 'NationalTaxRateDesc and NationalTaxRate values are from TaxRate table'
    union all
    select 'DimCustomer demographic fields', NULL, case
        when (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.DimCustomer c
            join ${catalog}.${wh_db}_${scale_factor}.Prospect p on upper(
              c.FirstName || c.LastName || c.AddressLine1 || COALESCE(c.AddressLine2, '') || c.PostalCode
            ) = upper(
              p.FirstName || p.LastName || p.AddressLine1 || COALESCE(p.AddressLine2, '') || p.PostalCode
            )
            and COALESCE(c.CreditRating, 0) = COALESCE(p.CreditRating, 0)
            and COALESCE(c.NetWorth, 0) = COALESCE(p.NetWorth, 0)
            and COALESCE(c.MarketingNameplate, '') = COALESCE(p.MarketingNameplate, '')
            and c.IsCurrent
        ) = (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.DimCustomer
          where AgencyID is not null
            and IsCurrent
        ) then 'OK'
        else 'Mismatch'
      end, 'For current customer records that match Prospect records, the demographic fields also match' --
    -- Checks against the DimSecurity table.
    union all
    select 'DimSecurity row count', BatchID, Result, 'Actual row count matches or exceeds Audit table minimum'
    from (
        select distinct BatchID, case
            when cast(
              (
                select sum(MessageData)
                from ${catalog}.${wh_db}_${scale_factor}.dimessages
                where MessageType = 'Validation'
                  and MessageSource = 'DimSecurity'
                  and MessageText = 'Row count'
                  and BatchID = a.BatchID
              ) as bigint
            ) >= (
              select sum(Value)
              from ${catalog}.${wh_db}_${scale_factor}.Audit
              where DataSet = 'DimSecurity'
                and Attribute = 'FW_SEC'
                and BatchID = a.BatchID
            ) -- This one was simpler since BatchID in (1) means only one batchid to compare to
            then 'OK'
            else 'Too few rows'
          end as Result
        from ${catalog}.${wh_db}_${scale_factor}.Audit a
        where BatchID in (1)
      ) o
    union all
    select 'DimSecurity distinct keys', NULL, case
        when (
          select count(distinct SK_SecurityID)
          from ${catalog}.${wh_db}_${scale_factor}.DimSecurity
        ) = (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.DimSecurity
        ) then 'OK'
        else 'Not unique'
      end, 'All SKs are distinct' -- Three tests together check for validity of the EffectiveDate and EndDate handling:
      --   'DimSecurity EndDate' checks that effective and end dates line up
      --   'DimSecurity Overlap' checks that there are not records that overlap in time
      --   'DimSecurity End of Time' checks that every company has a final record that goes to 9999-12-31
    union all
    select 'DimSecurity EndDate', NULL, case
        when (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.DimSecurity
        ) = (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.DimSecurity a
            join ${catalog}.${wh_db}_${scale_factor}.DimSecurity b on a.Symbol = b.Symbol
            and a.EndDate = b.EffectiveDate
        ) + (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.DimSecurity
          where EndDate = '9999-12-31'
        ) then 'OK'
        else 'Dates not aligned'
      end, 'EndDate of one record matches EffectiveDate of another, or the end of time'
    union all
    select 'DimSecurity Overlap', NULL, case
        when (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.DimSecurity a
            join ${catalog}.${wh_db}_${scale_factor}.DimSecurity b on a.Symbol = b.Symbol
            and a.SK_SecurityID <> b.SK_SecurityID
            and a.EffectiveDate >= b.EffectiveDate
            and a.EffectiveDate < b.EndDate
        ) = 0 then 'OK'
        else 'Dates overlap'
      end, 'Date ranges do not overlap for a given company'
    union all
    select 'DimSecurity End of Time', NULL, case
        when (
          select count(distinct Symbol)
          from ${catalog}.${wh_db}_${scale_factor}.DimSecurity
        ) = (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.DimSecurity
          where EndDate = '9999-12-31'
        ) then 'OK'
        else 'End of tome not reached'
      end, 'Every company has one record with a date range reaching the end of time'
    union all
    select 'DimSecurity consolidation', NULL, case
        when (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.DimSecurity
          where EffectiveDate = EndDate
        ) = 0 then 'OK'
        else 'Not consolidated'
      end, 'No records become effective and end on the same day'
    union all
    select 'DimSecurity batches', NULL, case
        when (
          select count(distinct BatchID)
          from ${catalog}.${wh_db}_${scale_factor}.DimSecurity
        ) = 1
        and (
          select max(BatchID)
          from ${catalog}.${wh_db}_${scale_factor}.DimSecurity
        ) = 1 then 'OK'
        else 'Mismatch'
      end, 'BatchID values must match Audit table'
    union all
    select 'DimSecurity IsCurrent', NULL, case
        when (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.DimSecurity
        ) = (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.DimSecurity
          where EndDate = '9999-12-31'
            and IsCurrent
        ) + (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.DimSecurity
          where EndDate < '9999-12-31'
            and ! IsCurrent
        ) then 'OK'
        else 'Not current'
      end, 'IsCurrent is 1 if EndDate is the end of time, else Iscurrent is 0'
    union all
    select 'DimSecurity EffectiveDate', BatchID, Result, 'All records from a batch have an EffectiveDate in the batch time window'
    from (
        select distinct BatchID, (
            case
              when (
                select count(*)
                from (
                    select BatchID, Date, Attribute
                    from ${catalog}.${wh_db}_${scale_factor}.Audit
                    where DataSet = 'Batch'
                      and Attribute in ('FirstDay', 'LastDay')
                  ) aud
                  join ${catalog}.${wh_db}_${scale_factor}.DimSecurity dc ON dc.batchid = aud.batchid
                  AND (
                    (
                      dc.EffectiveDate < aud.date
                      AND Attribute = 'FirstDay'
                    )
                    OR (
                      dc.EffectiveDate > aud.date
                      AND Attribute = 'LastDay'
                    )
                  )
                where dc.batchid = a.batchid
                  AND aud.BatchID = a.BatchID
              ) = 0 then 'OK'
              else 'Data out of range'
            end
          ) as Result
        from ${catalog}.${wh_db}_${scale_factor}.Audit a
        where BatchID in (1, 2, 3)
      ) o
    union all
    select 'DimSecurity Status', NULL, case
        when (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.DimSecurity
          where Status not in ('Active', 'Inactive')
        ) = 0 then 'OK'
        else 'Bad value'
      end, 'All Status values are valid'
    union all
    select 'DimSecurity SK_CompanyID', NULL, case
        when (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.DimSecurity
        ) = (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.DimSecurity a
            join ${catalog}.${wh_db}_${scale_factor}.DimCompany c on a.SK_CompanyID = c.SK_CompanyID
            and c.EffectiveDate <= a.EffectiveDate
            and a.EndDate <= c.EndDate
        ) then 'OK'
        else 'Bad join'
      end, 'All SK_CompanyIDs match a DimCompany record with a valid date range'
    union all
    select 'DimSecurity ExchangeID', NULL, case
        when (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.DimSecurity
          where ExchangeID not in ('NYSE', 'NASDAQ', 'AMEX', 'PCX')
        ) = 0 then 'OK'
        else 'Bad value - see ticket #65'
      end, 'All ExchangeID values are valid'
    union all
    select 'DimSecurity Issue', NULL, case
        when (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.DimSecurity
          where Issue not in ('COMMON', 'PREF_A', 'PREF_B', 'PREF_C', 'PREF_D')
        ) = 0 then 'OK'
        else 'Bad value - see ticket #65'
      end, 'All Issue values are valid' 
    -- Checks against the DimCompany table.
    union all
    select 'DimCompany row count', BatchID, Result, 'Actual row count matches or exceeds Audit table minimum'
    from (
        select distinct BatchID, case
            when --added sum(messagedata)
            cast(
              (
                select sum(MessageData)
                from ${catalog}.${wh_db}_${scale_factor}.dimessages
                where MessageType = 'Validation'
                  and BatchID = a.BatchID
                  and MessageSource = 'DimCompany'
                  and MessageText = 'Row count'
              ) as bigint
            ) <=
            (
              select sum(Value)
              from ${catalog}.${wh_db}_${scale_factor}.Audit a
              where DataSet = 'DimCompany'
                and Attribute = 'FW_CMP'
                and BatchID <= a.BatchID
            ) then 'OK'
            else 'Too few rows'
          end as Result
        from ${catalog}.${wh_db}_${scale_factor}.Audit a
        where BatchID in (1, 2, 3)
      ) o
    union all
    select 'DimCompany distinct keys', NULL, case
        when (
          select count(distinct SK_CompanyID)
          from ${catalog}.${wh_db}_${scale_factor}.DimCompany
        ) = (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.DimCompany
        ) then 'OK'
        else 'Not unique'
      end, 'All SKs are distinct' 
      -- Three tests together check for validity of the EffectiveDate and EndDate handling:
      --   'DimCompany EndDate' checks that effective and end dates line up
      --   'DimCompany Overlap' checks that there are not records that overlap in time
      --   'DimCompany End of Time' checks that every company has a final record that goes to 9999-12-31
    union all
    select 'DimCompany EndDate', NULL, case
        when (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.DimCompany
        ) = (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.DimCompany a
            join ${catalog}.${wh_db}_${scale_factor}.DimCompany b on a.CompanyID = b.CompanyID
            and a.EndDate = b.EffectiveDate
        ) + (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.DimCompany
          where EndDate = '9999-12-31'
        ) then 'OK'
        else 'Dates not aligned'
      end, 'EndDate of one record matches EffectiveDate of another, or the end of time'
    union all
    select 'DimCompany Overlap', NULL, case
        when (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.DimCompany a
            join ${catalog}.${wh_db}_${scale_factor}.DimCompany b on a.CompanyID = b.CompanyID
            and a.SK_CompanyID <> b.SK_CompanyID
            and a.EffectiveDate >= b.EffectiveDate
            and a.EffectiveDate < b.EndDate
        ) = 0 then 'OK'
        else 'Dates overlap'
      end, 'Date ranges do not overlap for a given company'
    union all
    select 'DimCompany End of Time', NULL, case
        when (
          select count(distinct CompanyID)
          from ${catalog}.${wh_db}_${scale_factor}.DimCompany
        ) = (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.DimCompany
          where EndDate = '9999-12-31'
        ) then 'OK'
        else 'End of tome not reached'
      end, 'Every company has one record with a date range reaching the end of time'
    union all
    select 'DimCompany consolidation', NULL, case
        when (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.DimCompany
          where EffectiveDate = EndDate
        ) = 0 then 'OK'
        else 'Not consolidated'
      end, 'No records become effective and end on the same day'
    union all
    select 'DimCompany batches', NULL, case
        when (
          select count(distinct BatchID)
          from ${catalog}.${wh_db}_${scale_factor}.DimCompany
        ) = 1
        and (
          select max(BatchID)
          from ${catalog}.${wh_db}_${scale_factor}.DimCompany
        ) = 1 then 'OK'
        else 'Mismatch'
      end, 'BatchID values must match Audit table'
    union all
    select 'DimCompany EffectiveDate', BatchID, Result, 'All records from a batch have an EffectiveDate in the batch time window'
    from (
        select distinct BatchID, (
            case
              when (
                select count(*)
                from (
                    select BatchID, Date, Attribute
                    from ${catalog}.${wh_db}_${scale_factor}.Audit
                    where DataSet = 'Batch'
                      and Attribute in ('FirstDay', 'LastDay')
                  ) aud
                  join ${catalog}.${wh_db}_${scale_factor}.DimCompany dc ON dc.batchid = aud.batchid
                  AND (
                    (
                      dc.EffectiveDate < aud.date
                      AND Attribute = 'FirstDay'
                    )
                    OR (
                      dc.EffectiveDate > aud.date
                      AND Attribute = 'LastDay'
                    )
                  )
                where dc.batchid = a.batchid
                  AND aud.BatchID = a.BatchID
              ) = 0 then 'OK'
              else 'Data out of range'
            end
          ) as Result
        from ${catalog}.${wh_db}_${scale_factor}.Audit a
        where BatchID in (1, 2, 3)
      ) o
    union all
    select 'DimCompany Status', NULL, case
        when (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.DimCompany
          where Status not in ('Active', 'Inactive')
        ) = 0 then 'OK'
        else 'Bad value'
      end, 'All Status values are valid'
    union all
    select 'DimCompany distinct names', NULL, case
        when (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.DimCompany a
            join ${catalog}.${wh_db}_${scale_factor}.DimCompany b on a.Name = b.Name
            and a.CompanyID <> b.CompanyID
        ) = 0 then 'OK'
        else 'Mismatch'
      end, 'Every company has a unique name'
    union all
      -- Curious, there are duplicate industry names in Industry table.  Should there be?  That's why the distinct stuff...
    select 'DimCompany Industry', NULL, case
        when (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.DimCompany
        ) = (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.DimCompany
          where Industry in (
              select distinct IN_NAME
              from ${catalog}.${wh_db}_${scale_factor}.industry
            )
        ) then 'OK'
        else 'Bad value'
      end, 'Industry values are from the Industry table'
    union all
    select 'DimCompany SPrating', NULL, case
        when (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.DimCompany
          where SPrating not in (
              'AAA', 'AA', 'A', 'BBB', 'BB', 'B', 'CCC', 'CC', 'C', 'D', 'AA+', 'A+', 'BBB+', 'BB+', 'B+', 'CCC+', 'AA-', 'A-', 'BBB-', 'BB-', 'B-', 'CCC-'
            )
            and SPrating is not null
        ) = 0 then 'OK'
        else 'Bad value'
      end, 'All SPrating values are valid'
    union all
      -- Right now we have blank (but not null) country names.  Should there be?
    select 'DimCompany Country', NULL, case
        when (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.DimCompany
          where Country not in ('Canada', 'United States of America', '')
            and Country is not null
        ) = 0 then 'OK'
        else 'Bad value'
      end, 'All Country values are valid' 
    -- Checks against the Prospect table.
    union all
    select 'Prospect SK_UpdateDateID', NULL, case
        when (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.Prospect
          where SK_RecordDateID < SK_UpdateDateID
        ) = 0 then 'OK'
        else 'Mismatch'
      end, 'SK_RecordDateID must be newer or same as SK_UpdateDateID'
    union all
    select 'Prospect SK_RecordDateID', BatchID, Result, 'All records from batch have SK_RecordDateID in or after the batch time window'
    from (
        select distinct BatchID, (
            case
              when (
                select count(*)
                from ${catalog}.${wh_db}_${scale_factor}.Prospect p
                  join ${catalog}.${wh_db}_${scale_factor}.DimDate on SK_DateId = P.SK_RecordDateID
                  join ${catalog}.${wh_db}_${scale_factor}.Audit _A on DataSet = 'Batch'
                  and Attribute = 'FirstDay'
                  and DateValue < Date
                  and P.BatchID = _A.BatchID
              ) = 0 then 'OK'
              else 'Mismatch'
            end
          ) as Result
        from ${catalog}.${wh_db}_${scale_factor}.Audit a
        where BatchID in (1, 2, 3)
      ) o
    union all
    select 'Prospect batches', NULL, case
        when (
          select count(distinct BatchID)
          from ${catalog}.${wh_db}_${scale_factor}.Prospect
        ) = 3
        and (
          select max(BatchID)
          from ${catalog}.${wh_db}_${scale_factor}.Prospect
        ) = 3 then 'OK'
        else 'Mismatch'
      end, 'BatchID values must match Audit table'
    union all
    select 'Prospect Country', NULL, case
        when (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.Prospect
          where Country not in ('Canada', 'United States of America') -- For the tiny sample data it would be ( 'CANADA', 'USA' )
            and Country is not null
        ) = 0 then 'OK'
        else 'Bad value'
      end, 'All Country values are valid'
    union all
    select 'Prospect MarketingNameplate', NULL, case
        when (
          select sum(
              case
                when (
                  COALESCE(NetWorth, 0) > 1000000
                  or COALESCE(Income, 0) > 200000
                )
                and MarketingNameplate not like '%HighValue%' then 1
                else 0
              end
            ) + sum(
              case
                when (
                  COALESCE(NumberChildren, 0) > 3
                  or COALESCE(NumberCreditCards, 0) > 5
                )
                and MarketingNameplate not like '%Expenses%' then 1
                else 0
              end
            ) + sum(
              case
                when (COALESCE(Age, 0) > 45)
                and MarketingNameplate not like '%Boomer%' then 1
                else 0
              end
            ) + sum(
              case
                when (
                  COALESCE(Income, 50000) < 50000
                  or COALESCE(CreditRating, 600) < 600
                  or COALESCE(NetWorth, 100000) < 100000
                )
                and MarketingNameplate not like '%MoneyAlert%' then 1
                else 0
              end
            ) + sum(
              case
                when (
                  COALESCE(NumberCars, 0) > 3
                  or COALESCE(NumberCreditCards, 0) > 7
                )
                and MarketingNameplate not like '%Spender%' then 1
                else 0
              end
            ) + sum(
              case
                when (
                  COALESCE(Age, 25) < 25
                  and COALESCE(NetWorth, 0) > 1000000
                )
                and MarketingNameplate not like '%Inherited%' then 1
                else 0
              end
            ) + sum(
              case
                when COALESCE(MarketingNameplate, '') not in (
                  -- Technically, a few of these combinations cannot really happen
                  '', 'HighValue', 'Expenses', 'HighValue+Expenses', 'Boomer', 'HighValue+Boomer', 'Expenses+Boomer', 'HighValue+Expenses+Boomer', 'MoneyAlert', 'HighValue+MoneyAlert', 'Expenses+MoneyAlert', 'HighValue+Expenses+MoneyAlert', 'Boomer+MoneyAlert', 'HighValue+Boomer+MoneyAlert', 'Expenses+Boomer+MoneyAlert', 'HighValue+Expenses+Boomer+MoneyAlert', 'Spender', 'HighValue+Spender', 'Expenses+Spender', 'HighValue+Expenses+Spender', 'Boomer+Spender', 'HighValue+Boomer+Spender', 'Expenses+Boomer+Spender', 'HighValue+Expenses+Boomer+Spender', 'MoneyAlert+Spender', 'HighValue+MoneyAlert+Spender', 'Expenses+MoneyAlert+Spender', 'HighValue+Expenses+MoneyAlert+Spender', 'Boomer+MoneyAlert+Spender', 'HighValue+Boomer+MoneyAlert+Spender', 'Expenses+Boomer+MoneyAlert+Spender', 'HighValue+Expenses+Boomer+MoneyAlert+Spender', 'Inherited', 'HighValue+Inherited', 'Expenses+Inherited', 'HighValue+Expenses+Inherited', 'Boomer+Inherited', 'HighValue+Boomer+Inherited', 'Expenses+Boomer+Inherited', 'HighValue+Expenses+Boomer+Inherited', 'MoneyAlert+Inherited', 'HighValue+MoneyAlert+Inherited', 'Expenses+MoneyAlert+Inherited', 'HighValue+Expenses+MoneyAlert+Inherited', 'Boomer+MoneyAlert+Inherited', 'HighValue+Boomer+MoneyAlert+Inherited', 'Expenses+Boomer+MoneyAlert+Inherited', 'HighValue+Expenses+Boomer+MoneyAlert+Inherited', 'Spender+Inherited', 'HighValue+Spender+Inherited', 'Expenses+Spender+Inherited', 'HighValue+Expenses+Spender+Inherited', 'Boomer+Spender+Inherited', 'HighValue+Boomer+Spender+Inherited', 'Expenses+Boomer+Spender+Inherited', 'HighValue+Expenses+Boomer+Spender+Inherited', 'MoneyAlert+Spender+Inherited', 'HighValue+MoneyAlert+Spender+Inherited', 'Expenses+MoneyAlert+Spender+Inherited', 'HighValue+Expenses+MoneyAlert+Spender+Inherited', 'Boomer+MoneyAlert+Spender+Inherited', 'HighValue+Boomer+MoneyAlert+Spender+Inherited', 'Expenses+Boomer+MoneyAlert+Spender+Inherited', 'HighValue+Expenses+Boomer+MoneyAlert+Spender+Inherited'
                ) then 1
                else 0
              end
            )
          from ${catalog}.${wh_db}_${scale_factor}.Prospect
        ) = 0 then 'OK'
        else 'Bad value'
      end, 'All MarketingNameplate values match the data'
    -- Checks against the FactWatches table.
    union all
    select 'FactWatches row count', BatchID, Result, 'Actual row count matches Audit table'
    from (
        select distinct BatchID, (
            case
              when --add sum message data and sum value
              cast(
                (
                  select sum(MessageData)
                  from ${catalog}.${wh_db}_${scale_factor}.dimessages
                  where MessageSource = 'FactWatches'
                    and MessageType = 'Validation'
                    and MessageText = 'Row count'
                    and BatchID = a.BatchID
                ) as int
              ) - cast(
                (
                  select sum(MessageData)
                  from ${catalog}.${wh_db}_${scale_factor}.dimessages
                  where MessageSource = 'FactWatches'
                    and MessageType = 'Validation'
                    and MessageText = 'Row count'
                    and BatchID = a.BatchID -1
                ) as int
              ) = (
                select sum(Value)
                from ${catalog}.${wh_db}_${scale_factor}.Audit
                where DataSet = 'FactWatches'
                  and Attribute = 'WH_ACTIVE'
                  and BatchID = a.BatchID
              ) then 'OK'
              else 'Mismatch'
            end
          ) as Result
        from ${catalog}.${wh_db}_${scale_factor}.Audit a
        where BatchID in (1, 2, 3)
      ) o
    union all
    select 'FactWatches batches', NULL, case
        when (
          select count(distinct BatchID)
          from ${catalog}.${wh_db}_${scale_factor}.FactWatches
        ) = 3
        and (
          select max(BatchID)
          from ${catalog}.${wh_db}_${scale_factor}.FactWatches
        ) = 3 then 'OK'
        else 'Mismatch'
      end, 'BatchID values must match Audit table'
    union all
    select 'FactWatches active watches', BatchID, Result, 'Actual total matches Audit table'
    from (
        select distinct BatchID, case
            when (
              select cast(sum(MessageData) as bigint)
              from ${catalog}.${wh_db}_${scale_factor}.dimessages
              where MessageSource = 'FactWatches'
                and MessageType = 'Validation'
                and MessageText = 'Row count'
                and BatchID = a.BatchID
            ) + (
              select cast(sum(MessageData) as bigint)
              from ${catalog}.${wh_db}_${scale_factor}.dimessages
              where MessageSource = 'FactWatches'
                and MessageType = 'Validation'
                and MessageText = 'Inactive watches'
                and BatchID = a.BatchID
            ) = (
              select sum(Audit_batch_total)
              from (
                  select batchid, sum(sum(Value)) over (
                      order by
                        batchid
                    ) as Audit_batch_total
                  from ${catalog}.${wh_db}_${scale_factor}.Audit
                  where BatchID in (1, 2, 3)
                    and DataSet = 'FactWatches'
                    and Attribute = 'WH_RECORDS'
                  group by
                    batchid
                )
              where BatchID = a.BatchID
            ) then 'OK'
            else 'Mismatch'
          end as Result
        from ${catalog}.${wh_db}_${scale_factor}.Audit a
        where BatchID in (1, 2, 3)
      ) o
    union all
    select 'FactWatches SK_CustomerID', NULL, case
        when (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.FactWatches
        ) = (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.FactWatches a
            join ${catalog}.${wh_db}_${scale_factor}.DimDate _d on _d.SK_DateID = A.SK_DateID_DatePlaced
            join ${catalog}.${wh_db}_${scale_factor}.DimCustomer c on a.SK_CustomerID = c.SK_CustomerID
            and c.EffectiveDate <= _d.DateValue --(select DateValue from ${catalog}.${wh_db}_${scale_factor}.DimDate where SK_DateID = a.SK_DateID_DatePlaced)
            and _d.DateValue <= c.EndDate
        ) --(select DateValue from ${catalog}.${wh_db}_${scale_factor}.DimDate where SK_DateID = a.SK_DateID_DatePlaced) <= c.EndDate )
        then 'OK'
        else 'Bad join'
      end, 'All SK_CustomerIDs match a DimCustomer record with a valid date range'
    union all
    select 'FactWatches SK_SecurityID', NULL, case
        when (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.FactWatches
        ) = (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.FactWatches a
            join ${catalog}.${wh_db}_${scale_factor}.DimDate _d on _d.SK_DateID = a.SK_DateID_DatePlaced
            join ${catalog}.${wh_db}_${scale_factor}.DimSecurity c on a.SK_SecurityID = c.SK_SecurityID
            and c.EffectiveDate <= _d.DateValue --(select DateValue from ${catalog}.${wh_db}_${scale_factor}.DimDate where SK_DateID = a.SK_DateID_DatePlaced)
            and _d.DateValue <= c.EndDate
        ) --(select DateValue from ${catalog}.${wh_db}_${scale_factor}.DimDate where SK_DateID = a.SK_DateID_DatePlaced) <= c.EndDate )
        then 'OK'
        else 'Bad join'
      end, 'All SK_SecurityIDs match a DimSecurity record with a valid date range'
    union all
    select 'FactWatches date check', BatchID, Result, 'All SK_DateID_ values are in the correct batch time window'
    from (
        select distinct BatchID, (
            case
              when (
                select count(*)
                from ${catalog}.${wh_db}_${scale_factor}.FactWatches w
                where w.BatchID = a.BatchID
                  and (
                    w.SK_DateID_DateRemoved is null
                    and (
                      (
                        select first(DateValue)
                        from ${catalog}.${wh_db}_${scale_factor}.DimDate
                        where   SK_DateID = w.SK_DateID_DatePlaced
                      ) > (
                        select first(Date)
                        from ${catalog}.${wh_db}_${scale_factor}.Audit
                        where   DataSet = 'Batch'
                          and Attribute = 'LastDay'
                          and BatchID = w.BatchID
                      )
                      or (
                        select first(DateValue)
                        from ${catalog}.${wh_db}_${scale_factor}.DimDate
                        where   SK_DateID = w.SK_DateID_DatePlaced
                      ) < (
                        select first(Date)
                        from ${catalog}.${wh_db}_${scale_factor}.Audit
                        where   DataSet = 'Batch'
                          and Attribute = 'FirstDay'
                          and BatchID = w.BatchID
                      )
                    )
                    or w.SK_DateID_DateRemoved is not null
                    and (
                      (
                        select first(DateValue)
                        from ${catalog}.${wh_db}_${scale_factor}.DimDate
                        where   SK_DateID = w.SK_DateID_DateRemoved
                      ) > (
                        select first(Date)
                        from ${catalog}.${wh_db}_${scale_factor}.Audit
                        where   DataSet = 'Batch'
                          and Attribute = 'LastDay'
                          and BatchID = w.BatchID
                      )
                      or (
                        select first(DateValue)
                        from ${catalog}.${wh_db}_${scale_factor}.DimDate
                        where   SK_DateID = w.SK_DateID_DateRemoved
                      ) < (
                        select first(Date)
                        from ${catalog}.${wh_db}_${scale_factor}.Audit
                        where   DataSet = 'Batch'
                          and Attribute = 'FirstDay'
                          and BatchID = w.BatchID
                      )
                      or SK_DateID_DatePlaced > SK_DateID_DateRemoved
                    )
                  )
              ) = 0 then 'OK'
              else 'Mismatch'
            end
          ) as Result
        from ${catalog}.${wh_db}_${scale_factor}.Audit a
        where BatchID in (1, 2, 3)
      ) o 
    -- Checks against the DimTrade table.
    union all
    select 'DimTrade row count', BatchID, Result, 'Actual total matches Audit table'
    from (
        select distinct BatchID, case
            when (
              select sum(MessageData)
              from ${catalog}.${wh_db}_${scale_factor}.dimessages
              where MessageSource = 'DimTrade'
                and MessageType = 'Validation'
                and MessageText = 'Row count'
                and BatchID = a.BatchID
            ) = (
              select sum(Audit_batch_total)
              from (
                  select batchid, sum(sum(Value)) over (
                      order by
                        batchid
                    ) as Audit_batch_total
                  from ${catalog}.${wh_db}_${scale_factor}.Audit
                  where BatchID in (1, 2, 3)
                    and DataSet = 'DimTrade'
                    and Attribute = 'T_NEW'
                  group by
                    batchid
                )
              where BatchID = a.BatchID
            ) then 'OK'
            else 'Mismatch'
          end as Result
        from ${catalog}.${wh_db}_${scale_factor}.Audit a
        where BatchID in (1, 2, 3)
      ) o
    union all
    select 'DimTrade canceled trades', NULL, case
        when (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.DimTrade
          where Status = 'Canceled'
        ) = (
          select sum(Value)
          from ${catalog}.${wh_db}_${scale_factor}.Audit
          where DataSet = 'DimTrade'
            and Attribute = 'T_CanceledTrades'
        ) then 'OK'
        else 'Mismatch'
      end, 'Actual row counts matches Audit table'
    union all
    select 'DimTrade commission alerts', NULL, case
        when (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.dimessages
          where MessageType = 'Alert'
            and messageText = 'Invalid trade commission'
        ) = (
          select sum(Value)
          from ${catalog}.${wh_db}_${scale_factor}.Audit
          where DataSet = 'DimTrade'
            and Attribute = 'T_InvalidCommision'
        ) then 'OK'
        else 'Mismatch'
      end, 'Actual row counts matches Audit table'
    union all
    select 'DimTrade charge alerts', NULL, case
        when (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.dimessages
          where MessageType = 'Alert'
            and messageText = 'Invalid trade fee'
        ) = (
          select sum(Value)
          from ${catalog}.${wh_db}_${scale_factor}.Audit
          where DataSet = 'DimTrade'
            and Attribute = 'T_InvalidCharge'
        ) then 'OK'
        else 'Mismatch'
      end, 'Actual row counts matches Audit table'
    union all
    select 'DimTrade batches', NULL, case
        when (
          select count(distinct BatchID)
          from ${catalog}.${wh_db}_${scale_factor}.DimTrade
        ) = 3
        and (
          select max(BatchID)
          from ${catalog}.${wh_db}_${scale_factor}.DimTrade
        ) = 3 then 'OK'
        else 'Mismatch'
      end, 'BatchID values must match Audit table'
    union all
    select 'DimTrade distinct keys', NULL, case
        when (
          select count(distinct TradeID)
          from ${catalog}.${wh_db}_${scale_factor}.DimTrade
        ) = (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.DimTrade
        ) then 'OK'
        else 'Not unique'
      end, 'All keys are distinct'
    union all
    select 'DimTrade SK_BrokerID', NULL, case
        when (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.DimTrade
        ) = (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.DimTrade a
            join ${catalog}.${wh_db}_${scale_factor}.DimDate _d on _d.SK_DateID = a.SK_CreateDateID
            join ${catalog}.${wh_db}_${scale_factor}.DimBroker c on a.SK_BrokerID = c.SK_BrokerID
            and c.EffectiveDate <= _d.DateValue
            and _d.DateValue <= c.EndDate
        ) --(select DateValue from ${catalog}.${wh_db}_${scale_factor}.DimDate where SK_DateID = a.SK_CreateDateID) and (select DateValue from ${catalog}.${wh_db}_${scale_factor}.DimDate where SK_DateID = a.SK_CreateDateID) <= c.EndDate)
        then 'OK'
        else 'Bad join'
      end, 'All SK_BrokerIDs match a DimBroker record with a valid date range'
    union all
    select 'DimTrade SK_CompanyID', NULL, case
        when (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.DimTrade
        ) = (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.DimTrade a
            join ${catalog}.${wh_db}_${scale_factor}.DimDate _d on _d.SK_DateID = a.SK_CreateDateID
            join ${catalog}.${wh_db}_${scale_factor}.DimCompany c on a.SK_CompanyID = c.SK_CompanyID
            and c.EffectiveDate <= _d.DateValue
            and _d.DateValue <= c.EndDate
        ) --(select DateValue from ${catalog}.${wh_db}_${scale_factor}.DimDate where SK_DateID = a.SK_CreateDateID) and (select DateValue from ${catalog}.${wh_db}_${scale_factor}.DimDate where SK_DateID = a.SK_CreateDateID) <= c.EndDate)
        then 'OK'
        else 'Bad join'
      end, 'All SK_CompanyIDs match a DimCompany record with a valid date range'
    union all
    select 'DimTrade SK_SecurityID', NULL, case
        when (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.DimTrade
        ) = (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.DimTrade a
            join ${catalog}.${wh_db}_${scale_factor}.DimDate _d on _d.SK_DateID = a.SK_CreateDateID
            join ${catalog}.${wh_db}_${scale_factor}.DimSecurity c on a.SK_SecurityID = c.SK_SecurityID
            and c.EffectiveDate <= _d.DateValue
            and _d.DateValue <= c.EndDate
        ) --(select DateValue from ${catalog}.${wh_db}_${scale_factor}.DimDate where SK_DateID = a.SK_CreateDateID) and (select DateValue from ${catalog}.${wh_db}_${scale_factor}.DimDate where SK_DateID = a.SK_CreateDateID) <= c.EndDate)
        then 'OK'
        else 'Bad join'
      end, 'All SK_SecurityIDs match a DimSecurity record with a valid date range'
    union all
    select 'DimTrade SK_CustomerID', NULL, case
        when (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.DimTrade
        ) = (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.DimTrade a
            join ${catalog}.${wh_db}_${scale_factor}.DimDate _d on _d.SK_DateID = a.SK_CreateDateID
            join ${catalog}.${wh_db}_${scale_factor}.DimCustomer c on a.SK_CustomerID = c.SK_CustomerID
            and c.EffectiveDate <= _d.DateValue
            and _d.DateValue <= c.EndDate
        ) --(select DateValue from ${catalog}.${wh_db}_${scale_factor}.DimDate where SK_DateID = a.SK_CreateDateID) and (select DateValue from ${catalog}.${wh_db}_${scale_factor}.DimDate where SK_DateID = a.SK_CreateDateID) <= c.EndDate)
        then 'OK'
        else 'Bad join'
      end, 'All SK_CustomerIDs match a DimCustomer record with a valid date range'
    union all
    select 'DimTrade SK_AccountID', NULL, case
        when (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.DimTrade
        ) = (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.DimTrade a
            join ${catalog}.${wh_db}_${scale_factor}.DimDate _d on _d.SK_DateID = a.SK_CreateDateID
            join ${catalog}.${wh_db}_${scale_factor}.DimAccount c on a.SK_AccountID = c.SK_AccountID
            and c.EffectiveDate <= _d.DateValue
            and _d.DateValue <= c.EndDate
        ) --(select DateValue from ${catalog}.${wh_db}_${scale_factor}.DimDate where SK_DateID = a.SK_CreateDateID) and (select DateValue from ${catalog}.${wh_db}_${scale_factor}.DimDate where SK_DateID = a.SK_CreateDateID) <= c.EndDate)
        then 'OK'
        else 'Bad join'
      end, 'All SK_AccountIDs match a DimAccount record with a valid date range'
    union all
    select 'DimTrade date check', BatchID, Result, 'All SK_DateID values are in the correct batch time window'
    from (
        select distinct BatchID, (
            case
              when (
                select count(*)
                from ${catalog}.${wh_db}_${scale_factor}.DimTrade w
                where w.BatchID = a.BatchID
                  and (
                    w.SK_CloseDateID is null
                    and (
                      (
                        select first(DateValue)
                        from ${catalog}.${wh_db}_${scale_factor}.DimDate
                        where   SK_DateID = w.SK_CreateDateID
                      ) > (
                        select first(Date)
                        from ${catalog}.${wh_db}_${scale_factor}.Audit
                        where   DataSet = 'Batch'
                          and Attribute = 'LastDay'
                          and BatchID = w.BatchID
                      )
                      or (
                        select first(DateValue)
                        from ${catalog}.${wh_db}_${scale_factor}.DimDate
                        where   SK_DateID = w.SK_CreateDateID
                      ) < (
                        case
                          when w.Type like 'Limit%'
                          /* Limit trades can have create dates earlier than the current Batch date, but not earlier than Batch1's first date */
                          then (
                            select first(Date)
                            from ${catalog}.${wh_db}_${scale_factor}.Audit
                            where       DataSet = 'Batch'
                              and Attribute = 'FirstDay'
                              and BatchID = 1
                          )
                          else (
                            select first(Date)
                            from ${catalog}.${wh_db}_${scale_factor}.Audit
                            where       DataSet = 'Batch'
                              and Attribute = 'FirstDay'
                              and BatchID = w.BatchID
                          )
                        end
                      )
                    )
                    or w.SK_CloseDateID is not null
                    and (
                      (
                        select first(DateValue)
                        from ${catalog}.${wh_db}_${scale_factor}.DimDate
                        where   SK_DateID = w.SK_CloseDateID
                      ) > (
                        select first(Date)
                        from ${catalog}.${wh_db}_${scale_factor}.Audit
                        where   DataSet = 'Batch'
                          and Attribute = 'LastDay'
                          and BatchID = w.BatchID
                      )
                      or (
                        select first(DateValue)
                        from ${catalog}.${wh_db}_${scale_factor}.DimDate
                        where   SK_DateID = w.SK_CloseDateID
                      ) < (
                        select first(Date)
                        from ${catalog}.${wh_db}_${scale_factor}.Audit
                        where   DataSet = 'Batch'
                          and Attribute = 'FirstDay'
                          and BatchID = w.BatchID
                      )
                      or SK_CloseDateID < SK_CreateDateID
                    )
                  )
              ) = 0 then 'OK'
              else 'Mismatch'
            end
          ) as Result
        from ${catalog}.${wh_db}_${scale_factor}.Audit a
        where BatchID in (1, 2, 3)
      ) o
    union all
    select 'DimTrade Status', NULL, case
        when (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.DimTrade
          where Status not in (
              'Canceled', 'Pending', 'Submitted', 'Active', 'Completed'
            )
        ) = 0 then 'OK'
        else 'Bad value'
      end, 'All Trade Status values are valid'
    union all
    select 'DimTrade Type', NULL, case
        when (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.DimTrade
          where Type not in (
              'Market Buy', 'Market Sell', 'Stop Loss', 'Limit Sell', 'Limit Buy'
            )
        ) = 0 then 'OK'
        else 'Bad value'
      end, 'All Trade Type values are valid'
    -- Checks against the Financial table.
    union all
    select 'Financial row count', NULL, case
        when (
          select MessageData
          from ${catalog}.${wh_db}_${scale_factor}.dimessages
          where MessageSource = 'Financial'
            and MessageType = 'Validation'
            and MessageText = 'Row count'
            and BatchID = 1
        ) = (
          select sum(Value)
          from ${catalog}.${wh_db}_${scale_factor}.Audit
          where DataSet = 'Financial'
            and Attribute = 'FW_FIN'
        ) then 'OK'
        else 'Mismatch'
      end, 'Actual row count matches Audit table'
    union all
    select 'Financial SK_CompanyID', NULL, case
        when (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.Financial
        ) = (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.Financial a
            join ${catalog}.${wh_db}_${scale_factor}.DimCompany c on a.SK_CompanyID = c.SK_CompanyID
        ) then 'OK'
        else 'Bad join'
      end, 'All SK_CompanyIDs match a DimCompany record'
    union all
    select 'Financial FI_YEAR', NULL, case
        when (
          (
            select count(*)
            from ${catalog}.${wh_db}_${scale_factor}.Financial
            where FI_YEAR < year(
                (
                  select Date
                  from ${catalog}.${wh_db}_${scale_factor}.Audit
                  where DataSet = 'Batch'
                    and BatchID = 1
                    and Attribute = 'FirstDay'
                )
              )
          ) + (
            select count(*)
            from ${catalog}.${wh_db}_${scale_factor}.Financial
            where FI_YEAR > year(
                (
                  select Date
                  from ${catalog}.${wh_db}_${scale_factor}.Audit
                  where DataSet = 'Batch'
                    and BatchID = 1
                    and Attribute = 'LastDay'
                )
              )
          )
        ) = 0 then 'OK'
        else 'Bad Year'
      end, 'All Years are within Batch1 range'
    union all
    select 'Financial FI_QTR', NULL, case
        when (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.Financial
          where FI_QTR not in (1, 2, 3, 4)
        ) = 0 then 'OK'
        else 'Bad Qtr'
      end, 'All quarters are in ( 1, 2, 3, 4 )'
    union all
    select 'Financial FI_QTR_START_DATE', NULL, case
        when (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.Financial
          where FI_YEAR <> year(FI_QTR_START_DATE)
            or month(FI_QTR_START_DATE) <> (FI_QTR -1) * 3 + 1
            or day(FI_QTR_START_DATE) <> 1
        ) = 0 then 'OK'
        else 'Bad date'
      end, 'All quarters start on correct date'
    union all
    select 'Financial EPS', NULL, case
        when (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.Financial
          where Round(FI_NET_EARN / FI_OUT_BASIC, 2) - FI_BASIC_EPS not between -0.4
            and 0.4
            or Round(FI_NET_EARN / FI_OUT_DILUT, 2) - FI_DILUT_EPS not between -0.4
            and 0.4
            or Round(FI_NET_EARN / FI_REVENUE, 2) - FI_MARGIN not between -0.4
            and 0.4
        ) = 0 then 'OK'
        else 'Bad EPS'
      end, 'Earnings calculations are valid'
    -- Checks against the FactMarketHistory table.
    union all
    select 'FactMarketHistory row count', BatchID, Result, 'Actual row count matches Audit table'
    from (
        select distinct BatchID, (
            case
              when --added sum(messageData) and sum(value)
              cast(
                (
                  select sum(MessageData)
                  from ${catalog}.${wh_db}_${scale_factor}.dimessages
                  where MessageSource = 'FactMarketHistory'
                    and MessageType = 'Validation'
                    and MessageText = 'Row count'
                    and BatchID = a.BatchID
                ) as int
              ) - cast(
                (
                  select sum(MessageData)
                  from ${catalog}.${wh_db}_${scale_factor}.dimessages
                  where MessageSource = 'FactMarketHistory'
                    and MessageType = 'Validation'
                    and MessageText = 'Row count'
                    and BatchID = a.BatchID -1
                ) as int
              ) = (
                select sum(Value)
                from ${catalog}.${wh_db}_${scale_factor}.Audit
                where DataSet = 'FactMarketHistory'
                  and Attribute = 'DM_RECORDS'
                  and BatchID = a.BatchID
              ) then 'OK'
              else 'Mismatch'
            end
          ) as Result
        from ${catalog}.${wh_db}_${scale_factor}.Audit a
        where BatchID in (1, 2, 3)
      ) o
    union all
    select 'FactMarketHistory batches', NULL, case
        when (
          select count(distinct BatchID)
          from ${catalog}.${wh_db}_${scale_factor}.FactMarketHistory
        ) = 3
        and (
          select max(BatchID)
          from ${catalog}.${wh_db}_${scale_factor}.FactMarketHistory
        ) = 3 then 'OK'
        else 'Mismatch'
      end, 'BatchID values must match Audit table'
    union all
    select 'FactMarketHistory SK_CompanyID', NULL, case
        when (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.FactMarketHistory
        ) = (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.FactMarketHistory a
            join ${catalog}.${wh_db}_${scale_factor}.DimDate _d on _d.SK_DateID = a.SK_DateID
            join ${catalog}.${wh_db}_${scale_factor}.DimCompany c on a.SK_CompanyID = c.SK_CompanyID
            and c.EffectiveDate <= _d.DateValue
            and _d.DateValue <= c.EndDate
        ) --(select DateValue from ${catalog}.${wh_db}_${scale_factor}.DimDate where SK_DateID = a.SK_DateID) and (select DateValue from ${catalog}.${wh_db}_${scale_factor}.DimDate where SK_DateID = a.SK_DateID) <= c.EndDate)
        then 'OK'
        else 'Bad join'
      end, 'All SK_CompanyIDs match a DimCompany record with a valid date range'
    union all
    select 'FactMarketHistory SK_SecurityID', NULL, case
        when (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.FactMarketHistory
        ) = (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.FactMarketHistory a
            join ${catalog}.${wh_db}_${scale_factor}.DimDate _d on _d.SK_DateID = a.SK_DateID
            join ${catalog}.${wh_db}_${scale_factor}.DimSecurity c on a.SK_SecurityID = c.SK_SecurityID
            and c.EffectiveDate <= _d.DateValue
            and _d.DateValue <= c.EndDate
        ) --(select DateValue from ${catalog}.${wh_db}_${scale_factor}.DimDate where SK_DateID = a.SK_DateID) and (select DateValue from ${catalog}.${wh_db}_${scale_factor}.DimDate where SK_DateID = a.SK_DateID) <= c.EndDate)
        then 'OK'
        else 'Bad join'
      end, 'All SK_SecurityIDs match a DimSecurity record with a valid date range'
    union all
    select 'FactMarketHistory SK_DateID', BatchID, Result, 'All dates are within batch date range'
    from (
        select distinct BatchID, (
            case
              when (
                (
                  select count(*)
                  from ${catalog}.${wh_db}_${scale_factor}.FactMarketHistory m
                  where m.BatchID = a.BatchID
                    and (
                      select first(DateValue)
                      from ${catalog}.${wh_db}_${scale_factor}.DimDate
                      where SK_DateID = m.SK_DateID
                    ) < (
                      select first(Date) -1 day
                      from ${catalog}.${wh_db}_${scale_factor}.Audit
                      where DataSet = 'Batch'
                        and BatchID = m.BatchID
                        and Attribute = 'FirstDay'
                    )
                ) + (
                  select count(*)
                  from ${catalog}.${wh_db}_${scale_factor}.FactMarketHistory m
                  where m.BatchID = a.BatchID
                    and (
                      select first(DateValue)
                      from ${catalog}.${wh_db}_${scale_factor}.DimDate
                      where SK_DateID = m.SK_DateID
                    ) >= (
                      select first(Date)
                      from ${catalog}.${wh_db}_${scale_factor}.Audit
                      where DataSet = 'Batch'
                        and BatchID = m.BatchID
                        and Attribute = 'LastDay'
                    )
                )
              ) = 0 then 'OK'
              else 'Bad Date'
            end
          ) as Result
        from ${catalog}.${wh_db}_${scale_factor}.Audit a
        where BatchID in (1, 2, 3)
      ) o
    union all
    select 'FactMarketHistory relative dates', NULL, case
        when (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.FactMarketHistory
          where FiftyTwoWeekLow > DayLow
            or DayLow > ClosePrice
            or ClosePrice > DayHigh
            or DayHigh > FiftyTwoWeekHigh
        ) = 0 then 'OK'
        else 'Bad Date'
      end, '52-week-low <= day_low <= close_price <= day_high <= 52-week-high'
    -- Checks against the FactHoldings table.
    union all
    select 'FactHoldings row count', BatchID, Result, 'Actual row count matches Audit table'
    from (
        select distinct BatchID, (
            case
              when --added sum for messagedata and value
              cast(
                (
                  select sum(MessageData)
                  from ${catalog}.${wh_db}_${scale_factor}.dimessages
                  where MessageSource = 'FactHoldings'
                    and MessageType = 'Validation'
                    and MessageText = 'Row count'
                    and BatchID = a.BatchID
                ) as int
              ) - cast(
                (
                  select sum(MessageData)
                  from ${catalog}.${wh_db}_${scale_factor}.dimessages
                  where MessageSource = 'FactHoldings'
                    and MessageType = 'Validation'
                    and MessageText = 'Row count'
                    and BatchID = a.BatchID -1
                ) as int
              ) = (
                select sum(Value)
                from ${catalog}.${wh_db}_${scale_factor}.Audit
                where DataSet = 'FactHoldings'
                  and Attribute = 'HH_RECORDS'
                  and BatchID = a.BatchID
              ) then 'OK'
              else 'Mismatch'
            end
          ) as Result
        from ${catalog}.${wh_db}_${scale_factor}.Audit a
        where BatchID in (1, 2, 3)
      ) o
    union all
    select 'FactHoldings batches', NULL, case
        when (
          select count(distinct BatchID)
          from ${catalog}.${wh_db}_${scale_factor}.FactHoldings
        ) = 3
        and (
          select max(BatchID)
          from ${catalog}.${wh_db}_${scale_factor}.FactHoldings
        ) = 3 then 'OK'
        else 'Mismatch'
      end, 'BatchID values must match Audit table'
    union all
      /* It is possible that the dimension record has changed between orgination of the trade and the completion of the trade. *
       * So, we can check that the Effective Date of the dimension record is older than the the completion date, but the end date could be earlier or later than the completion date
       */
    select 'FactHoldings SK_CustomerID', NULL, case
        when (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.FactHoldings
        ) = (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.FactHoldings a
            join ${catalog}.${wh_db}_${scale_factor}.DimDate _d on _d.SK_DateID = a.SK_DateID
            join ${catalog}.${wh_db}_${scale_factor}.DimCustomer c on a.SK_CustomerID = c.SK_CustomerID
            and c.EffectiveDate <= _d.DateValue --(select DateValue from ${catalog}.${wh_db}_${scale_factor}.DimDate where SK_DateID = a.SK_DateID)
        ) then 'OK'
        else 'Bad join'
      end, 'All SK_CustomerIDs match a DimCustomer record with a valid date range'
    union all
    select 'FactHoldings SK_AccountID', NULL, case
        when (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.FactHoldings
        ) = (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.FactHoldings a
            join ${catalog}.${wh_db}_${scale_factor}.DimDate _d on _d.SK_DateID = a.SK_DateID
            join ${catalog}.${wh_db}_${scale_factor}.DimAccount c on a.SK_AccountID = c.SK_AccountID
            and c.EffectiveDate <= _d.DateValue --(select DateValue from ${catalog}.${wh_db}_${scale_factor}.DimDate where SK_DateID = a.SK_DateID)
        ) then 'OK'
        else 'Bad join'
      end, 'All SK_AccountIDs match a DimAccount record with a valid date range'
    union all
    select 'FactHoldings SK_CompanyID', NULL, case
        when (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.FactHoldings
        ) = (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.FactHoldings a
            join ${catalog}.${wh_db}_${scale_factor}.DimDate _d on _d.SK_DateID = a.SK_DateID
            join ${catalog}.${wh_db}_${scale_factor}.DimCompany c on a.SK_CompanyID = c.SK_CompanyID
            and c.EffectiveDate <= _d.DateValue --(select DateValue from ${catalog}.${wh_db}_${scale_factor}.DimDate where SK_DateID = a.SK_DateID)
        ) then 'OK'
        else 'Bad join'
      end, 'All SK_CompanyIDs match a DimCompany record with a valid date range'
    union all
    select 'FactHoldings SK_SecurityID', NULL, case
        when (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.FactHoldings
        ) = (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.FactHoldings a
            join ${catalog}.${wh_db}_${scale_factor}.DimDate _d on _d.SK_DateID = a.SK_DateID
            join ${catalog}.${wh_db}_${scale_factor}.DimSecurity c on a.SK_SecurityID = c.SK_SecurityID
            and c.EffectiveDate <= _d.DateValue --(select DateValue from ${catalog}.${wh_db}_${scale_factor}.DimDate where SK_DateID = a.SK_DateID)
        ) then 'OK'
        else 'Bad join'
      end, 'All SK_SecurityIDs match a DimSecurity record with a valid date range'
    union all
    select 'FactHoldings CurrentTradeID', NULL, case
        when (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.FactHoldings
        ) = (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.FactHoldings a
            join ${catalog}.${wh_db}_${scale_factor}.DimTrade t on a.CurrentTradeID = t.TradeID
            and a.SK_DateID = t.SK_CloseDateID
            and a.SK_TimeID = t.SK_CloseTimeID
        ) then 'OK'
        else 'Failed'
      end, 'CurrentTradeID matches a DimTrade record with and Close Date and Time are values are used as the holdings date and time'
    union all
    select 'FactHoldings SK_DateID', BatchID, Result, 'All dates are within batch date range'
    from (
        select distinct BatchID, (
            case
              when (
                (
                  select count(*)
                  from ${catalog}.${wh_db}_${scale_factor}.FactHoldings m
                  where BatchID = a.BatchID
                    and (
                      select first(DateValue)
                      from ${catalog}.${wh_db}_${scale_factor}.DimDate
                      where SK_DateID = m.SK_DateID
                    ) < (
                      select first(Date)
                      from ${catalog}.${wh_db}_${scale_factor}.Audit
                      where DataSet = 'Batch'
                        and BatchID = m.BatchID
                        and Attribute = 'FirstDay'
                    )
                ) + (
                  select count(*)
                  from ${catalog}.${wh_db}_${scale_factor}.FactHoldings m
                  where BatchID = a.BatchID
                    and (
                      select first(DateValue)
                      from ${catalog}.${wh_db}_${scale_factor}.DimDate
                      where SK_DateID = m.SK_DateID
                    ) > (
                      select first(Date)
                      from ${catalog}.${wh_db}_${scale_factor}.Audit
                      where DataSet = 'Batch'
                        and BatchID = m.BatchID
                        and Attribute = 'LastDay'
                    )
                )
              ) = 0 then 'OK'
              else 'Bad Date'
            end
          ) as Result
        from ${catalog}.${wh_db}_${scale_factor}.Audit a
        where BatchID in (1, 2, 3)
      ) o
    -- Checks against the FactCashBalances table.
    union all
    select 'FactCashBalances batches', NULL, case
        when (
          select count(distinct BatchID)
          from ${catalog}.${wh_db}_${scale_factor}.FactCashBalances
        ) = 3
        and (
          select max(BatchID)
          from ${catalog}.${wh_db}_${scale_factor}.FactCashBalances
        ) = 3 then 'OK'
        else 'Mismatch'
      end, 'BatchID values must match Audit table'
    union all
    select 'FactCashBalances SK_CustomerID', NULL, case
        when (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.FactCashBalances
        ) = (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.FactCashBalances a
            join ${catalog}.${wh_db}_${scale_factor}.DimDate _d on _d.SK_DateID = a.SK_DateID
            join ${catalog}.${wh_db}_${scale_factor}.DimCustomer c on a.SK_CustomerID = c.SK_CustomerID
            and c.EffectiveDate <= _d.DateValue
            and _d.DateValue <= c.EndDate
        ) --(select DateValue from ${catalog}.${wh_db}_${scale_factor}.DimDate where SK_DateID = a.SK_DateID) and (select DateValue from ${catalog}.${wh_db}_${scale_factor}.DimDate where SK_DateID = a.SK_DateID) <= c.EndDate)
        then 'OK'
        else 'Bad join'
      end, 'All SK_CustomerIDs match a DimCustomer record with a valid date range'
    union all
    select 'FactCashBalances SK_AccountID', NULL, case
        when (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.FactCashBalances
        ) = (
          select count(*)
          from ${catalog}.${wh_db}_${scale_factor}.FactCashBalances a
            join ${catalog}.${wh_db}_${scale_factor}.DimDate _d on _d.SK_DateID = a.SK_DateID
            join ${catalog}.${wh_db}_${scale_factor}.DimAccount c on a.SK_AccountID = c.SK_AccountID
            and c.EffectiveDate <= _d.DateValue
            and _d.DateValue <= c.EndDate
        ) --(select DateValue from ${catalog}.${wh_db}_${scale_factor}.DimDate where SK_DateID = a.SK_DateID) and (select DateValue from ${catalog}.${wh_db}_${scale_factor}.DimDate where SK_DateID = a.SK_DateID) <= c.EndDate)
        then 'OK'
        else 'Bad join'
      end, 'All SK_AccountIDs match a DimAccount record with a valid date range'
    union all
    select 'FactCashBalances SK_DateID', BatchID, Result, 'All dates are within batch date range'
    from (
        select distinct BatchID, (
            case
              when (
                (
                  select count(*)
                  from ${catalog}.${wh_db}_${scale_factor}.FactCashBalances m
                  where BatchID = a.BatchID
                    and (
                      select first(DateValue)
                      from ${catalog}.${wh_db}_${scale_factor}.DimDate
                      where SK_DateID = m.SK_DateID
                    ) < (
                      select first(Date)
                      from ${catalog}.${wh_db}_${scale_factor}.Audit
                      where DataSet = 'Batch'
                        and BatchID = m.BatchID
                        and Attribute = 'FirstDay'
                    )
                ) + (
                  select count(*)
                  from ${catalog}.${wh_db}_${scale_factor}.FactCashBalances m
                  where BatchID = a.BatchID
                    and (
                      select first(DateValue)
                      from ${catalog}.${wh_db}_${scale_factor}.DimDate
                      where SK_DateID = m.SK_DateID
                    ) > (
                      select first(Date)
                      from ${catalog}.${wh_db}_${scale_factor}.Audit
                      where DataSet = 'Batch'
                        and BatchID = m.BatchID
                        and Attribute = 'LastDay'
                    )
                )
              ) = 0 then 'OK'
              else 'Bad Date'
            end
          ) as Result
        from ${catalog}.${wh_db}_${scale_factor}.Audit a
        where BatchID in (1, 2, 3)
      ) o
    -- Checks against the Batch Validation Query row counts
    union all
    select 'Batch row count: ' || MessageSource, BatchID, case
        when RowsLastBatch > RowsThisBatch then 'Row count decreased'
        else 'OK'
      end, 'Row counts do not decrease between successive batches'
    from (
        select distinct(a.BatchID), m.MessageSource, cast(m1.MessageData as bigint) as RowsThisBatch, cast(m2.MessageData as bigint) as RowsLastBatch
        from ${catalog}.${wh_db}_${scale_factor}.Audit a full
          join ${catalog}.${wh_db}_${scale_factor}.dimessages m on m.BatchID = 0
          and m.MessageText = 'Row count'
          and m.MessageType = 'Validation'
          join ${catalog}.${wh_db}_${scale_factor}.dimessages m1 on m1.BatchID = a.BatchID
          and m1.MessageSource = m.MessageSource
          and m1.MessageText = 'Row count'
          and m1.MessageType = 'Validation'
          join ${catalog}.${wh_db}_${scale_factor}.dimessages m2 on m2.BatchID = a.BatchID -1
          and m2.MessageSource = m.MessageSource
          and m2.MessageText = 'Row count'
          and m2.MessageType = 'Validation'
        where a.BatchID in (1, 2, 3)
      ) o
    union all
    select 'Batch joined row count: ' || MessageSource, BatchID, case
        when RowsJoined = RowsUnjoined then 'OK'
        else 'No match'
      end, 'Row counts match when joined to dimensions'
    from (
        select distinct(a.BatchID), m.MessageSource, cast(m1.MessageData as bigint) as RowsUnjoined, cast(m2.MessageData as bigint) as RowsJoined
        from ${catalog}.${wh_db}_${scale_factor}.Audit a full
          join ${catalog}.${wh_db}_${scale_factor}.dimessages m on m.BatchID = 0
          and m.MessageText = 'Row count'
          and m.MessageType = 'Validation'
          join ${catalog}.${wh_db}_${scale_factor}.dimessages m1 on m1.BatchID = a.BatchID
          and m1.MessageSource = m.MessageSource
          and m1.MessageText = 'Row count'
          and m1.MessageType = 'Validation'
          join ${catalog}.${wh_db}_${scale_factor}.dimessages m2 on m2.BatchID = a.BatchID
          and m2.MessageSource = m.MessageSource
          and m2.MessageText = 'Row count joined'
          and m2.MessageType = 'Validation'
        where a.BatchID in (1, 2, 3)
      ) o
    -- Checks against the Data Visibility Query row counts
    union all
    select 'Data visibility row counts: ' || MessageSource, NULL as BatchID, case
        when regressions = 0 then 'OK'
        else 'Row count decreased'
      end, 'Row counts must be non-decreasing over time'
    from (
        select m1.MessageSource, sum(
            case
              when cast(m1.MessageData as bigint) > cast(m2.MessageData as bigint) then 1
              else 0
            end
          ) as regressions
        from ${catalog}.${wh_db}_${scale_factor}.dimessages m1
          join ${catalog}.${wh_db}_${scale_factor}.dimessages m2 on m2.MessageType IN ('Visibility_1', 'Visibility_2')
          and m2.MessageText = 'Row count'
          and m2.MessageSource = m1.MessageSource
          and m2.MessageDateAndTime > m1.MessageDateAndTime
        where m1.MessageType IN ('Visibility_1', 'Visibility_2')
          and m1.MessageText = 'Row count'
        group by
          m1.MessageSource
      ) o
    union all
    select 'Data visibility joined row counts: ' || MessageSource, NULL as BatchID, case
        when regressions = 0 then 'OK'
        else 'No match'
      end, 'Row counts match when joined to dimensions'
    from (
        select m1.MessageSource, sum(
            case
              when cast(m1.MessageData as bigint) > cast(m2.MessageData as bigint) then 1
              else 0
            end
          ) as regressions
        from ${catalog}.${wh_db}_${scale_factor}.dimessages m1
          join ${catalog}.${wh_db}_${scale_factor}.dimessages m2 on m2.MessageType = 'Visibility_1'
          and m2.MessageText = 'Row count joined'
          and m2.MessageSource = m1.MessageSource
          and m2.MessageDateAndTime = m1.MessageDateAndTime
        where m1.MessageType = 'Visibility_1'
          and m1.MessageText = 'Row count'
        group by
          m1.MessageSource
      ) o
  ) q;

-- COMMAND ----------

-- DISABLE Predictive Optimization since this is the end of the benchmark and is just for testing purposes
ALTER DATABASE ${catalog}.${wh_db}_${scale_factor}_stage DISABLE PREDICTIVE OPTIMIZATION;
ALTER DATABASE ${catalog}.${wh_db}_${scale_factor} DISABLE PREDICTIVE OPTIMIZATION;
