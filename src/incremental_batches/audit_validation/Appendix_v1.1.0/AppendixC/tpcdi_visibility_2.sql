/* ++++++++++++++++++++++++++++++++++++++++++++++++++ *
 * +                                                + *
 * +        TPC-DI  Visibility_2 Query              + *
 * +        Version 1.1.0                           + *
 * +                                                + *
 * ++++++++++++++++++++++++++++++++++++++++++++++++++ *
 *                                                    *
 *       ====== Portability Substitutions ======      *
 *     ---        [FROM DUMMY_TABLE]    ------        *
 * DB2            from sysibm.sysdummy1               *
 * ORACLE         from dual                           *
 * SQLSERVER       <blank>                            *
 * -------------------------------------------------- *
 *     ------  [||] (String concatenation) ------     *    
 * SQLSERVER      +                                   *
 * -------------------------------------------------- *
 */
insert into DImessages
select
	 CURRENT_TIMESTAMP as MessageDateAndTime
	,case when BatchID is null then 0 else BatchID end as BatchID
	,MessageSource
	,MessageText 
	,'Visibility_2' as MessageType
	,MessageData
from (
	select max(BatchID) as BatchID from DImessages 
) x join (

	/* Basic row counts */
       select 'DimAccount' as MessageSource, 'Row count' as MessageText, 
			count(*) as MessageData from DimAccount
	union select 'DimBroker' as MessageSource, 'Row count' as MessageText, 
			count(*) as MessageData from DimBroker
	union select 'DimCompany' as MessageSource, 'Row count' as MessageText, 
			count(*) as MessageData from DimCompany
	union select 'DimCustomer' as MessageSource, 'Row count' as MessageText, 
			count(*) as MessageData from DimCustomer
	union select 'DimDate' as MessageSource, 'Row count' as MessageText, 
			count(*) as MessageData from DimDate
	union select 'DimSecurity' as MessageSource, 'Row count' as MessageText, 
			count(*) as MessageData from DimSecurity
	union select 'DimTime' as MessageSource, 'Row count' as MessageText, 
			count(*) as MessageData from DimTime
	union select 'DimTrade' as MessageSource, 'Row count' as MessageText, 
			count(*) as MessageData from DimTrade
	union select 'Financial' as MessageSource, 'Row count' as MessageText, 
			count(*) as MessageData from Financial
	union select 'Industry' as MessageSource, 'Row count' as MessageText, 
			count(*) as MessageData from Industry
	union select 'Prospect' as MessageSource, 'Row count' as MessageText,
			 count(*) as MessageData from Prospect
	union select 'StatusType' as MessageSource, 'Row count' as MessageText, 
			count(*) as MessageData from StatusType
	union select 'TaxRate' as MessageSource, 'Row count' as MessageText, 
			count(*) as MessageData from TaxRate
	union select 'TradeType' as MessageSource, 'Row count' as MessageText, 
			count(*) as MessageData from TradeType
	/* Row counts for Fact tables */
	union select 'FactCashBalances' as MessageSource, 'Row count' as MessageText, 
			count(*) as MessageData from FactCashBalances
	union select 'FactHoldings' as MessageSource, 'Row count' as MessageText, 
			count(*) as MessageData from FactHoldings
	union select 'FactMarketHistory' as MessageSource, 'Row count' as MessageText, 
			count(*) as MessageData from FactMarketHistory
	union select 'FactWatches' as MessageSource, 'Row count' as MessageText, 
			count(*) as MessageData from FactWatches

) y on 1=1
;
