CREATE OR REPLACE FILE FORMAT parquet_format
TYPE = 'PARQUET';

CREATE OR REPLACE TABLE TPCDI.SF_10000_SPLITTED_STAGING.Customermgmt
USING TEMPLATE (
  SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
  FROM TABLE(
    INFER_SCHEMA(
      LOCATION => '@TPCDI.SF_10000.EXTERNAL_STAGE/data/customermgmt',
      FILE_FORMAT => 'parquet_format'
    )
  )
);

COPY INTO TPCDI.SF_10000_SPLITTED_STAGING.Customermgmt 
FROM @TPCDI.SF_10000_SPLITTED.EXTERNAL_STAGE/data/customermgmt
FILE_FORMAT = (TYPE = 'PARQUET')
PATTERN = '.*\\.parquet'

MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

--using a cleaned table with unquoted column bname 
create or replace TABLE TPCDI.SF_10000_SPLITTED_STAGING.CUSTOMERMGMT_CLEAN (
	customerid NUMBER(38,0),
	accountid NUMBER(38,0),
	brokerid NUMBER(38,0),
	taxid VARCHAR(16777216),
	tier NUMBER(38,0),
	dob DATE,
	addressline1 VARCHAR(16777216),
	nat_tx_id VARCHAR(16777216),
	update_ts TIMESTAMP_NTZ(9),
	ActionType VARCHAR(16777216),
	addressline2 VARCHAR(16777216),
	postalcode VARCHAR(16777216),
	city VARCHAR(16777216),
	lastname VARCHAR(16777216),
	firstname VARCHAR(16777216),
	middleinitial VARCHAR(16777216),
	gender VARCHAR(16777216),
	phone3 VARCHAR(16777216),
	email1 VARCHAR(16777216),
	email2 VARCHAR(16777216),
	lcl_tx_id VARCHAR(16777216),
	accountdesc VARCHAR(16777216),
	taxstatus NUMBER(38,0),
	status VARCHAR(16777216),
	stateprov VARCHAR(16777216),
	country VARCHAR(16777216),
	phone1 VARCHAR(16777216),
	phone2 VARCHAR(16777216)
);


insert into TPCDI.SF_10000_SPLITTED_STAGING.CUSTOMERMGMT_clean select 
    "customerid",
	"accountid",
	"brokerid",
	"taxid" ,
	"tier",
	"dob",
	"addressline1" ,
	"nat_tx_id" ,
	"update_ts",
	"ActionType" ,
	"addressline2" ,
	"postalcode" ,
	"city" ,
	"lastname" ,
	"firstname" ,
	"middleinitial" ,
	"gender" ,
	"phone3" ,
	"email1" ,
	"email2" ,
	"lcl_tx_id" ,
	"accountdesc" ,
	"taxstatus",
	"status" ,
	"stateprov" ,
	"country" ,
	"phone1" ,
	"phone2"  from TPCDI.SF_10000_SPLITTED_STAGING.CUSTOMERMGMT;


select count(1) from TPCDI.SF_1000_STAGING.CUSTOMERMGMT_clean;

select count(*) from  TPCDI.SF_10000_SPLITTED_STAGING.Customermgmt