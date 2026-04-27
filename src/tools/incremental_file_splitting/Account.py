-- Databricks notebook source
CREATE WIDGET DROPDOWN scale_factor DEFAULT "10" CHOICES SELECT * FROM (VALUES ("10"), ("100"), ("1000"), ("5000"), ("10000"), ("20000"));
CREATE WIDGET TEXT tpcdi_directory DEFAULT "/Volumes/tpcdi/tpcdi_raw_data/tpcdi_volume/";
CREATE WIDGET TEXT target_dir DEFAULT 'split_data';
CREATE WIDGET TEXT catalog DEFAULT 'tpcdi';

-- COMMAND ----------

CREATE OR REPLACE TABLE ${catalog}.tpcdi_raw_data.rawaccount${scale_factor}
PARTITIONED BY (update_dt)
TBLPROPERTIES (
  'delta.autoOptimize.autoCompact'=True, 
  'delta.autoOptimize.optimizeWrite'=True
)
with hist as (
  select 
    decode(_ActionType, 
      "NEW","I",
      "ADDACCT","I",
      "UPDACCT","U",
      "UPDCUST","U",
      "CLOSEACCT","U",
      "INACT","U") cdc_flag,
    row_number() over (order by _ActionTS) - 1 cdc_dsn,
    nullif(Customer.Account._CA_ID, '') accountid,
    nullif(Customer.Account.CA_B_ID, '') brokerid, 
    nullif(Customer._C_ID, '') customerid,
    nullif(Customer.Account.CA_NAME, '') accountdesc, 
    nullif(Customer.Account._CA_TAX_ST, '') taxstatus, 
    decode(_ActionType, 
      "NEW","ACTV",
      "ADDACCT","ACTV",
      "UPDACCT","ACTV",
      "UPDCUST","ACTV",
      "CLOSEACCT","INAC",
      "INACT","INAC") status,
    timestamp(_ActionTS) update_ts
  FROM read_files(
    "${tpcdi_directory}sf=${scale_factor}/Batch1",
    format => "xml",
    inferSchema => False, 
    rowTag => "TPCDI:Action",
    fileNamePattern => "CustomerMgmt.xml"
  )
  WHERE
    _ActionType NOT IN ('UPDCUST', 'INACT')
),
changes as (
  SELECT
    cdc_flag,
    cdc_dsn,
    accountid,
    coalesce(
      brokerid,
      last_value(brokerid) IGNORE NULLS OVER (
        PARTITION BY accountid
        ORDER BY update_ts
      )
    ) brokerid,
    coalesce(
      customerid,
      last_value(customerid) IGNORE NULLS OVER (
        PARTITION BY accountid
        ORDER BY update_ts
      )
    ) customerid,
    coalesce(
      accountdesc,
      last_value(accountdesc) IGNORE NULLS OVER (
        PARTITION BY accountid
        ORDER BY update_ts
      )
    ) accountdesc,
    coalesce(
      taxstatus,
      last_value(taxstatus) IGNORE NULLS OVER (
        PARTITION BY accountid
        ORDER BY update_ts
      )
    ) taxstatus,
    status,
    date(update_ts) update_dt,
    row_number() over (
      PARTITION BY 
        accountid, 
        date(update_ts)
      ORDER BY update_ts desc
    ) rn
  from hist
)
select
  * except (rn)
from changes
where rn = 1