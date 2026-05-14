{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'append',
    on_schema_change = 'ignore',
    file_format = 'delta',
  )
}}


{# Table is pre-created in setup_dbt.py with CLUSTER BY + #}
{# delta.dataSkippingNumIndexedCols=34. We do NOT declare a layout here — #}
{# declaring liquid_clustered_by in the dbt config makes dbt-databricks #}
{# issue ALTER TABLE CLUSTER BY (and ALTER TABLE SET TBLPROPERTIES) on #}
{# every batch, even when the existing table matches. Layout is owned by #}
{# the setup notebook ("setup-owns-layout" pattern), not dbt. #}

{# bronzeaccount holds ONLY the day's Account.txt drop. Customer-driven
   cascade rows (when a Customer SCD2 update needs to ripple onto matching
   account rows in dimaccount) are staged separately in
   `account_updates_from_customer` and UNIONed in by silver/dimaccount.

   This keeps bronzeaccount pure across all three variants:
     * Cluster routes derived rows to bronzeaccount (historical leak;
       Run-History totals already account for this)
     * SDP keeps bronzeaccount pure and pipes derived rows straight to
       dimaccount via a CDC flow
     * dbt (this project) keeps bronzeaccount pure and stages derived
       rows in account_updates_from_customer for dimaccount to UNION

   DAG benefit: account_updates_from_customer only needs bronzecustomer
   ready, so it can run in parallel with dimcustomer; dimaccount waits
   for all three (bronzeaccount + account_updates_from_customer +
   dimcustomer). #}

{%- set schema_str -%}
cdc_flag STRING, cdc_dsn BIGINT, accountid BIGINT, brokerid BIGINT,
customerid BIGINT, accountdesc STRING, taxstatus TINYINT, status STRING,
update_dt DATE
{%- endset -%}

select * from {{ read_daily_csv('Account.txt', schema_str) }}
{{ since_last_load('update_dt') }}
