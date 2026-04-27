"""Builder for the CLUSTER/DBSQL single-batch benchmark workflow.

Replaces `jinja_templates/workflows_single_batch.json`.

The original Jinja template is highly repetitive — every dim/fact/ingest
task repeats the same envelope (depends_on, run_if, notebook_task,
optional warehouse_id, optional job_cluster_key, notification_settings).
We push that boilerplate into `_workflow_common.make_task()` and only
spell out the per-task parameters here.
"""
from __future__ import annotations

from typing import Any

try:
    from workflow_builders import _workflow_common as common
except ImportError:
    import _workflow_common as common


def _description(*, data_generator: str, exec_type: str, scale_factor: int,
                 catalog: str, wh_target: str, tpcdi_directory: str) -> str:
    if data_generator == "spark":
        return (
            f"TPC-DI single-batch workflow on {exec_type} (**Spark** data "
            f"generator, SF={scale_factor}). Reads distributed PySpark output "
            f"from `{tpcdi_directory}sf={scale_factor}/` — split files per "
            f"table (e.g. `Customer_1.txt`, `Customer_2.txt`, ...). Writes to "
            f"schema `{catalog}.{wh_target}_{exec_type}_spark_data_gen_"
            f"single_batch_{scale_factor}` (and `..._stage`). All 3 TPC-DI "
            f"batches are ingested in parallel after dw_init, then dimension/"
            f"fact tables are built in a single pass. NOTE: official audit "
            f"checks are NOT run in single-batch mode — use the incremental "
            f"variant for audit validation."
        )
    return (
        f"TPC-DI single-batch workflow on {exec_type} (**DIGen.jar** native, "
        f"legacy data generator, SF={scale_factor}). Reads single-threaded "
        f"DIGen output from `{tpcdi_directory}sf={scale_factor}/` — one file "
        f"per table (e.g. `Customer.txt`, `Trade.txt`). Writes to schema "
        f"`{catalog}.{wh_target}_{exec_type}_native_data_gen_single_batch_"
        f"{scale_factor}` (and `..._stage`). All 3 TPC-DI batches are "
        f"ingested in parallel after dw_init, then dimension/fact tables are "
        f"built in a single pass. NOTE: official audit checks are NOT run in "
        f"single-batch mode — use the incremental variant for audit "
        f"validation."
    )


def _constraint(default_clause: str, perf_opt_flg: bool) -> str:
    return "" if perf_opt_flg else default_clause


def _tprops(opt_write: str, index_cols: str) -> str:
    return f"'delta.autoOptimize.autoCompact'=False, {opt_write} {index_cols}"


def _finwire_tprops(opt_write: str) -> str:
    return f"'delta.dataSkippingNumIndexedCols' = 0, 'delta.autoOptimize.autoCompact'=False, {opt_write}"


def build(*, job_name: str, catalog: str, wh_target: str, scale_factor: int,
          tpcdi_directory: str, repo_src_path: str, exec_type: str,
          datagen_label: str, batched_label: str, data_generator: str,
          serverless: str, pred_opt: str, perf_opt_flg: bool, opt_write: str,
          null_constraint: str, index_cols: str, cloud_provider: str,
          dbr: str | None = None, driver_node_type: str | None = None,
          worker_node_type: str | None = None,
          worker_node_count: int | None = None,
          wh_id: str | None = None,
          **_unused) -> dict:

    # Repeated parameter values
    common_params = {
        "tbl_props": _tprops(opt_write, index_cols),
    }

    def task(**kwargs):
        return common.make_task(
            exec_type=exec_type,
            serverless=serverless,
            job_name=job_name,
            wh_id=wh_id,
            **kwargs,
        )

    # ---------- Tasks ----------
    tasks: list[dict] = [
        task(
            task_key="dw_init",
            notebook_path=f"{repo_src_path}/single_batch/SQL/dw_init",
            base_params={"pred_opt": "{{job.parameters.predictive_optimization}}"},
        ),
        common.make_customermgmt_task(
            repo_src_path=repo_src_path,
            job_name=job_name,
            data_generator=data_generator,
            scale_factor=scale_factor,
            serverless=serverless,
            exec_type=exec_type,
            wh_id=wh_id,
            depends_on=["dw_init"],
            modern_notebook_path=f"{repo_src_path}/single_batch/SQL/CustomerMgmtRaw",
        ),
    ]

    raw_ingest_path = f"{repo_src_path}/single_batch/SQL/raw_ingestion"

    # ingest_DimDate
    tasks.append(task(
        task_key="ingest_DimDate", notebook_path=raw_ingest_path,
        depends_on=["dw_init"], run_if="AT_LEAST_ONE_SUCCESS",
        base_params={
            "raw_schema": f"sk_dateid BIGINT {null_constraint} COMMENT 'Surrogate key for the date', datevalue DATE COMMENT 'The date stored appropriately for doing comparisons in the Data Warehouse', datedesc STRING COMMENT 'The date in full written form e.g. July 7 2004', calendaryearid INT COMMENT 'Year number as a number', calendaryeardesc STRING COMMENT 'Year number as text', calendarqtrid INT COMMENT 'Quarter as a number e.g. 20042', calendarqtrdesc STRING COMMENT 'Quarter as text e.g. 2004 Q2', calendarmonthid INT COMMENT 'Month as a number e.g. 20047', calendarmonthdesc STRING COMMENT 'Month as text e.g. 2004 July', calendarweekid INT COMMENT 'Week as a number e.g. 200428', calendarweekdesc STRING COMMENT 'Week as text e.g. 2004-W28', dayofweeknum INT COMMENT 'Day of week as a number e.g. 3', dayofweekdesc STRING COMMENT 'Day of week as text e.g. Wednesday', fiscalyearid INT COMMENT 'Fiscal year as a number e.g. 2005', fiscalyeardesc STRING COMMENT 'Fiscal year as text e.g. 2005', fiscalqtrid INT COMMENT 'Fiscal quarter as a number e.g. 20051', fiscalqtrdesc STRING COMMENT 'Fiscal quarter as text e.g. 2005 Q1', holidayflag BOOLEAN COMMENT 'Indicates holidays'",
            "filename": "Date.txt",
            "tbl": "DimDate",
            "constraints": _constraint(", CONSTRAINT dimdate_pk PRIMARY KEY(sk_dateid)", perf_opt_flg),
            "tbl_props": _tprops(opt_write, index_cols),
        },
    ))

    # ingest_DimTime
    tasks.append(task(
        task_key="ingest_DimTime", notebook_path=raw_ingest_path,
        depends_on=["dw_init"], run_if="AT_LEAST_ONE_SUCCESS",
        base_params={
            "raw_schema": f"sk_timeid BIGINT {null_constraint} COMMENT 'Surrogate key for the time', timevalue STRING COMMENT 'The time stored appropriately for doing', hourid INT COMMENT 'Hour number as a number e.g. 01', hourdesc STRING COMMENT 'Hour number as text e.g. 01', minuteid INT COMMENT 'Minute as a number e.g. 23', minutedesc STRING COMMENT 'Minute as text e.g. 01:23', secondid INT COMMENT 'Second as a number e.g. 45', seconddesc STRING COMMENT 'Second as text e.g. 01:23:45', markethoursflag BOOLEAN COMMENT 'Indicates a time during market hours', officehoursflag BOOLEAN COMMENT 'Indicates a time during office hours'",
            "filename": "Time.txt",
            "tbl": "DimTime",
            "constraints": _constraint(", CONSTRAINT dimtime_pk PRIMARY KEY(sk_timeid)", perf_opt_flg),
            "tbl_props": _tprops(opt_write, index_cols),
        },
    ))

    # ingest_StatusType
    tasks.append(task(
        task_key="ingest_StatusType", notebook_path=raw_ingest_path,
        depends_on=["dw_init"], run_if="AT_LEAST_ONE_SUCCESS",
        base_params={
            "raw_schema": f"st_id STRING COMMENT 'Status code', st_name STRING {null_constraint} COMMENT 'Status description'",
            "filename": "StatusType.txt",
            "tbl": "StatusType",
            "constraints": _constraint(", CONSTRAINT statustype_pk PRIMARY KEY(st_name)", perf_opt_flg),
            "tbl_props": _tprops(opt_write, index_cols),
        },
    ))

    # ingest_TaxRate
    tasks.append(task(
        task_key="ingest_TaxRate", notebook_path=raw_ingest_path,
        depends_on=["dw_init"], run_if="AT_LEAST_ONE_SUCCESS",
        base_params={
            "raw_schema": f"tx_id STRING {null_constraint} COMMENT 'Tax rate code', tx_name STRING COMMENT 'Tax rate description', tx_rate FLOAT COMMENT 'Tax rate'",
            "filename": "TaxRate.txt",
            "tbl": "TaxRate",
            "constraints": _constraint(", CONSTRAINT taxrate_pk PRIMARY KEY(tx_id)", perf_opt_flg),
            "tbl_props": _tprops(opt_write, index_cols),
        },
    ))

    # ingest_TradeType
    tasks.append(task(
        task_key="ingest_TradeType", notebook_path=raw_ingest_path,
        depends_on=["dw_init"], run_if="AT_LEAST_ONE_SUCCESS",
        base_params={
            "raw_schema": f"tt_id STRING {null_constraint} COMMENT 'Trade type code', tt_name STRING COMMENT 'Trade type description', tt_is_sell INT COMMENT 'Flag indicating a sale', tt_is_mrkt INT COMMENT 'Flag indicating a market order'",
            "filename": "TradeType.txt",
            "tbl": "TradeType",
            "constraints": _constraint(", CONSTRAINT tradetype_pk PRIMARY KEY(tt_id)", perf_opt_flg),
            "tbl_props": _tprops(opt_write, index_cols),
        },
    ))

    # ingest_industry
    tasks.append(task(
        task_key="ingest_industry", notebook_path=raw_ingest_path,
        depends_on=["dw_init"], run_if="AT_LEAST_ONE_SUCCESS",
        base_params={
            "raw_schema": f"in_id STRING COMMENT 'Industry code', in_name STRING {null_constraint} COMMENT 'Industry description', in_sc_id STRING COMMENT 'Sector identifier'",
            "filename": "Industry.txt",
            "tbl": "Industry",
            "constraints": _constraint(", CONSTRAINT industry_pk PRIMARY KEY(in_name)", perf_opt_flg),
            "tbl_props": _tprops(opt_write, index_cols),
        },
    ))

    # ingest_BatchDate (uses Ingest_Incremental notebook)
    tasks.append(task(
        task_key="ingest_BatchDate",
        notebook_path=f"{repo_src_path}/single_batch/SQL/Ingest_Incremental",
        depends_on=["dw_init"], run_if="AT_LEAST_ONE_SUCCESS",
        base_params={
            "filename": "BatchDate.txt",
            "raw_schema": f"batchdate DATE {null_constraint} COMMENT 'Batch date'",
            "tbl": "BatchDate",
            "constraints": _constraint(", CONSTRAINT batchdate_pk PRIMARY KEY(batchdate)", perf_opt_flg),
            "tbl_props": _tprops(opt_write, index_cols),
        },
    ))

    # ingest_FinWire
    tasks.append(task(
        task_key="ingest_FinWire",
        notebook_path=f"{repo_src_path}/single_batch/SQL/ingest_finwire",
        depends_on=["dw_init"], run_if="AT_LEAST_ONE_SUCCESS",
        base_params={"tbl_props": _finwire_tprops(opt_write)},
    ))

    # ingest_ProspectIncremental
    tasks.append(task(
        task_key="ingest_ProspectIncremental",
        notebook_path=f"{repo_src_path}/single_batch/SQL/ingest_prospectincremental",
        depends_on=["dw_init"], run_if="AT_LEAST_ONE_SUCCESS",
        base_params={"tbl_props": _tprops(opt_write, index_cols)},
    ))

    # Silver_DimBroker
    tasks.append(task(
        task_key="Silver_DimBroker",
        notebook_path=f"{repo_src_path}/single_batch/SQL/DimBroker",
        depends_on=["ingest_DimDate"],
        base_params={
            "tgt_schema": f"sk_brokerid BIGINT {null_constraint} COMMENT 'Surrogate key for broker', brokerid BIGINT COMMENT 'Natural key for broker', managerid BIGINT COMMENT 'Natural key for manager\u2019s HR record', firstname STRING COMMENT 'First name', lastname STRING COMMENT 'Last Name', middleinitial STRING COMMENT 'Middle initial', branch STRING COMMENT 'Facility in which employee has office', office STRING COMMENT 'Office number or description', phone STRING COMMENT 'Employee phone number', iscurrent BOOLEAN COMMENT 'True if this is the current record', batchid INT COMMENT 'Batch ID when this record was inserted', effectivedate DATE COMMENT 'Beginning of date range when this record was the current record', enddate DATE COMMENT 'Ending of date range when this record was the current record. A record that is not expired will use the date 9999-12-31.'",
            "constraints": _constraint(", CONSTRAINT dimbroker_pk PRIMARY KEY(sk_brokerid)", perf_opt_flg),
            "tbl_props": _tprops(opt_write, index_cols),
        },
    ))

    # Silver_DimCustomer
    tasks.append(task(
        task_key="Silver_DimCustomer",
        notebook_path=f"{repo_src_path}/single_batch/SQL/DimCustomer",
        depends_on=["ingest_customermgmt", "ingest_ProspectIncremental",
                    "ingest_TaxRate", "ingest_BatchDate"],
        base_params={
            "tgt_schema": f"sk_customerid BIGINT {null_constraint} COMMENT 'Surrogate key for CustomerID', customerid BIGINT COMMENT 'Customer identifier', taxid STRING COMMENT 'Customer\u2019s tax identifier', status STRING COMMENT 'Customer status type', lastname STRING COMMENT 'Customers last name.', firstname STRING COMMENT 'Customers first name.', middleinitial STRING COMMENT 'Customers middle name initial', gender STRING COMMENT 'Gender of the customer', tier TINYINT COMMENT 'Customer tier', dob DATE COMMENT 'Customer\u2019s date of birth.', addressline1 STRING COMMENT 'Address Line 1', addressline2 STRING COMMENT 'Address Line 2', postalcode STRING COMMENT 'Zip or Postal Code', city STRING COMMENT 'City', stateprov STRING COMMENT 'State or Province', country STRING COMMENT 'Country', phone1 STRING COMMENT 'Phone number 1', phone2 STRING COMMENT 'Phone number 2', phone3 STRING COMMENT 'Phone number 3', email1 STRING COMMENT 'Email address 1', email2 STRING COMMENT 'Email address 2', nationaltaxratedesc STRING COMMENT 'National Tax rate description', nationaltaxrate FLOAT COMMENT 'National Tax rate', localtaxratedesc STRING COMMENT 'Local Tax rate description', localtaxrate FLOAT COMMENT 'Local Tax rate', agencyid STRING COMMENT 'Agency identifier', creditrating INT COMMENT 'Credit rating', networth INT COMMENT 'Net worth', marketingnameplate STRING COMMENT 'Marketing nameplate', iscurrent BOOLEAN COMMENT 'True if this is the current record', batchid INT COMMENT 'Batch ID when this record was inserted', effectivedate DATE COMMENT 'Beginning of date range when this record was the current record', enddate DATE COMMENT 'Ending of date range when this record was the current record. A record that is not expired will use the date 9999-12-31.'",
            "constraints": _constraint(", CONSTRAINT dimcustomer_pk PRIMARY KEY(sk_customerid)", perf_opt_flg),
            "tbl_props": _tprops(opt_write, index_cols),
        },
    ))

    # Silver_DimAccount
    tasks.append(task(
        task_key="Silver_DimAccount",
        notebook_path=f"{repo_src_path}/single_batch/SQL/DimAccount",
        depends_on=["Silver_DimBroker", "Silver_DimCustomer"],
        base_params={
            "tgt_schema": f"sk_accountid BIGINT {null_constraint} COMMENT 'Surrogate key for AccountID', accountid BIGINT COMMENT 'Customer account identifier', sk_brokerid BIGINT COMMENT 'Surrogate key of managing broker', sk_customerid BIGINT COMMENT 'Surrogate key of customer', accountdesc STRING COMMENT 'Name of customer account', taxstatus TINYINT COMMENT 'Tax status of this account', status STRING COMMENT 'Account status, active or closed', iscurrent BOOLEAN COMMENT 'True if this is the current record', batchid INT COMMENT 'Batch ID when this record was inserted', effectivedate DATE COMMENT 'Beginning of date range when this record was the current record', enddate DATE COMMENT 'Ending of date range when this record was the current record. A record that is not expired will use the date 9999-12-31.'",
            "constraints": _constraint(", CONSTRAINT dimaccount_pk PRIMARY KEY(sk_accountid), CONSTRAINT dimaccount_customer_fk FOREIGN KEY (sk_customerid) REFERENCES DimCustomer(sk_customerid), CONSTRAINT dimaccount_broker_fk FOREIGN KEY (sk_brokerid) REFERENCES DimBroker(sk_brokerid)", perf_opt_flg),
            "tbl_props": _tprops(opt_write, index_cols),
        },
    ))

    # Gold_FactCashBalances
    tasks.append(task(
        task_key="Gold_FactCashBalances",
        notebook_path=f"{repo_src_path}/single_batch/SQL/FactCashBalances",
        depends_on=["Silver_DimAccount"],
        base_params={
            "tgt_schema": f"sk_customerid BIGINT {null_constraint} COMMENT 'Surrogate key for CustomerID', sk_accountid BIGINT {null_constraint} COMMENT 'Surrogate key for AccountID', sk_dateid BIGINT {null_constraint} COMMENT 'Surrogate key for the date', cash DOUBLE COMMENT 'Cash balance for the account after applying', batchid INT COMMENT 'Batch ID when this record was inserted'",
            "constraints": _constraint(", CONSTRAINT cashbalances_pk PRIMARY KEY(sk_customerid, sk_accountid, sk_dateid), CONSTRAINT cashbalances_customer_fk FOREIGN KEY (sk_customerid) REFERENCES DimCustomer(sk_customerid), CONSTRAINT cashbalances_account_fk FOREIGN KEY (sk_accountid) REFERENCES DimAccount(sk_accountid), CONSTRAINT cashbalances_date_fk FOREIGN KEY (sk_dateid) REFERENCES DimDate(sk_dateid)", perf_opt_flg),
            "tbl_props": _tprops(opt_write, index_cols),
        },
    ))

    # Silver_Prospect
    tasks.append(task(
        task_key="Silver_Prospect",
        notebook_path=f"{repo_src_path}/single_batch/SQL/Prospect",
        depends_on=["Silver_DimCustomer"],
        base_params={
            "tgt_schema": f"agencyid STRING {null_constraint} COMMENT 'Unique identifier from agency', sk_recorddateid BIGINT COMMENT 'Last date this prospect appeared in input', sk_updatedateid BIGINT COMMENT 'Latest change date for this prospect', batchid INT COMMENT 'Batch ID when this record was last modified', iscustomer BOOLEAN COMMENT 'True if this person is also in DimCustomer,else False', lastname STRING COMMENT 'Last name', firstname STRING COMMENT 'First name', middleinitial STRING COMMENT 'Middle initial', gender STRING COMMENT 'M / F / U', addressline1 STRING COMMENT 'Postal address', addressline2 STRING COMMENT 'Postal address', postalcode STRING COMMENT 'Postal code', city STRING COMMENT 'City', state STRING COMMENT 'State or province', country STRING COMMENT 'Postal country', phone STRING COMMENT 'Telephone number', income STRING COMMENT 'Annual income', numbercars INT COMMENT 'Cars owned', numberchildren INT COMMENT 'Dependent children', maritalstatus STRING COMMENT 'S / M / D / W / U', age INT COMMENT 'Current age', creditrating INT COMMENT 'Numeric rating', ownorrentflag STRING COMMENT 'O / R / U', employer STRING COMMENT 'Name of employer', numbercreditcards INT COMMENT 'Credit cards', networth INT COMMENT 'Estimated total net worth', marketingnameplate STRING COMMENT 'For marketing purposes'",
            "constraints": _constraint(", CONSTRAINT prospect_pk PRIMARY KEY(agencyid)", perf_opt_flg),
            "tbl_props": _tprops(opt_write, index_cols),
        },
    ))

    # Silver_DimCompany
    tasks.append(task(
        task_key="Silver_DimCompany",
        notebook_path=f"{repo_src_path}/single_batch/SQL/DimCompany",
        depends_on=["ingest_FinWire", "ingest_industry"],
        base_params={
            "tgt_schema": f"sk_companyid BIGINT {null_constraint} COMMENT 'Surrogate key for CompanyID', companyid BIGINT COMMENT 'Company identifier (CIK number)', status STRING COMMENT 'Company status', name STRING COMMENT 'Company name', industry STRING COMMENT 'Company\u2019s industry', sprating STRING COMMENT 'Standard & Poor company\u2019s rating', islowgrade BOOLEAN COMMENT 'True if this company is low grade', ceo STRING COMMENT 'CEO name', addressline1 STRING COMMENT 'Address Line 1', addressline2 STRING COMMENT 'Address Line 2', postalcode STRING COMMENT 'Zip or postal code', city STRING COMMENT 'City', stateprov STRING COMMENT 'State or Province', country STRING COMMENT 'Country', description STRING COMMENT 'Company description', foundingdate DATE COMMENT 'Date the company was founded', iscurrent BOOLEAN COMMENT 'True if this is the current record', batchid INT COMMENT 'Batch ID when this record was inserted', effectivedate DATE COMMENT 'Beginning of date range when this record was the current record', enddate DATE COMMENT 'Ending of date range when this record was the current record. A record that is not expired will use the date 9999-12-31.'",
            "constraints": _constraint(", CONSTRAINT dimcompany_pk PRIMARY KEY(sk_companyid), CONSTRAINT dimcompany_status_fk FOREIGN KEY (status) REFERENCES StatusType(st_name), CONSTRAINT dimcompany_industry_fk FOREIGN KEY (industry) REFERENCES Industry(in_name)", perf_opt_flg),
            "tbl_props": _tprops(opt_write, index_cols),
        },
    ))

    # Silver_DimSecurity
    tasks.append(task(
        task_key="Silver_DimSecurity",
        notebook_path=f"{repo_src_path}/single_batch/SQL/DimSecurity",
        depends_on=["Silver_DimCompany"],
        base_params={
            "tgt_schema": f"sk_securityid BIGINT {null_constraint} COMMENT 'Surrogate key for Symbol', symbol STRING COMMENT 'Identifies security on ticker', issue STRING COMMENT 'Issue type', status STRING COMMENT 'Status type', name STRING COMMENT 'Security name', exchangeid STRING COMMENT 'Exchange the security is traded on', sk_companyid BIGINT COMMENT 'Company issuing security', sharesoutstanding BIGINT COMMENT 'Shares outstanding', firsttrade DATE COMMENT 'Date of first trade', firsttradeonexchange DATE COMMENT 'Date of first trade on this exchange', dividend DOUBLE COMMENT 'Annual dividend per share', iscurrent BOOLEAN COMMENT 'True if this is the current record', batchid INT COMMENT 'Batch ID when this record was inserted', effectivedate DATE COMMENT 'Beginning of date range when this record was the current record', enddate DATE COMMENT 'Ending of date range when this record was the current record. A record that is not expired will use the date 9999-12-31.'",
            "constraints": _constraint(", CONSTRAINT dimsecurity_pk PRIMARY KEY(sk_securityid), CONSTRAINT dimsecurity_status_fk FOREIGN KEY (status) REFERENCES StatusType(st_name), CONSTRAINT dimsecurity_company_fk FOREIGN KEY (sk_companyid) REFERENCES DimCompany(sk_companyid)", perf_opt_flg),
            "tbl_props": _tprops(opt_write, index_cols),
        },
    ))

    # Gold_FactWatches
    tasks.append(task(
        task_key="Gold_FactWatches",
        notebook_path=f"{repo_src_path}/single_batch/SQL/FactWatches",
        depends_on=["Silver_DimSecurity", "Silver_DimCustomer"],
        base_params={
            "tgt_schema": f"sk_customerid BIGINT {null_constraint} COMMENT 'Customer associated with watch list', sk_securityid BIGINT {null_constraint} COMMENT 'Security listed on watch list', sk_dateid_dateplaced BIGINT COMMENT 'Date the watch list item was added', sk_dateid_dateremoved BIGINT COMMENT 'Date the watch list item was removed', batchid INT COMMENT 'Batch ID when this record was inserted'",
            "constraints": _constraint(", CONSTRAINT factwatches_pk PRIMARY KEY(sk_customerid, sk_securityid), CONSTRAINT factwatches_customer_fk FOREIGN KEY (sk_customerid) REFERENCES DimCustomer(sk_customerid), CONSTRAINT factwatches_security_fk FOREIGN KEY (sk_securityid) REFERENCES DimSecurity(sk_securityid), CONSTRAINT factwatches_dateplaced_fk FOREIGN KEY (sk_dateid_dateplaced) REFERENCES DimDate(sk_dateid), CONSTRAINT factwatches_dateremoved_fk FOREIGN KEY (sk_dateid_dateremoved) REFERENCES DimDate(sk_dateid)", perf_opt_flg),
            "tbl_props": _tprops(opt_write, index_cols),
        },
    ))

    # Silver_DimTrade
    tasks.append(task(
        task_key="Silver_DimTrade",
        notebook_path=f"{repo_src_path}/single_batch/SQL/DimTrade",
        depends_on=["Silver_DimSecurity", "Silver_DimAccount"],
        base_params={
            "tgt_schema": f"tradeid BIGINT {null_constraint} COMMENT 'Trade identifier', sk_brokerid BIGINT COMMENT 'Surrogate key for BrokerID', sk_createdateid BIGINT COMMENT 'Surrogate key for date created', sk_createtimeid BIGINT COMMENT 'Surrogate key for time created', sk_closedateid BIGINT COMMENT 'Surrogate key for date closed', sk_closetimeid BIGINT COMMENT 'Surrogate key for time closed', status STRING COMMENT 'Trade status', type STRING COMMENT 'Trade type', cashflag BOOLEAN COMMENT 'Is this trade a cash or margin trade?', sk_securityid BIGINT COMMENT 'Surrogate key for SecurityID', sk_companyid BIGINT COMMENT 'Surrogate key for CompanyID', quantity INT COMMENT 'Quantity of securities traded.', bidprice DOUBLE COMMENT 'The requested unit price.', sk_customerid BIGINT COMMENT 'Surrogate key for CustomerID', sk_accountid BIGINT COMMENT 'Surrogate key for AccountID', executedby STRING COMMENT 'Name of person executing the trade.', tradeprice DOUBLE COMMENT 'Unit price at which the security was traded.', fee DOUBLE COMMENT 'Fee charged for placing this trade request', commission DOUBLE COMMENT 'Commission earned on this trade', tax DOUBLE COMMENT 'Amount of tax due on this trade', batchid INT COMMENT 'Batch ID when this record was inserted'",
            "constraints": _constraint(", CONSTRAINT dimtrade_pk PRIMARY KEY(tradeid), CONSTRAINT dimtrade_security_fk FOREIGN KEY (sk_securityid) REFERENCES DimSecurity(sk_securityid), CONSTRAINT dimtrade_company_fk FOREIGN KEY (sk_companyid) REFERENCES DimCompany(sk_companyid), CONSTRAINT dimtrade_broker_fk FOREIGN KEY (sk_brokerid) REFERENCES DimBroker(sk_brokerid), CONSTRAINT dimtrade_account_fk FOREIGN KEY (sk_accountid) REFERENCES DimAccount(sk_accountid), CONSTRAINT dimtrade_customer_fk FOREIGN KEY (sk_customerid) REFERENCES DimCustomer(sk_customerid), CONSTRAINT dimtrade_createdate_fk FOREIGN KEY (sk_createdateid) REFERENCES DimDate(sk_dateid), CONSTRAINT dimtrade_closedate_fk FOREIGN KEY (sk_closedateid) REFERENCES DimDate(sk_dateid), CONSTRAINT dimtrade_createtime_fk FOREIGN KEY (sk_createtimeid) REFERENCES DimTime(sk_timeid), CONSTRAINT dimtrade_closetime_fk FOREIGN KEY (sk_closetimeid) REFERENCES DimTime(sk_timeid)", perf_opt_flg),
            "tbl_props": _tprops(opt_write, index_cols),
        },
    ))

    # Silver_FactHoldings
    tasks.append(task(
        task_key="Silver_FactHoldings",
        notebook_path=f"{repo_src_path}/single_batch/SQL/FactHoldings",
        depends_on=["Silver_DimTrade"],
        base_params={
            "tgt_schema": f"tradeid BIGINT COMMENT 'Key for Orignial Trade Indentifier', currenttradeid BIGINT {null_constraint} COMMENT 'Key for the current trade', sk_customerid BIGINT COMMENT 'Surrogate key for Customer Identifier', sk_accountid BIGINT COMMENT 'Surrogate key for Account Identifier', sk_securityid BIGINT COMMENT 'Surrogate key for Security Identifier', sk_companyid BIGINT COMMENT 'Surrogate key for Company Identifier', sk_dateid BIGINT COMMENT 'Surrogate key for the date associated with the', sk_timeid BIGINT COMMENT 'Surrogate key for the time associated with the', currentprice DOUBLE COMMENT 'Unit price of this security for the current trade', currentholding INT COMMENT 'Quantity of a security held after the current trade.', batchid INT COMMENT 'Batch ID when this record was inserted'",
            "constraints": _constraint(", CONSTRAINT factholdings_pk PRIMARY KEY(currenttradeid), CONSTRAINT factholdings_security_fk FOREIGN KEY (sk_securityid) REFERENCES DimSecurity(sk_securityid), CONSTRAINT factholdings_company_fk FOREIGN KEY (sk_companyid) REFERENCES DimCompany(sk_companyid), CONSTRAINT factholdings_trade_fk FOREIGN KEY (tradeid) REFERENCES DimTrade(tradeid), CONSTRAINT factholdings_currenttrade_fk FOREIGN KEY (currenttradeid) REFERENCES DimTrade(tradeid), CONSTRAINT factholdings_account_fk FOREIGN KEY (sk_accountid) REFERENCES DimAccount(sk_accountid), CONSTRAINT factholdings_customer_fk FOREIGN KEY (sk_customerid) REFERENCES DimCustomer(sk_customerid), CONSTRAINT factholdings_date_fk FOREIGN KEY (sk_dateid) REFERENCES DimDate(sk_dateid), CONSTRAINT factholdings_time_fk FOREIGN KEY (sk_timeid) REFERENCES DimTime(sk_timeid)", perf_opt_flg),
            "tbl_props": _tprops(opt_write, index_cols),
        },
    ))

    # Silver_Financial
    tasks.append(task(
        task_key="Silver_Financial",
        notebook_path=f"{repo_src_path}/single_batch/SQL/Financial",
        depends_on=["Silver_DimCompany"],
        base_params={
            "tgt_schema": f"sk_companyid BIGINT {null_constraint} COMMENT 'Company SK.', fi_year INT {null_constraint} COMMENT 'Year of the quarter end.', fi_qtr INT {null_constraint} COMMENT 'Quarter number that the financial information is for: valid values 1, 2, 3, 4.', fi_qtr_start_date DATE COMMENT 'Start date of quarter.', fi_revenue DOUBLE COMMENT 'Reported revenue for the quarter.', fi_net_earn DOUBLE COMMENT 'Net earnings reported for the quarter.', fi_basic_eps DOUBLE COMMENT 'Basic earnings per share for the quarter.', fi_dilut_eps DOUBLE COMMENT 'Diluted earnings per share for the quarter.', fi_margin DOUBLE COMMENT 'Profit divided by revenues for the quarter.', fi_inventory DOUBLE COMMENT 'Value of inventory on hand at the end of quarter.', fi_assets DOUBLE COMMENT 'Value of total assets at the end of the quarter.', fi_liability DOUBLE COMMENT 'Value of total liabilities at the end of the quarter.', fi_out_basic BIGINT COMMENT 'Average number of shares outstanding (basic).', fi_out_dilut BIGINT COMMENT 'Average number of shares outstanding (diluted).'",
            "constraints": _constraint(", CONSTRAINT financial_pk PRIMARY KEY(sk_companyid, fi_year, fi_qtr), CONSTRAINT financial_company_fk FOREIGN KEY (sk_companyid) REFERENCES DimCompany(sk_companyid)", perf_opt_flg),
            "tbl_props": _tprops(opt_write, index_cols),
        },
    ))

    # Gold_FactMarketHistory
    tasks.append(task(
        task_key="Gold_FactMarketHistory",
        notebook_path=f"{repo_src_path}/single_batch/SQL/FactMarketHistory",
        depends_on=["Silver_DimSecurity", "Silver_Financial"],
        base_params={
            "tgt_schema": f"sk_securityid BIGINT {null_constraint} COMMENT 'Surrogate key for SecurityID', sk_companyid BIGINT COMMENT 'Surrogate key for CompanyID', sk_dateid BIGINT {null_constraint} COMMENT 'Surrogate key for the date', peratio DOUBLE COMMENT 'Price to earnings per share ratio', yield DOUBLE COMMENT 'Dividend to price ratio, as a percentage', fiftytwoweekhigh DOUBLE COMMENT 'Security highest price in last 52 weeks from this day', sk_fiftytwoweekhighdate BIGINT COMMENT 'Earliest date on which the 52 week high price was set', fiftytwoweeklow DOUBLE COMMENT 'Security lowest price in last 52 weeks from this day', sk_fiftytwoweeklowdate BIGINT COMMENT 'Earliest date on which the 52 week low price was set', closeprice DOUBLE COMMENT 'Security closing price on this day', dayhigh DOUBLE COMMENT 'Highest price for the security on this day', daylow DOUBLE COMMENT 'Lowest price for the security on this day', volume INT COMMENT 'Trading volume of the security on this day', batchid INT COMMENT 'Batch ID when this record was inserted'",
            "constraints": _constraint(", CONSTRAINT fmh_pk PRIMARY KEY(sk_securityid, sk_dateid), CONSTRAINT fmh_security_fk FOREIGN KEY (sk_securityid) REFERENCES DimSecurity(sk_securityid), CONSTRAINT fmh_company_fk FOREIGN KEY (sk_companyid) REFERENCES DimCompany(sk_companyid), CONSTRAINT fmh_date_fk FOREIGN KEY (sk_dateid) REFERENCES DimDate(sk_dateid)", perf_opt_flg),
            "tbl_props": _tprops(opt_write, index_cols),
        },
    ))

    # ---------- Top-level workflow ----------
    workflow: dict[str, Any] = {
        "name": job_name,
        "description": _description(
            data_generator=data_generator, exec_type=exec_type,
            scale_factor=scale_factor, catalog=catalog, wh_target=wh_target,
            tpcdi_directory=tpcdi_directory,
        ),
        "tags": {"data_generator": "spark" if data_generator == "spark" else "native_jar"},
        "email_notifications": {"no_alert_for_skipped_runs": False},
        "webhook_notifications": {},
        "timeout_seconds": 0,
        "max_concurrent_runs": 1,
        "parameters": common.base_parameters(
            catalog=catalog, scale_factor=scale_factor,
            tpcdi_directory=tpcdi_directory,
            wh_db_default=f"{wh_target}_{exec_type}_{datagen_label}_{batched_label}",
            pred_opt=pred_opt,
        ) + [common.cleanup_param()],
    }

    # Cluster: serverless+digen+SF>100 → SingleNode for mavenlib;
    # not serverless → classic Photon used by all tasks; serverless and
    # not digen-high-SF → no job_clusters block.
    digen_high_sf_serverless = (
        data_generator == "digen" and scale_factor > 100 and serverless == "YES"
    )
    if digen_high_sf_serverless:
        workflow["job_clusters"] = [common.serverless_singlenode_cluster(
            job_name=job_name, dbr=dbr or "",
            driver_node_type=driver_node_type or "",
            worker_node_type=worker_node_type or "",
        )]
    elif serverless != "YES":
        workflow["job_clusters"] = [common.classic_photon_cluster(
            job_name=job_name, dbr=dbr or "",
            driver_node_type=driver_node_type or "",
            worker_node_type=worker_node_type or "",
            worker_node_count=worker_node_count or 0,
            cloud_provider=cloud_provider,
        )]

    # Final cleanup pair — condition gate + SQL notebook. Depends on every
    # leaf so it's last; ALL_DONE means partial-failure runs still reach the
    # gate, and the SQL only fires when delete_tables_when_finished=TRUE.
    tasks.extend(common.make_cleanup_tasks(
        repo_src_path=repo_src_path, job_name=job_name,
        exec_type=exec_type, serverless=serverless, wh_id=wh_id,
        depends_on=[
            "Gold_FactCashBalances", "Silver_Prospect", "Gold_FactWatches",
            "Silver_FactHoldings", "Gold_FactMarketHistory",
        ],
    ))

    workflow["tasks"] = tasks
    return workflow
