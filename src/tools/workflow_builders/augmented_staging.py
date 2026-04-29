"""Builder for the augmented_incremental data-gen workflow.

Multi-task DAG: stage 0 (spark gen → temp Delta) → stage 1 (build the
shared staging schema `tpcdi_incremental_staging_{sf}` + per-day staging
files at `_staging/sf={sf}/{date}/`) → cleanup_stage0 (drop the temp
Delta tables).

Stage 1 splits into two parallel branches that both depend only on
data_gen completing:

  Branch A — staging tables (Phase 2a):
    - The `tpcdi_incremental_staging_{sf}` + `..._stage` schemas are
      created inline by spark_runner during stage 0, so this workflow
      has no separate dw_init task. PO is intentionally NOT enabled on
      the staging schema (these tables are read-only deliverables).
    - raw_ingestion ingests StatusType / TaxRate / DimDate / DimTime /
      Industry / TradeType / BatchDate from the .txt files spark gen
      writes under {volume}/Batch1/. Reuses single_batch/SQL files
      verbatim.
    - ingest_FinWire ingests FinWire fixed-width files
    - Silver_DimBroker/DimCompany/DimSecurity/Financial — single_batch SQL
    - DimCustomer/DimAccount/DimTrade Historical — augmented historical
      SQL (rewired to read from temp Delta with stg_target='tables')
    - FactCashBalances/FactHoldings/FactWatches Historical — same
    - CompanyYearEPS — augmented historical SQL (reads from Financial +
      DimCompany staging tables)

  Branch B — staging files (Phase 2b):
    - 7 stage_files notebooks fan out per-day CSVs from the temp Delta
      tables WHERE stg_target='files' to {volume}/augmented_incremental/
      _staging/sf={sf}/{date}/{filename}

cleanup_stage0 depends on every leaf in both branches. Runs ALL_DONE
(not gated by delete_tables_when_finished — the temp Delta tables are
unconditionally temporary).

Data-gen widget (`data_gen_type`) defaults to
``augmented_incremental`` so the spark_runner skips Batch2/Batch3 and
writes Delta tables to ``tpcdi_raw_data.{dataset}{sf}``.
"""
from __future__ import annotations

from typing import Any


_DEFAULT_NOTIF = {
    "no_alert_for_skipped_runs": False,
    "no_alert_for_canceled_runs": False,
    "alert_on_last_attempt": False,
}

# wh_db for the shared staging schema. Hard-coded — it's not a per-user
# value (the augmented benchmark CLONEs from this schema, so every user
# expects to find it at the same well-known location).
_STAGING_WH_DB = "tpcdi_incremental_staging"


def _description(scale_factor: int, catalog: str) -> str:
    return (
        f"TPC-DI **augmented_incremental** data-generation workflow "
        f"(SF={scale_factor}, serverless). Stage 0 runs the Spark generator "
        f"in Delta-only mode (skips Batch2/Batch3, writes 7 temp tables to "
        f"`{catalog}.tpcdi_raw_data.*{scale_factor}`). Stage 1 fans out: "
        f"(a) stage_tables — populates `{catalog}.{_STAGING_WH_DB}_"
        f"{scale_factor}` (the shared staging schema the augmented "
        f"benchmark clones from); (b) stage_files — writes per-day CSV "
        f"files under `tpcdi_volume/augmented_incremental/_staging/"
        f"sf={scale_factor}/{{date}}/`. cleanup_stage0 drops the 7 temp "
        f"Delta tables once both branches complete."
    )


def _make_task(*, task_key: str, notebook_path: str,
               depends_on: list | None = None,
               base_params: dict | None = None,
               run_if: str = "ALL_SUCCESS") -> dict:
    """Common task envelope. All tasks run on serverless (no job_cluster_key).

    ``depends_on`` accepts a mix of strings (interpreted as task_keys) and
    dicts (passed through, e.g. ``{"task_key": "staging_check",
    "outcome": "true"}`` to depend on a condition_task's true branch).
    """
    notebook_task: dict[str, Any] = {
        "notebook_path": notebook_path,
        "source": "WORKSPACE",
    }
    if base_params is not None:
        notebook_task["base_parameters"] = base_params

    task: dict[str, Any] = {"task_key": task_key}
    if depends_on:
        task["depends_on"] = [
            d if isinstance(d, dict) else {"task_key": d} for d in depends_on
        ]
    task["run_if"] = run_if
    task["notebook_task"] = notebook_task
    task["timeout_seconds"] = 0
    task["email_notifications"] = {}
    task["notification_settings"] = dict(_DEFAULT_NOTIF)
    return task


# Convenience: dependency on staging_check's true branch (= "regenerate, staging is incomplete"). Used by every Stage 1a / 1b task in place of the old depends_on=["data_gen"].
_NEEDS_REGEN = {"task_key": "staging_check", "outcome": "true"}


# ---------- Schema strings (lifted from workflows_single_batch.py) ----------
# These mirror the single_batch builder's per-task base_parameters so the
# raw_ingestion / Silver_* notebooks build identical staging table shapes.
# null_constraint defaults to "NOT NULL" (we always enable PK constraints
# in augmented mode — the staging tables are read-only deliverables).

_NN = "NOT NULL"


def _tprops() -> str:
    """Stable tbl_props for staging tables — autoCompact off (we'll do a
    final OPTIMIZE later if needed), optimizeWrite on for fewer/larger files."""
    return ("'delta.autoOptimize.autoCompact'=False, "
            "'delta.autoOptimize.optimizeWrite'=True")


def _finwire_tprops() -> str:
    return ("'delta.dataSkippingNumIndexedCols' = 0, "
            "'delta.autoOptimize.autoCompact'=False, "
            "'delta.autoOptimize.optimizeWrite'=True")


def build(*, job_name: str, scale_factor: int, catalog: str,
          regenerate_data: str, log_level: str, repo_src_path: str,
          serverless: str = "YES",
          **_unused) -> dict:
    """Build the augmented_incremental data-gen workflow JSON."""

    # tpcdi_directory uses {{job.parameters.catalog}} interpolation so the path tracks any run-time override of `catalog` (the previous build-time interpolation baked the original catalog into the path, which silently went stale if the user later overrode catalog). single_batch SQL notebooks (raw_ingestion / ingest_finwire / Ingest_Incremental) read this via base_parameters per-task; stage_files notebooks read it via the same job-level parameter, but they only need it for the volume path so the dynamic reference is sufficient.
    _tpcdi_dir_dynamic = (
        "/Volumes/{{job.parameters.catalog}}/tpcdi_raw_data/tpcdi_volume/"
        "augmented_incremental/_staging/"
    )

    # tpcdi_directory base_param common to every task that reads volume files via the ${tpcdi_directory} widget — passed per-task with the dynamic catalog reference so a run-time override of `catalog` automatically flows through.
    _td_param = {"tpcdi_directory": _tpcdi_dir_dynamic}

    # wh_db base_param — hardcoded to the shared staging schema. Not a job-level param: data_gen_type=augmented_incremental fully determines where staging tables go, so there's no user knob.
    _wh_param = {"wh_db": _STAGING_WH_DB}

    _td_wh = {**_td_param, **_wh_param}

    tasks: list[dict] = []

    # ---------------- Stage 0: data_gen ----------------
    # Spark gen also creates the tpcdi_incremental_staging_{sf} + _stage schemas inline (replaces the old dw_init notebook task). Sets dbutils.jobs.taskValues `staging_complete` (string "true"/"false") which gates the condition_task below.
    tasks.append(_make_task(
        task_key="data_gen",
        notebook_path=f"{repo_src_path}/tools/data_gen",
    ))

    # ---------------- Early-exit gate ----------------
    # condition_task evaluates the staging_complete task value spark_runner sets. When staging is intact (all 19 staging tables present + all 730 per-day file dirs present), staging_complete="true" and this condition is FALSE → outcome="false" → downstream stage_tables / stage_files tasks all skip cleanly. When something's missing, staging_complete="false" and condition is TRUE → outcome="true" → downstream runs normally. cleanup_stage0 still fires either way (run_if=ALL_DONE) but DROP IF EXISTS is a no-op when nothing was created.
    tasks.append({
        "task_key": "staging_check",
        "depends_on": [{"task_key": "data_gen"}],
        "run_if": "ALL_SUCCESS",
        "condition_task": {
            "op": "EQUAL_TO",
            "left": "{{tasks.data_gen.values.staging_complete}}",
            "right": "false",
        },
        "timeout_seconds": 0,
        "email_notifications": {},
        "notification_settings": dict(_DEFAULT_NOTIF),
    })

    # ---------------- Stage 1a: raw_ingestion ----------------
    raw_ingest_path = f"{repo_src_path}/single_batch/SQL/raw_ingestion"

    tasks.append(_make_task(
        task_key="ingest_DimDate", notebook_path=raw_ingest_path,
        depends_on=[_NEEDS_REGEN],
        base_params={
            "raw_schema": f"sk_dateid BIGINT {_NN} COMMENT 'Surrogate key for the date', datevalue DATE COMMENT 'The date stored appropriately for doing comparisons in the Data Warehouse', datedesc STRING COMMENT 'The date in full written form e.g. July 7 2004', calendaryearid INT COMMENT 'Year number as a number', calendaryeardesc STRING COMMENT 'Year number as text', calendarqtrid INT COMMENT 'Quarter as a number e.g. 20042', calendarqtrdesc STRING COMMENT 'Quarter as text e.g. 2004 Q2', calendarmonthid INT COMMENT 'Month as a number e.g. 20047', calendarmonthdesc STRING COMMENT 'Month as text e.g. 2004 July', calendarweekid INT COMMENT 'Week as a number e.g. 200428', calendarweekdesc STRING COMMENT 'Week as text e.g. 2004-W28', dayofweeknum INT COMMENT 'Day of week as a number e.g. 3', dayofweekdesc STRING COMMENT 'Day of week as text e.g. Wednesday', fiscalyearid INT COMMENT 'Fiscal year as a number e.g. 2005', fiscalyeardesc STRING COMMENT 'Fiscal year as text e.g. 2005', fiscalqtrid INT COMMENT 'Fiscal quarter as a number e.g. 20051', fiscalqtrdesc STRING COMMENT 'Fiscal quarter as text e.g. 2005 Q1', holidayflag BOOLEAN COMMENT 'Indicates holidays'",
            "filename": "Date.txt",
            "tbl": "DimDate",
            "constraints": ", CONSTRAINT dimdate_pk PRIMARY KEY(sk_dateid)",
            "tbl_props": _tprops(),
            **_td_wh,
        },
    ))
    tasks.append(_make_task(
        task_key="ingest_DimTime", notebook_path=raw_ingest_path,
        depends_on=[_NEEDS_REGEN],
        base_params={
            "raw_schema": f"sk_timeid BIGINT {_NN} COMMENT 'Surrogate key for the time', timevalue STRING COMMENT 'The time stored appropriately for doing', hourid INT COMMENT 'Hour number as a number e.g. 01', hourdesc STRING COMMENT 'Hour number as text e.g. 01', minuteid INT COMMENT 'Minute as a number e.g. 23', minutedesc STRING COMMENT 'Minute as text e.g. 01:23', secondid INT COMMENT 'Second as a number e.g. 45', seconddesc STRING COMMENT 'Second as text e.g. 01:23:45', markethoursflag BOOLEAN COMMENT 'Indicates a time during market hours', officehoursflag BOOLEAN COMMENT 'Indicates a time during office hours'",
            "filename": "Time.txt",
            "tbl": "DimTime",
            "constraints": ", CONSTRAINT dimtime_pk PRIMARY KEY(sk_timeid)",
            "tbl_props": _tprops(),
            **_td_wh,
        },
    ))
    tasks.append(_make_task(
        task_key="ingest_StatusType", notebook_path=raw_ingest_path,
        depends_on=[_NEEDS_REGEN],
        base_params={
            "raw_schema": f"st_id STRING COMMENT 'Status code', st_name STRING {_NN} COMMENT 'Status description'",
            "filename": "StatusType.txt",
            "tbl": "StatusType",
            "constraints": ", CONSTRAINT statustype_pk PRIMARY KEY(st_name)",
            "tbl_props": _tprops(),
            **_td_wh,
        },
    ))
    tasks.append(_make_task(
        task_key="ingest_TaxRate", notebook_path=raw_ingest_path,
        depends_on=[_NEEDS_REGEN],
        base_params={
            "raw_schema": f"tx_id STRING {_NN} COMMENT 'Tax rate code', tx_name STRING COMMENT 'Tax rate description', tx_rate FLOAT COMMENT 'Tax rate'",
            "filename": "TaxRate.txt",
            "tbl": "TaxRate",
            "constraints": ", CONSTRAINT taxrate_pk PRIMARY KEY(tx_id)",
            "tbl_props": _tprops(),
            **_td_wh,
        },
    ))
    tasks.append(_make_task(
        task_key="ingest_TradeType", notebook_path=raw_ingest_path,
        depends_on=[_NEEDS_REGEN],
        base_params={
            "raw_schema": f"tt_id STRING {_NN} COMMENT 'Trade type code', tt_name STRING COMMENT 'Trade type description', tt_is_sell INT COMMENT 'Flag indicating a sale', tt_is_mrkt INT COMMENT 'Flag indicating a market order'",
            "filename": "TradeType.txt",
            "tbl": "TradeType",
            "constraints": ", CONSTRAINT tradetype_pk PRIMARY KEY(tt_id)",
            "tbl_props": _tprops(),
            **_td_wh,
        },
    ))
    tasks.append(_make_task(
        task_key="ingest_industry", notebook_path=raw_ingest_path,
        depends_on=[_NEEDS_REGEN],
        base_params={
            "raw_schema": f"in_id STRING COMMENT 'Industry code', in_name STRING {_NN} COMMENT 'Industry description', in_sc_id STRING COMMENT 'Sector identifier'",
            "filename": "Industry.txt",
            "tbl": "Industry",
            "constraints": ", CONSTRAINT industry_pk PRIMARY KEY(in_name)",
            "tbl_props": _tprops(),
            **_td_wh,
        },
    ))
    # BatchDate uses the Ingest_Incremental notebook (different file pattern).
    tasks.append(_make_task(
        task_key="ingest_BatchDate",
        notebook_path=f"{repo_src_path}/single_batch/SQL/Ingest_Incremental",
        depends_on=[_NEEDS_REGEN],
        base_params={
            "filename": "BatchDate.txt",
            "raw_schema": f"batchdate DATE {_NN} COMMENT 'Batch date'",
            "tbl": "BatchDate",
            "constraints": ", CONSTRAINT batchdate_pk PRIMARY KEY(batchdate)",
            "tbl_props": _tprops(),
            **_td_wh,
        },
    ))

    # ---------------- Stage 1a: ingest_FinWire ----------------
    tasks.append(_make_task(
        task_key="ingest_FinWire",
        notebook_path=f"{repo_src_path}/single_batch/SQL/ingest_finwire",
        depends_on=[_NEEDS_REGEN],
        base_params={"tbl_props": _finwire_tprops(), **_td_wh},
    ))

    # ---------------- Stage 1a: Silver static dims ----------------
    tasks.append(_make_task(
        task_key="Silver_DimBroker",
        notebook_path=f"{repo_src_path}/single_batch/SQL/DimBroker",
        depends_on=["ingest_DimDate"],
        base_params={
            "tgt_schema": f"sk_brokerid BIGINT {_NN} COMMENT 'Surrogate key for broker', brokerid BIGINT COMMENT 'Natural key for broker', managerid BIGINT COMMENT 'Natural key for manager\u2019s HR record', firstname STRING COMMENT 'First name', lastname STRING COMMENT 'Last Name', middleinitial STRING COMMENT 'Middle initial', branch STRING COMMENT 'Facility in which employee has office', office STRING COMMENT 'Office number or description', phone STRING COMMENT 'Employee phone number', iscurrent BOOLEAN COMMENT 'True if this is the current record', batchid INT COMMENT 'Batch ID when this record was inserted', effectivedate DATE COMMENT 'Beginning of date range when this record was the current record', enddate DATE COMMENT 'Ending of date range when this record was the current record. A record that is not expired will use the date 9999-12-31.'",
            "constraints": ", CONSTRAINT dimbroker_pk PRIMARY KEY(sk_brokerid)",
            "tbl_props": _tprops(),
            **_td_wh,
        },
    ))
    tasks.append(_make_task(
        task_key="Silver_DimCompany",
        notebook_path=f"{repo_src_path}/single_batch/SQL/DimCompany",
        depends_on=["ingest_FinWire", "ingest_industry"],
        base_params={
            "tgt_schema": f"sk_companyid BIGINT {_NN} COMMENT 'Surrogate key for CompanyID', companyid BIGINT COMMENT 'Company identifier (CIK number)', status STRING COMMENT 'Company status', name STRING COMMENT 'Company name', industry STRING COMMENT 'Company\u2019s industry', sprating STRING COMMENT 'Standard & Poor company\u2019s rating', islowgrade BOOLEAN COMMENT 'True if this company is low grade', ceo STRING COMMENT 'CEO name', addressline1 STRING COMMENT 'Address Line 1', addressline2 STRING COMMENT 'Address Line 2', postalcode STRING COMMENT 'Zip or postal code', city STRING COMMENT 'City', stateprov STRING COMMENT 'State or Province', country STRING COMMENT 'Country', description STRING COMMENT 'Company description', foundingdate DATE COMMENT 'Date the company was founded', iscurrent BOOLEAN COMMENT 'True if this is the current record', batchid INT COMMENT 'Batch ID when this record was inserted', effectivedate DATE COMMENT 'Beginning of date range when this record was the current record', enddate DATE COMMENT 'Ending of date range when this record was the current record. A record that is not expired will use the date 9999-12-31.'",
            "constraints": ", CONSTRAINT dimcompany_pk PRIMARY KEY(sk_companyid), CONSTRAINT dimcompany_status_fk FOREIGN KEY (status) REFERENCES StatusType(st_name), CONSTRAINT dimcompany_industry_fk FOREIGN KEY (industry) REFERENCES Industry(in_name)",
            "tbl_props": _tprops(),
            **_wh_param,
        },
    ))
    tasks.append(_make_task(
        task_key="Silver_DimSecurity",
        notebook_path=f"{repo_src_path}/single_batch/SQL/DimSecurity",
        depends_on=["Silver_DimCompany"],
        base_params={
            "tgt_schema": f"sk_securityid BIGINT {_NN} COMMENT 'Surrogate key for Symbol', symbol STRING COMMENT 'Identifies security on ticker', issue STRING COMMENT 'Issue type', status STRING COMMENT 'Status type', name STRING COMMENT 'Security name', exchangeid STRING COMMENT 'Exchange the security is traded on', sk_companyid BIGINT COMMENT 'Company issuing security', sharesoutstanding BIGINT COMMENT 'Shares outstanding', firsttrade DATE COMMENT 'Date of first trade', firsttradeonexchange DATE COMMENT 'Date of first trade on this exchange', dividend DOUBLE COMMENT 'Annual dividend per share', iscurrent BOOLEAN COMMENT 'True if this is the current record', batchid INT COMMENT 'Batch ID when this record was inserted', effectivedate DATE COMMENT 'Beginning of date range when this record was the current record', enddate DATE COMMENT 'Ending of date range when this record was the current record. A record that is not expired will use the date 9999-12-31.'",
            "constraints": ", CONSTRAINT dimsecurity_pk PRIMARY KEY(sk_securityid), CONSTRAINT dimsecurity_status_fk FOREIGN KEY (status) REFERENCES StatusType(st_name), CONSTRAINT dimsecurity_company_fk FOREIGN KEY (sk_companyid) REFERENCES DimCompany(sk_companyid)",
            "tbl_props": _tprops(),
            **_wh_param,
        },
    ))
    tasks.append(_make_task(
        task_key="Silver_Financial",
        notebook_path=f"{repo_src_path}/single_batch/SQL/Financial",
        depends_on=["Silver_DimCompany"],
        base_params={
            "tgt_schema": f"sk_companyid BIGINT {_NN} COMMENT 'Company SK.', fi_year INT {_NN} COMMENT 'Year of the quarter end.', fi_qtr INT {_NN} COMMENT 'Quarter number that the financial information is for: valid values 1, 2, 3, 4.', fi_qtr_start_date DATE COMMENT 'Start date of quarter.', fi_revenue DOUBLE COMMENT 'Reported revenue for the quarter.', fi_net_earn DOUBLE COMMENT 'Net earnings reported for the quarter.', fi_basic_eps DOUBLE COMMENT 'Basic earnings per share for the quarter.', fi_dilut_eps DOUBLE COMMENT 'Diluted earnings per share for the quarter.', fi_margin DOUBLE COMMENT 'Profit divided by revenues for the quarter.', fi_inventory DOUBLE COMMENT 'Value of inventory on hand at the end of quarter.', fi_assets DOUBLE COMMENT 'Value of total assets at the end of the quarter.', fi_liability DOUBLE COMMENT 'Value of total liabilities at the end of the quarter.', fi_out_basic BIGINT COMMENT 'Average number of shares outstanding (basic).', fi_out_dilut BIGINT COMMENT 'Average number of shares outstanding (diluted).'",
            "constraints": ", CONSTRAINT financial_pk PRIMARY KEY(sk_companyid, fi_year, fi_qtr), CONSTRAINT financial_company_fk FOREIGN KEY (sk_companyid) REFERENCES DimCompany(sk_companyid)",
            "tbl_props": _tprops(),
            **_wh_param,
        },
    ))

    # ---------------- Stage 1a: SCD2 historical dim/facts ----------------
    hist_path = f"{repo_src_path}/incremental_batches/augmented_incremental/historical"

    # Each historical task writes to {wh_db}_{sf}.{table} — wh_db is hardcoded to the shared staging schema, no user knob.
    tasks.append(_make_task(
        task_key="DimCustomerHistorical",
        notebook_path=f"{hist_path}/DimCustomerHistorical",
        depends_on=[_NEEDS_REGEN, "ingest_TaxRate"],
        base_params=dict(_wh_param),
    ))
    tasks.append(_make_task(
        task_key="DimAccountHistorical",
        notebook_path=f"{hist_path}/DimAccountHistorical",
        depends_on=["DimCustomerHistorical", "Silver_DimBroker"],
        base_params=dict(_wh_param),
    ))
    tasks.append(_make_task(
        task_key="DimTradeHistorical",
        notebook_path=f"{hist_path}/DimTradeHistorical",
        depends_on=[_NEEDS_REGEN, "Silver_DimSecurity", "DimAccountHistorical"],
        base_params=dict(_wh_param),
    ))
    tasks.append(_make_task(
        task_key="FactCashBalancesHistorical",
        notebook_path=f"{hist_path}/FactCashBalancesHistorical",
        depends_on=[_NEEDS_REGEN, "DimAccountHistorical"],
        base_params=dict(_wh_param),
    ))
    tasks.append(_make_task(
        task_key="FactHoldingsHistorical",
        notebook_path=f"{hist_path}/FactHoldingsHistorical",
        depends_on=[_NEEDS_REGEN, "DimTradeHistorical"],
        base_params=dict(_wh_param),
    ))
    tasks.append(_make_task(
        task_key="FactWatchesHistorical",
        notebook_path=f"{hist_path}/FactWatchesHistorical",
        depends_on=[_NEEDS_REGEN, "Silver_DimSecurity", "DimCustomerHistorical"],
        base_params=dict(_wh_param),
    ))
    tasks.append(_make_task(
        task_key="CompanyYearEPS",
        notebook_path=f"{hist_path}/CompanyYearEPS",
        depends_on=["Silver_Financial", "Silver_DimCompany"],
        base_params=dict(_wh_param),
    ))

    # ---------------- Stage 1b: stage_files (per-day CSVs) ----------------
    stg_path = f"{repo_src_path}/tools/augmented_staging/stage_files"
    stage_files_keys: list[str] = []
    for tbl in ["Customer", "Account", "Trade", "CashTransaction",
                "HoldingHistory", "DailyMarket", "WatchHistory"]:
        key = f"stage_files_{tbl}"
        tasks.append(_make_task(
            task_key=key,
            notebook_path=f"{stg_path}/{tbl}",
            depends_on=[_NEEDS_REGEN],
            base_params=dict(_td_param),
        ))
        stage_files_keys.append(key)

    # ---------------- Cleanup: drop the 7 temp Delta tables ----------------
    # Depends on every leaf in both branches. ALL_DONE so partial-failure
    # runs still get the cleanup. Not gated by delete_tables_when_finished
    # because the temp Delta tables are unconditionally temporary.
    cleanup_deps = stage_files_keys + [
        "DimAccountHistorical", "DimTradeHistorical",
        "FactCashBalancesHistorical", "FactHoldingsHistorical",
        "FactWatchesHistorical", "CompanyYearEPS",
    ]
    tasks.append(_make_task(
        task_key="cleanup_stage0",
        notebook_path=f"{repo_src_path}/tools/augmented_staging/cleanup_stage0",
        depends_on=cleanup_deps,
        run_if="ALL_DONE",
    ))

    # ---------------- Cleanup: delete per-day dirs on file-gen failure ----
    # Only fires when at least one stage_files task failed. Wipes the
    # augmented_incremental/_staging/sf={sf}/ tree so a stale partial set
    # of date dirs (e.g. 6 of 7 stage_files succeeded) doesn't trick the
    # next run's early-exit check (730 dirs present) into skipping the
    # rebuild.
    tasks.append(_make_task(
        task_key="cleanup_files_on_failure",
        notebook_path=f"{repo_src_path}/tools/augmented_staging/cleanup_files_on_failure",
        depends_on=stage_files_keys,
        run_if="AT_LEAST_ONE_FAILED",
    ))

    # ---------------- Top-level workflow ----------------
    workflow: dict[str, Any] = {
        "name": job_name,
        "description": _description(scale_factor, catalog),
        "tags": {"data_generator": "augmented_incremental"},
        "email_notifications": {"no_alert_for_skipped_runs": False},
        "webhook_notifications": {},
        "timeout_seconds": 0,
        "max_concurrent_runs": 1,
        "performance_target": "PERFORMANCE_OPTIMIZED",
        "parameters": [
            # data_gen task params
            {"name": "data_gen_type", "default": "augmented_incremental"},
            {"name": "scale_factor", "default": str(scale_factor)},
            {"name": "catalog", "default": catalog},
            {"name": "regenerate_data", "default": regenerate_data},
            {"name": "log_level", "default": log_level},
            # tpcdi_directory and wh_db are NOT job-level params — they're hardcoded per-task via base_parameters since data_gen_type=augmented_incremental fully determines both. predictive_optimization is dropped (PO is not enabled on the shared staging schema).
        ],
        "queue": {"enabled": True},
        "tasks": tasks,
    }
    return workflow
