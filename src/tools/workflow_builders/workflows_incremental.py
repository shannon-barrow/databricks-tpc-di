"""Builder for the CLUSTER/DBSQL incremental-batch benchmark workflow.

Replaces `jinja_templates/workflows_incremental.json`. This is the
auditable variant of the benchmark — runs all 3 TPC-DI batches with
audit checks at the end of each. ~50 tasks; the per-batch incremental
group repeats with just `batch_id` differing, so we loop over those.
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
            f"TPC-DI incremental-batch workflow on {exec_type} (**Spark** "
            f"data generator, SF={scale_factor}). Reads distributed PySpark "
            f"output from `{tpcdi_directory}sf={scale_factor}/` — split files "
            f"per table (e.g. `Customer_1.txt`, `Customer_2.txt`, ...). "
            f"Writes to schema `{catalog}.{wh_target}_{exec_type}_spark_data_"
            f"gen_incremental_{scale_factor}` (and `..._stage`). Processes 3 "
            f"TPC-DI batches sequentially with audit checks at the end of "
            f"each batch — this is the auditable variant required for spec "
            f"validation."
        )
    return (
        f"TPC-DI incremental-batch workflow on {exec_type} (**DIGen.jar** "
        f"native, legacy data generator, SF={scale_factor}). Reads "
        f"single-threaded DIGen output from `{tpcdi_directory}sf={scale_factor}"
        f"/` — one file per table (e.g. `Customer.txt`, `Trade.txt`). Writes "
        f"to schema `{catalog}.{wh_target}_{exec_type}_native_data_gen_"
        f"incremental_{scale_factor}` (and `..._stage`). Processes 3 TPC-DI "
        f"batches sequentially with audit checks at the end of each batch — "
        f"this is the auditable variant required for spec validation."
    )


def _audit_validation_task(*, batch_id: int, repo_src_path: str,
                           job_name: str, exec_type: str, serverless: str,
                           wh_id: str | None, depends_on: list[str]) -> dict:
    return common.make_task(
        task_key=f"batch{batch_id}_validation",
        notebook_path=f"{repo_src_path}/incremental_batches/audit_validation/batch_validation",
        depends_on=depends_on,
        base_params={"batch_id": str(batch_id)},
        exec_type=exec_type, serverless=serverless,
        job_name=job_name, wh_id=wh_id,
    )


def _audit_complete_task(*, batch_id: int, repo_src_path: str,
                         job_name: str, exec_type: str, serverless: str,
                         wh_id: str | None, depends_on: list[str]) -> dict:
    return common.make_task(
        task_key=f"batch{batch_id}_complete",
        notebook_path=f"{repo_src_path}/incremental_batches/audit_validation/batch_complete",
        depends_on=depends_on,
        base_params={"batch_id": str(batch_id)},
        exec_type=exec_type, serverless=serverless,
        job_name=job_name, wh_id=wh_id,
    )


def _per_batch_tasks(*, batch_id: int, batch_dep: str, repo_src_path: str,
                     job_name: str, exec_type: str, serverless: str,
                     wh_id: str | None, market_history_dep: str) -> list[dict]:
    """Build the per-incremental-batch task group (batch 2 or 3).

    Each batch performs the same shape: DimCustomer → Prospect, DimAccount
    → DimTrade, FactWatches, FactCashBalances; FactMarketHistory branches
    off the prior batch's validation directly.
    """
    bp = {"batch_id": str(batch_id)}

    def t(task_key, notebook_path, depends_on):
        return common.make_task(
            task_key=task_key, notebook_path=notebook_path,
            depends_on=depends_on, base_params=bp,
            exec_type=exec_type, serverless=serverless,
            job_name=job_name, wh_id=wh_id,
        )

    silver = f"{repo_src_path}/incremental_batches/silver"
    gold = f"{repo_src_path}/incremental_batches/gold"
    return [
        t(f"Batch{batch_id}_DimCustomer", f"{silver}/DimCustomer Incremental", [batch_dep]),
        t(f"Batch{batch_id}_Prospect",   f"{silver}/Prospect",              [f"Batch{batch_id}_DimCustomer"]),
        t(f"Batch{batch_id}_DimAccount", f"{silver}/DimAccount Incremental", [f"Batch{batch_id}_DimCustomer"]),
        t(f"Batch{batch_id}_DimTrade",   f"{silver}/DimTrade Incremental",   [f"Batch{batch_id}_DimAccount"]),
        t(f"Batch{batch_id}_FactWatches",  f"{silver}/FactWatches Incremental", [f"Batch{batch_id}_DimCustomer"]),
        t(f"Batch{batch_id}_FactCashBalances", f"{gold}/FactCashBalances Incremental", [f"Batch{batch_id}_DimAccount"]),
        t(f"Batch{batch_id}_FactMarketHistory", f"{gold}/FactMarketHistory Incremental", [market_history_dep]),
    ]


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

    bronze = f"{repo_src_path}/incremental_batches/bronze"
    silver = f"{repo_src_path}/incremental_batches/silver"
    gold = f"{repo_src_path}/incremental_batches/gold"
    audit = f"{repo_src_path}/incremental_batches/audit_validation"

    def t(**kwargs):
        return common.make_task(
            exec_type=exec_type, serverless=serverless,
            job_name=job_name, wh_id=wh_id, **kwargs,
        )

    # ---------- Tasks ----------
    tasks: list[dict] = []

    # dw_init + batch0
    tasks.append(t(
        task_key="dw_init",
        notebook_path=f"{repo_src_path}/incremental_batches/dw_init",
        base_params={"pred_opt": "{{job.parameters.predictive_optimization}}"},
    ))
    tasks.append(_audit_validation_task(
        batch_id=0, repo_src_path=repo_src_path, job_name=job_name,
        exec_type=exec_type, serverless=serverless, wh_id=wh_id,
        depends_on=["dw_init"],
    ))
    tasks.append(_audit_complete_task(
        batch_id=0, repo_src_path=repo_src_path, job_name=job_name,
        exec_type=exec_type, serverless=serverless, wh_id=wh_id,
        depends_on=["batch0_validation"],
    ))

    # ingest_customermgmt — gates on data_generator + scale_factor
    tasks.append(common.make_customermgmt_task(
        repo_src_path=repo_src_path, job_name=job_name,
        data_generator=data_generator, scale_factor=scale_factor,
        serverless=serverless, exec_type=exec_type, wh_id=wh_id,
        depends_on=["batch0_validation"],
        modern_notebook_path=f"{bronze}/CustomerMgmtRaw",
    ))

    # Bronze ingest tasks
    raw_ingest = f"{bronze}/raw_ingestion"
    tasks.append(t(
        task_key="ingest_DimDate", notebook_path=raw_ingest,
        depends_on=["batch0_validation"], run_if="AT_LEAST_ONE_SUCCESS",
        base_params={
            "raw_schema": "sk_dateid BIGINT, datevalue DATE, datedesc STRING, calendaryearid INT, calendaryeardesc STRING, calendarqtrid INT, calendarqtrdesc STRING, calendarmonthid INT, calendarmonthdesc STRING, calendarweekid INT, calendarweekdesc STRING, dayofweeknum INT, dayofweekdesc STRING, fiscalyearid INT, fiscalyeardesc STRING, fiscalqtrid INT, fiscalqtrdesc STRING, holidayflag BOOLEAN",
            "filename": "Date.txt",
            "tbl": "DimDate",
        },
    ))
    tasks.append(t(
        task_key="ingest_FinWire",
        notebook_path=f"{bronze}/ingest_finwire",
        depends_on=["batch0_validation"], run_if="AT_LEAST_ONE_SUCCESS",
    ))
    tasks.append(t(
        task_key="ingest_DimTime", notebook_path=raw_ingest,
        depends_on=["ingest_FinWire"], run_if="AT_LEAST_ONE_SUCCESS",
        base_params={
            "raw_schema": "sk_timeid BIGINT, timevalue STRING, hourid INT, hourdesc STRING, minuteid INT, minutedesc STRING, secondid INT, seconddesc STRING, markethoursflag BOOLEAN, officehoursflag BOOLEAN",
            "filename": "Time.txt",
            "tbl": "DimTime",
        },
    ))
    tasks.append(t(
        task_key="ingest_StatusType", notebook_path=raw_ingest,
        depends_on=["ingest_FinWire"], run_if="AT_LEAST_ONE_SUCCESS",
        base_params={
            "raw_schema": "st_id STRING, st_name STRING",
            "filename": "StatusType.txt",
            "tbl": "StatusType",
        },
    ))
    tasks.append(t(
        task_key="ingest_TaxRate", notebook_path=raw_ingest,
        depends_on=["batch0_validation"], run_if="AT_LEAST_ONE_SUCCESS",
        base_params={
            "raw_schema": "tx_id STRING, tx_name STRING, tx_rate FLOAT",
            "filename": "TaxRate.txt",
            "tbl": "TaxRate",
        },
    ))
    tasks.append(t(
        task_key="ingest_TradeType", notebook_path=raw_ingest,
        depends_on=["ingest_FinWire"], run_if="AT_LEAST_ONE_SUCCESS",
        base_params={
            "raw_schema": "tt_id STRING, tt_name STRING, tt_is_sell INT, tt_is_mrkt INT",
            "filename": "TradeType.txt",
            "tbl": "TradeType",
        },
    ))
    tasks.append(t(
        task_key="ingest_industry", notebook_path=raw_ingest,
        depends_on=["batch0_validation"], run_if="AT_LEAST_ONE_SUCCESS",
        base_params={
            "raw_schema": "in_id STRING, in_name STRING, in_sc_id STRING",
            "filename": "Industry.txt",
            "tbl": "Industry",
        },
    ))
    tasks.append(t(
        task_key="ingest_ProspectIncremental",
        notebook_path=f"{bronze}/ingest_prospectincremental",
        depends_on=["batch0_validation"], run_if="AT_LEAST_ONE_SUCCESS",
    ))
    tasks.append(t(
        task_key="ingest_DailyMarketIncremental",
        notebook_path=f"{bronze}/ingest_dailymarketincremental",
        depends_on=["batch0_validation"], run_if="AT_LEAST_ONE_SUCCESS",
    ))
    tasks.append(t(
        task_key="ingest_BatchDate",
        notebook_path=f"{bronze}/Ingest_Incremental",
        depends_on=["batch0_validation"], run_if="AT_LEAST_ONE_SUCCESS",
        base_params={
            "filename": "BatchDate.txt",
            "raw_schema": "batchdate DATE",
            "tbl": "BatchDate",
        },
    ))

    # Silver historical
    tasks.append(t(
        task_key="Silver_DimBroker", notebook_path=f"{silver}/DimBroker",
        depends_on=["ingest_DimDate"],
    ))
    tasks.append(t(
        task_key="Historical_DimCustomer",
        notebook_path=f"{silver}/DimCustomer Historical",
        depends_on=["ingest_customermgmt", "ingest_ProspectIncremental",
                    "ingest_TaxRate"],
    ))
    tasks.append(t(
        task_key="Historical_DimAccount",
        notebook_path=f"{silver}/DimAccount Historical",
        depends_on=["Silver_DimBroker", "Historical_DimCustomer"],
    ))
    tasks.append(t(
        task_key="Historical_FactCashBalances",
        notebook_path=f"{gold}/FactCashBalances Historical",
        depends_on=["Historical_DimAccount"],
    ))
    tasks.append(t(
        task_key="Batch1_Prospect", notebook_path=f"{silver}/Prospect",
        depends_on=["Historical_DimCustomer", "ingest_BatchDate"],
        base_params={"batch_id": "1"},
    ))
    tasks.append(t(
        task_key="Silver_DimCompany", notebook_path=f"{silver}/DimCompany",
        depends_on=["ingest_FinWire", "ingest_industry"],
    ))
    tasks.append(t(
        task_key="Silver_DimSecurity", notebook_path=f"{silver}/DimSecurity",
        depends_on=["Silver_DimCompany"],
    ))
    tasks.append(t(
        task_key="Historical_FactWatches",
        notebook_path=f"{silver}/FactWatches Historical",
        depends_on=["Silver_DimSecurity", "Historical_DimCustomer"],
    ))
    tasks.append(t(
        task_key="Historical_DimTrade",
        notebook_path=f"{silver}/DimTrade Historical",
        depends_on=["Silver_DimSecurity", "Historical_DimAccount"],
        base_params={"wh_timezone": ""},
    ))
    tasks.append(t(
        task_key="Silver_Financial", notebook_path=f"{silver}/Financial",
        depends_on=["Silver_DimCompany"],
    ))
    tasks.append(t(
        task_key="Historical_FactMarketHistory",
        notebook_path=f"{gold}/FactMarketHistory Historical",
        depends_on=["Silver_DimSecurity", "Silver_Financial",
                    "ingest_DailyMarketIncremental"],
    ))

    # Batch 1 audit + complete
    tasks.append(_audit_validation_task(
        batch_id=1, repo_src_path=repo_src_path, job_name=job_name,
        exec_type=exec_type, serverless=serverless, wh_id=wh_id,
        depends_on=[
            "Historical_FactCashBalances", "Historical_DimTrade",
            "Historical_FactWatches", "Historical_FactMarketHistory",
            "ingest_DimTime", "ingest_StatusType", "ingest_TradeType",
            "Batch1_Prospect",
        ],
    ))
    tasks.append(_audit_complete_task(
        batch_id=1, repo_src_path=repo_src_path, job_name=job_name,
        exec_type=exec_type, serverless=serverless, wh_id=wh_id,
        depends_on=["batch1_validation"],
    ))

    # Batch 2 (depends on batch1_validation; its FactMarketHistory branches
    # off batch1_validation directly per the original DAG).
    tasks.extend(_per_batch_tasks(
        batch_id=2, batch_dep="batch1_validation",
        repo_src_path=repo_src_path, job_name=job_name,
        exec_type=exec_type, serverless=serverless, wh_id=wh_id,
        market_history_dep="batch1_validation",
    ))
    tasks.append(_audit_validation_task(
        batch_id=2, repo_src_path=repo_src_path, job_name=job_name,
        exec_type=exec_type, serverless=serverless, wh_id=wh_id,
        depends_on=[
            "Batch2_FactMarketHistory", "Batch2_FactCashBalances",
            "Batch2_FactWatches", "Batch2_DimTrade", "Batch2_Prospect",
        ],
    ))
    tasks.append(_audit_complete_task(
        batch_id=2, repo_src_path=repo_src_path, job_name=job_name,
        exec_type=exec_type, serverless=serverless, wh_id=wh_id,
        depends_on=["batch2_validation"],
    ))

    # Batch 3 (depends on batch2_complete; FactMarketHistory branches off
    # batch2_validation per the original DAG).
    tasks.extend(_per_batch_tasks(
        batch_id=3, batch_dep="batch2_complete",
        repo_src_path=repo_src_path, job_name=job_name,
        exec_type=exec_type, serverless=serverless, wh_id=wh_id,
        market_history_dep="batch2_validation",
    ))
    # Batch3 ordering differs: batch3_complete is BEFORE batch3_validation.
    tasks.append(_audit_complete_task(
        batch_id=3, repo_src_path=repo_src_path, job_name=job_name,
        exec_type=exec_type, serverless=serverless, wh_id=wh_id,
        depends_on=[
            "Batch3_FactMarketHistory", "Batch3_FactCashBalances",
            "Batch3_FactWatches", "Batch3_DimTrade", "Batch3_Prospect",
        ],
    ))
    tasks.append(_audit_validation_task(
        batch_id=3, repo_src_path=repo_src_path, job_name=job_name,
        exec_type=exec_type, serverless=serverless, wh_id=wh_id,
        depends_on=["batch3_complete"],
    ))

    # Audit alerts + automated audit (final tasks)
    tasks.append(t(
        task_key="audit_alerts",
        notebook_path=f"{audit}/audit_alerts",
        depends_on=["Batch3_FactMarketHistory", "Batch3_DimTrade"],
    ))
    tasks.append(t(
        task_key="automated_audit",
        notebook_path=f"{audit}/automated_audit",
        depends_on=["audit_alerts", "batch3_validation"],
    ))

    # ---------- Top-level workflow ----------
    workflow: dict[str, Any] = {
        "name": job_name,
        "description": _description(
            data_generator=data_generator, exec_type=exec_type,
            scale_factor=scale_factor, catalog=catalog, wh_target=wh_target,
            tpcdi_directory=tpcdi_directory,
        ),
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

    # Cluster choice — same logic as single_batch.
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

    # Final cleanup task — drops the run's schemas if delete_tables_when_finished=TRUE.
    # automated_audit is the last "real" task; cleanup waits on ALL_DONE so a
    # partial-failure run still gets cleaned up.
    tasks.append(common.make_cleanup_task(
        repo_src_path=repo_src_path, job_name=job_name,
        exec_type=exec_type, serverless=serverless, wh_id=wh_id,
        depends_on=["automated_audit"],
    ))

    workflow["tasks"] = tasks
    return workflow
