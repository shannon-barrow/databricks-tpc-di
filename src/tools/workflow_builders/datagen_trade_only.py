"""Builder for an isolated trade-family copy-strategy perf-testing workflow.

A 6-task DAG used to A/B test trade-family copy strategies in isolation,
without the resource contention of the full datagen DAG:

    data_gen   (regenerate_data=NO baked in → idempotent: ensures
      │        _stage schema, tpcdi_raw_data + volume, Batch1/2/3 dirs
      │        all exist. Skips the volume wipe entirely.)
      ↓
    gen_trade  (regenerate_data=YES from job param → always rewrites
      │        the Trade-family staging dirs.)
      ↓
    copy_trade / copy_tradehistory / copy_cashtransaction / copy_holdinghistory
               (parallel siblings, one per dataset.)

``regenerate_data=NO`` is hard-baked on data_gen so the job-level
``regenerate_data=YES`` only affects gen_trade. ``cleanup_intermediates``
is deliberately omitted so ``_gen_symbols`` persists across iterations.

prepare_symbols (gen_finwire) was previously a precursor task — it
self-skipped when ``_gen_symbols`` Delta existed but its bootstrap
(sys.path, dictionaries, cfg, table check) cost ~2m of critical path
every run anyway. Removed: this harness assumes a prior full datagen
run has populated ``_gen_symbols`` (one-time setup outside this
workflow). If missing, gen_trade will fail fast.
"""
from __future__ import annotations

# Reuse cluster + helper functions from the main datagen_spark builder so
# any change to cluster sizing / cloud attrs there flows here too.
try:
    from workflow_builders.datagen_spark import (
        _spark_cluster_spec, _cloud_attrs, _make_task, _DEFAULT_NOTIF,
    )
except ImportError:
    from datagen_spark import (  # type: ignore
        _spark_cluster_spec, _cloud_attrs, _make_task, _DEFAULT_NOTIF,
    )


def build(*, job_name: str, scale_factor: int, catalog: str,
          regenerate_data: str, log_level: str, repo_src_path: str,
          serverless: str = "YES",
          node_types: dict | None = None,
          cloud_provider: str | None = None,
          default_worker_type: str | None = None,
          default_dbr_version: str | None = None,
          wh_db: str = "tpcdi_incremental_staging",
          **_unused) -> dict:
    # Spark output goes under spark_datagen/ subdir to avoid clobbering DIGen
    # output (which uses the unprefixed sf=N/ path).
    tpcdi_directory = f"/Volumes/{catalog}/tpcdi_raw_data/tpcdi_volume/spark_datagen/"
    is_serverless = (serverless or "YES").upper() == "YES"

    if is_serverless:
        cluster_blurb = "serverless"
        job_cluster_key = None
        job_clusters = None
    else:
        cluster_spec = _spark_cluster_spec(
            scale_factor, node_types or {}, cloud_provider or "",
            default_worker_type or "",
            default_dbr_version or "",
            _cloud_attrs,
        )
        if cluster_spec["num_workers"] == 0:
            cluster_blurb = f"non-serverless single-node ({cluster_spec['node_type_id']})"
        else:
            cluster_blurb = (
                f"non-serverless: {cluster_spec['driver_node_type_id']} driver + "
                f"{cluster_spec['num_workers']} × {cluster_spec['node_type_id']} workers"
            )
        job_cluster_key = f"{job_name}_compute"
        job_clusters = [{"job_cluster_key": job_cluster_key,
                         "new_cluster": cluster_spec}]

    description = (
        f"TPC-DI **Trade-only perf harness** (SF={scale_factor}, "
        f"{cluster_blurb}). Two-task DAG: gen_trade → copy_trade. "
        f"Assumes a prior full datagen run already created _gen_symbols in "
        f"`{catalog}.{wh_db}_{scale_factor}_stage` and Batch1/2/3 dirs in "
        f"`{tpcdi_directory}sf={scale_factor}/`. Used to A/B test trade "
        f"copy strategies without other-dataset contention. Set "
        f"`regenerate_data=YES` to force gen_trade to rewrite staging."
    )

    _base = {
        "tpcdi_directory": tpcdi_directory,
        "augmented_incremental": "false",
        "wh_db": wh_db,
    }
    # data_gen pins regenerate_data=NO so the job-level
    # `regenerate_data=YES` only affects gen_trade. data_gen still runs
    # idempotent setup (schemas, volume, Batch dirs) without the wipe.
    _setup_base = {**_base, "regenerate_data": "NO"}
    _dgt = f"{repo_src_path}/tools/data_gen_tasks"

    tasks = [
        _make_task(
            task_key="data_gen",
            notebook_path=f"{_dgt}/data_gen",
            base_params=_setup_base,
            job_cluster_key=job_cluster_key,
        ),
        _make_task(
            task_key="gen_trade",
            notebook_path=f"{_dgt}/gen_trade",
            depends_on=["data_gen"],
            base_params=_base,
            job_cluster_key=job_cluster_key,
        ),
    ]
    # 4 sibling copy tasks, each touching one trade-family dataset.
    # No copy-side cross-dependency — they run in parallel after gen_trade.
    for _name in ("copy_trade", "copy_tradehistory",
                  "copy_cashtransaction", "copy_holdinghistory"):
        tasks.append(_make_task(
            task_key=_name,
            notebook_path=f"{_dgt}/{_name}",
            depends_on=["gen_trade"],
            base_params=_base,
            job_cluster_key=job_cluster_key,
        ))

    payload = {
        "name": job_name,
        "description": description,
        "tags": {"data_generator": "spark", "perf_harness": "trade_only"},
        "email_notifications": {"no_alert_for_skipped_runs": False},
        "webhook_notifications": {},
        "timeout_seconds": 0,
        "max_concurrent_runs": 1,
        "parameters": [
            {"name": "data_gen_type", "default": "spark"},
            {"name": "scale_factor", "default": str(scale_factor)},
            {"name": "catalog", "default": catalog},
            {"name": "regenerate_data", "default": regenerate_data},
            {"name": "log_level", "default": log_level},
        ],
        "tasks": tasks,
        "queue": {"enabled": True},
    }
    if is_serverless:
        payload["performance_target"] = "PERFORMANCE_OPTIMIZED"
    else:
        payload["job_clusters"] = job_clusters
    return payload
