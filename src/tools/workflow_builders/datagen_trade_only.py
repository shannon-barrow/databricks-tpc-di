"""Builder for an isolated `gen_trade` → `copy_trade` perf-testing workflow.

This is a stripped-down 2-task DAG used to A/B test trade-family copy
strategies in isolation, without the resource contention of the full
datagen DAG. Assumes a prior full datagen run already populated:

  - ``{wh_db}_{sf}_stage._gen_symbols`` Delta (produced by gen_finwire)
  - ``{volume}/sf={sf}/Batch{1,2,3}/`` directories (produced by data_gen)

Neither is recreated here — that's the point. The user pre-populates
those once, then iterates on copy strategies inside ``copy_trade``.

Layout:

    gen_trade   (regenerate_data=YES → rewrites the staging dirs)
      └── copy_trade   (the strategy being measured)

No ``data_gen``, no ``cleanup_intermediates`` — _gen_symbols stays
intact across iterations.
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
    tpcdi_directory = f"/Volumes/{catalog}/tpcdi_raw_data/tpcdi_volume/"
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
    _dgt = f"{repo_src_path}/tools/data_gen_tasks"

    tasks = [
        _make_task(
            task_key="gen_trade",
            notebook_path=f"{_dgt}/gen_trade",
            base_params=_base,
            job_cluster_key=job_cluster_key,
        ),
        _make_task(
            task_key="copy_trade",
            notebook_path=f"{_dgt}/copy_trade",
            depends_on=["gen_trade"],
            base_params=_base,
            job_cluster_key=job_cluster_key,
        ),
    ]

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
