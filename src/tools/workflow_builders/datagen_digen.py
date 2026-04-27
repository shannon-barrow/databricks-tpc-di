"""Builder for the DIGen.jar (native, legacy) data-generation workflow.

Replaces `jinja_templates/datagen_workflow_digen.json`. Produces a dict
suitable for POST to `/api/2.1/jobs/create`.

Cluster: forced **non-serverless DBR 15.4 + Photon**, regardless of the
user's overall serverless choice (DIGen.jar is a Java subprocess that
can't run on serverless, and the audit logic relies on DBR 15.4 behaviors).
The node type is just the workspace default for now.

Worker count scales with scale_factor — single-node up through SF=1000,
then 1 worker per 1000 of scale_factor at higher SFs:

  scale_factor   num_workers   shape
  ------------   -----------   --------------
  ≤ 1000         0             single-node
  5000           5             driver + 5 workers
  10000          10            driver + 10 workers
  20000          20            driver + 20 workers
"""
from __future__ import annotations


_DIGEN_DBR = "15.4.x-scala2.12"


def _cluster_spec(scale_factor: int, default_worker_type: str) -> dict:
    """Forced DBR 15.4 + Photon. Single-node up through SF=1000, then
    +1 × default-type worker per 1000 of scale_factor."""
    if scale_factor <= 1000:
        return {
            "spark_version": _DIGEN_DBR,
            "spark_conf": {
                "spark.master": "local[*, 4]",
                "spark.databricks.cluster.profile": "singleNode",
            },
            "node_type_id": default_worker_type,
            "custom_tags": {"ResourceClass": "SingleNode"},
            "data_security_mode": "SINGLE_USER",
            "runtime_engine": "PHOTON",
            "num_workers": 0,
        }

    return {
        "spark_version": _DIGEN_DBR,
        "spark_conf": {"spark.sql.shuffle.partitions": "auto"},
        "driver_node_type_id": default_worker_type,
        "node_type_id": default_worker_type,
        "data_security_mode": "SINGLE_USER",
        "runtime_engine": "PHOTON",
        "num_workers": scale_factor // 1000,
    }


def build(*, job_name: str, scale_factor: int, catalog: str,
          regenerate_data: str, repo_src_path: str,
          default_worker_type: str,
          **_unused) -> dict:
    """Build the DIGen data-generation Jobs API payload.

    Extra kwargs are silently accepted to match the existing call interface
    where the caller passes an over-broad `dag_args` dict.
    """
    tpcdi_directory = f"/Volumes/{catalog}/tpcdi_raw_data/tpcdi_volume/"
    cluster = _cluster_spec(scale_factor, default_worker_type)
    if cluster["num_workers"] == 0:
        cluster_blurb = f"single-node ({default_worker_type})"
    else:
        cluster_blurb = (
            f"driver + {cluster['num_workers']} workers, all {default_worker_type}"
        )
    description = (
        f"TPC-DI **DIGen.jar** (native, legacy) data-generation workflow "
        f"(SF={scale_factor}). Single-threaded Java utility — runs on a "
        f"non-serverless **DBR {_DIGEN_DBR}** Photon cluster ({cluster_blurb}); "
        f"forced regardless of the overall job's serverless setting since "
        f"a Java subprocess cannot run on serverless. Writes raw input "
        f"files to `{tpcdi_directory}sf={scale_factor}/`. Outputs are single "
        f"files per table (`Customer.txt`, `Trade.txt`, etc.). Set "
        f"`regenerate_data=YES` to wipe and rebuild; defaults to `NO` "
        f"(no-op if output already exists)."
    )

    return {
        "name": f"{job_name}_DataGen",
        "description": description,
        "email_notifications": {"no_alert_for_skipped_runs": False},
        "webhook_notifications": {},
        "timeout_seconds": 0,
        "max_concurrent_runs": 1,
        "parameters": [
            {"name": "scale_factor", "default": str(scale_factor)},
            {"name": "catalog", "default": catalog},
            {"name": "regenerate_data", "default": regenerate_data},
            {"name": "tpcdi_directory", "default": tpcdi_directory},
        ],
        "tasks": [{
            "task_key": "generate_data",
            "run_if": "ALL_SUCCESS",
            "notebook_task": {
                "notebook_path": f"{repo_src_path}/tools/data_generator",
                "source": "WORKSPACE",
            },
            "job_cluster_key": "digen_cluster",
            "timeout_seconds": 0,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": False,
                "no_alert_for_canceled_runs": False,
                "alert_on_last_attempt": False,
            },
        }],
        "job_clusters": [{
            "job_cluster_key": "digen_cluster",
            "new_cluster": cluster,
        }],
        "queue": {"enabled": True},
    }
