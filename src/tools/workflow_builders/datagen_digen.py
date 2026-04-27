"""Builder for the DIGen.jar (native, legacy) data-generation workflow.

Replaces `jinja_templates/datagen_workflow_digen.json`. Produces a dict
suitable for POST to `/api/2.1/jobs/create`.
"""
from __future__ import annotations


def build(*, job_name: str, scale_factor: int, catalog: str,
          regenerate_data: str, repo_src_path: str,
          default_dbr_version: str, default_worker_type: str,
          **_unused) -> dict:
    """Build the DIGen data-generation Jobs API payload.

    Extra kwargs are silently accepted to match the existing render_dag
    interface where the caller passes an over-broad `dag_args` dict.
    """
    description = (
        f"TPC-DI **DIGen.jar** (native, legacy) data-generation workflow "
        f"(SF={scale_factor}). Single-threaded Java utility running on a "
        f"small classic single-node cluster (subprocess + Java can't run "
        f"on serverless). Writes raw input files to "
        f"`/Volumes/{catalog}/tpcdi_raw_data/tpcdi_volume/sf={scale_factor}/`. "
        f"Outputs are single files per table (`Customer.txt`, `Trade.txt`, "
        f"etc.). Set `regenerate_data=YES` to wipe and rebuild; defaults to "
        f"`NO` (no-op if output already exists)."
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
            "new_cluster": {
                "spark_version": default_dbr_version,
                "spark_conf": {
                    "spark.master": "local[*, 4]",
                    "spark.databricks.cluster.profile": "singleNode",
                },
                "node_type_id": default_worker_type,
                "custom_tags": {"ResourceClass": "SingleNode"},
                "data_security_mode": "SINGLE_USER",
                "runtime_engine": "STANDARD",
                "num_workers": 0,
            },
        }],
        "queue": {"enabled": True},
    }
