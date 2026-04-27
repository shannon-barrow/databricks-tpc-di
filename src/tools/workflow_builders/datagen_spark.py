"""Builder for the Spark distributed data-generation workflow.

Replaces `jinja_templates/datagen_workflow.json`.
"""
from __future__ import annotations


def build(*, job_name: str, scale_factor: int, catalog: str,
          regenerate_data: str, log_level: str, repo_src_path: str,
          **_unused) -> dict:
    description = (
        f"TPC-DI **Spark** data-generation workflow (SF={scale_factor}). "
        f"Distributed PySpark generator running as a single serverless notebook "
        f"task. Writes raw input files to "
        f"`/Volumes/{catalog}/tpcdi_raw_data/tpcdi_volume/spark_datagen/"
        f"sf={scale_factor}/`. Outputs are split files like `Customer_1.txt`, "
        f"`Customer_2.txt`, ... (matched by the benchmark via "
        f"`{{Customer.txt,Customer_[0-9]*.txt}}`-style globs). Set "
        f"`regenerate_data=YES` to wipe and rebuild; defaults to `NO` (no-op "
        f"if output already exists)."
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
            {"name": "log_level", "default": log_level},
        ],
        "tasks": [{
            "task_key": "generate_data",
            "run_if": "ALL_SUCCESS",
            "notebook_task": {
                "notebook_path": f"{repo_src_path}/tools/spark_data_generator",
                "source": "WORKSPACE",
            },
            "timeout_seconds": 0,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": False,
                "no_alert_for_canceled_runs": False,
                "alert_on_last_attempt": False,
            },
        }],
        "queue": {"enabled": True},
        "performance_target": "PERFORMANCE_OPTIMIZED",
    }
