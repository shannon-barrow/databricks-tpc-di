"""Builder for the DIGen.jar (native, legacy) data-generation workflow.

Replaces `jinja_templates/datagen_workflow_digen.json`. Produces a dict
suitable for POST to `/api/2.1/jobs/create`.

DIGen is a sequential single-threaded Java utility — no Spark involvement
during generation — so the cluster is **always single-node** and forced
**non-serverless DBR 15.4 + Photon**, regardless of the user's overall
serverless choice. Node type scales with scale_factor; storage-optimized
(local NVMe + high disk throughput) once the dataset gets large enough to
matter:

  scale_factor   driver type
  ------------   ----------------------------------------
  ≤ 100          8-core general-purpose (e.g. Standard_D8pds_v6, m8gd.2xlarge)
  1000           16-core storage-optimized (Azure L*sv3, AWS i7i/i4i.4xlarge)
  ≥ 5000         32-core storage-optimized

ARM-based instances are preferred when available within the chosen tier
(Azure D*pds*_v6, AWS Graviton g/gd). Storage-optimized ARM is rare on
Azure today, so SF≥1000 may pick an x86 storage-opt VM.
"""
from __future__ import annotations

try:
    from workflow_builders import _node_picker
except ImportError:
    import _node_picker  # type: ignore


_DIGEN_DBR = "15.4.x-scala2.12"


def _digen_cluster_spec(scale_factor: int, node_types: dict, cloud_provider: str,
                        default_worker_type: str) -> dict:
    """Single-node DBR 15.4 Photon cluster with an SF-tier-appropriate node."""
    if scale_factor <= 100:
        target_cores, prefer_storage = 8, False
    elif scale_factor <= 1000:
        target_cores, prefer_storage = 16, True
    else:  # SF >= 5000
        target_cores, prefer_storage = 32, True

    node = _node_picker.pick_node(
        node_types, cloud_provider,
        target_cores=target_cores,
        prefer_storage_opt=prefer_storage,
        fallback=default_worker_type,
    )

    new_cluster = {
        "spark_version": _DIGEN_DBR,
        "spark_conf": {
            "spark.master": "local[*, 4]",
            "spark.databricks.cluster.profile": "singleNode",
        },
        "node_type_id": node,
        "custom_tags": {"ResourceClass": "SingleNode"},
        "data_security_mode": "SINGLE_USER",
        "runtime_engine": "PHOTON",
        "num_workers": 0,
        # Auto-attach EBS / managed disks if the chosen node has no local NVMe
        # (e.g. AWS m8g without 'd' suffix). DIGen writes its tmp output to
        # /local_disk0; without elastic disk small SF runs may OOM on root.
        "enable_elastic_disk": True,
    }
    # On GCP, if the picked node isn't an `-lssd` variant, explicitly attach
    # local SSDs via gcp_attributes.local_ssd_count (~1 SSD/4 cores).
    _node_picker.apply_gcp_local_ssd_if_needed(
        new_cluster, node, node_types.get(node, {}), cloud_provider, target_cores,
    )
    return new_cluster


def build(*, job_name: str, scale_factor: int, catalog: str,
          regenerate_data: str, repo_src_path: str,
          default_worker_type: str,
          node_types: dict | None = None,
          cloud_provider: str | None = None,
          **_unused) -> dict:
    """Build the DIGen data-generation Jobs API payload.

    `node_types` and `cloud_provider` enable SF-tier-aware node picking
    (preferring ARM + storage-opt + local NVMe). When omitted (older
    callers), falls back to `default_worker_type` for everything.
    """
    tpcdi_directory = f"/Volumes/{catalog}/tpcdi_raw_data/tpcdi_volume/"
    cluster = _digen_cluster_spec(
        scale_factor, node_types or {}, cloud_provider or "", default_worker_type,
    )
    cluster_blurb = f"single-node ({cluster['node_type_id']})"
    description = (
        f"TPC-DI **DIGen.jar** (native, legacy) data-generation workflow "
        f"(SF={scale_factor}). Single-threaded Java utility — runs on a "
        f"non-serverless **DBR {_DIGEN_DBR}** Photon cluster ({cluster_blurb}); "
        f"forced regardless of the overall job's serverless setting since a "
        f"Java subprocess cannot run on serverless. Writes raw input files "
        f"to `{tpcdi_directory}sf={scale_factor}/`. Outputs are single files "
        f"per table (`Customer.txt`, `Trade.txt`, etc.). Set "
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
            {"name": "spark_or_native_datagen", "default": "native"},
            {"name": "scale_factor", "default": str(scale_factor)},
            {"name": "catalog", "default": catalog},
            {"name": "regenerate_data", "default": regenerate_data},
        ],
        "tasks": [{
            "task_key": "generate_data",
            "run_if": "ALL_SUCCESS",
            "notebook_task": {
                "notebook_path": f"{repo_src_path}/tools/data_gen",
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
