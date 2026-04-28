"""Builder for the Spark distributed data-generation workflow.

By default runs as a single serverless notebook task. If the user picks
non-serverless on the Driver, builds a classic Photon cluster sized by
scale_factor:

  scale_factor   shape
  ------------   --------------------------------------------------
  ≤ 100          single-node 8-core
  1000           single-node 16-core
  5000           32-core driver + 5 × 16-core workers
  10000          64-core driver + 10 × 16-core workers
  20000          64-core driver + 20 × 16-core workers
  (general rule above SF=1000: 1 × 16-core worker per 1000 of SF)

ARM-based instances are preferred when available (Azure D*pds*_v6, AWS
Graviton g/gd). Local NVMe disk is preferred; if unavailable on the
target ARM family `enable_elastic_disk: True` lets Databricks attach
managed disks to keep shuffle off root.
"""
from __future__ import annotations

try:
    from workflow_builders import _node_picker
except ImportError:
    import _node_picker  # type: ignore


def _spark_cluster_spec(scale_factor: int, node_types: dict, cloud_provider: str,
                        default_worker_type: str, default_dbr_version: str,
                        cloud_attrs_for: callable) -> dict:
    """Classic Photon cluster sized by scale_factor for non-serverless Spark
    datagen. Single-node up through SF=1000; +1 worker per 1000 of SF above
    that. All nodes are general-purpose (Spark distributes across workers,
    so per-node disk doesn't dominate the way it does for sequential DIGen).
    """
    if scale_factor <= 100:
        driver_cores, worker_cores, num_workers = 8, 8, 0
    elif scale_factor <= 1000:
        driver_cores, worker_cores, num_workers = 16, 16, 0
    elif scale_factor < 10000:  # SF == 5000
        driver_cores, worker_cores, num_workers = 32, 16, scale_factor // 1000
    else:  # SF >= 10000
        driver_cores, worker_cores, num_workers = 64, 16, scale_factor // 1000

    driver = _node_picker.pick_node(
        node_types, cloud_provider,
        target_cores=driver_cores,
        prefer_storage_opt=False,
        fallback=default_worker_type,
    )
    worker = _node_picker.pick_node(
        node_types, cloud_provider,
        target_cores=worker_cores,
        prefer_storage_opt=False,
        fallback=default_worker_type,
    )

    is_single_node = num_workers == 0
    spark_conf: dict = {"spark.sql.shuffle.partitions": "auto"}
    if is_single_node:
        spark_conf["spark.master"] = "local[*, 4]"
        spark_conf["spark.databricks.cluster.profile"] = "singleNode"

    new_cluster: dict = {
        "spark_version": default_dbr_version,
        "spark_conf": spark_conf,
    }
    cloud_attrs = cloud_attrs_for(cloud_provider)
    if cloud_attrs:
        new_cluster.update(cloud_attrs)
    new_cluster["spark_env_vars"] = {"PYSPARK_PYTHON": "/databricks/python3/bin/python3"}
    if is_single_node:
        new_cluster["custom_tags"] = {"ResourceClass": "SingleNode"}
    new_cluster["driver_node_type_id"] = driver
    new_cluster["node_type_id"] = worker
    new_cluster["num_workers"] = num_workers
    new_cluster["enable_elastic_disk"] = True  # safety for nodes without local NVMe
    new_cluster["data_security_mode"] = "SINGLE_USER"
    new_cluster["runtime_engine"] = "PHOTON"
    # On GCP, attach local SSDs to non-lssd nodes. We size by the WORKER cores (not driver), since workers do the heavy shuffle.
    _node_picker.apply_gcp_local_ssd_if_needed(
        new_cluster, worker, node_types.get(worker, {}), cloud_provider, worker_cores,
    )
    return new_cluster


def _cloud_attrs(cloud_provider: str) -> dict:
    """Per-cloud spot-with-fallback attributes (best TCO for transient gen jobs)."""
    if cloud_provider == "Azure":
        return {"azure_attributes": {
            "availability": "SPOT_WITH_FALLBACK_AZURE",
            "first_on_demand": 1,
            "spot_bid_price_percent": -1,
        }}
    if cloud_provider == "AWS":
        return {"aws_attributes": {
            "availability": "SPOT_WITH_FALLBACK",
            "first_on_demand": 1,
            "zone_id": "auto",
            "spot_bid_price_percent": 100,
        }}
    if cloud_provider == "GCP":
        # local_ssd_count is added later by apply_gcp_local_ssd_if_needed if the picked node has no built-in local SSD.
        return {"gcp_attributes": {
            "availability": "PREEMPTIBLE_WITH_FALLBACK_GCP",
        }}
    return {}


def build(*, job_name: str, scale_factor: int, catalog: str,
          regenerate_data: str, log_level: str, repo_src_path: str,
          serverless: str = "YES",
          node_types: dict | None = None,
          cloud_provider: str | None = None,
          default_worker_type: str | None = None,
          default_dbr_version: str | None = None,
          **_unused) -> dict:
    tpcdi_directory = f"/Volumes/{catalog}/tpcdi_raw_data/tpcdi_volume/"
    is_serverless = (serverless or "YES").upper() == "YES"

    if is_serverless:
        cluster_blurb = "serverless"
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

    description = (
        f"TPC-DI **Spark** data-generation workflow (SF={scale_factor}, "
        f"{cluster_blurb}). Distributed PySpark generator. Writes raw input "
        f"files to `{tpcdi_directory}spark_datagen/sf={scale_factor}/`. "
        f"Outputs are split files like `Customer_1.txt`, `Customer_2.txt`, "
        f"... (matched by the benchmark via "
        f"`{{Customer.txt,Customer_[0-9]*.txt}}`-style globs). Set "
        f"`regenerate_data=YES` to wipe and rebuild; defaults to `NO` "
        f"(no-op if output already exists)."
    )

    payload = {
        "name": job_name,
        "description": description,
        "tags": {"data_generator": "spark"},
        "email_notifications": {"no_alert_for_skipped_runs": False},
        "webhook_notifications": {},
        "timeout_seconds": 0,
        "max_concurrent_runs": 1,
        "parameters": [
            {"name": "spark_or_native_datagen", "default": "spark"},
            {"name": "scale_factor", "default": str(scale_factor)},
            {"name": "catalog", "default": catalog},
            {"name": "regenerate_data", "default": regenerate_data},
            {"name": "log_level", "default": log_level},
        ],
        "tasks": [{
            "task_key": "generate_data",
            "run_if": "ALL_SUCCESS",
            "notebook_task": {
                "notebook_path": f"{repo_src_path}/tools/data_gen",
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
    }

    if is_serverless:
        payload["performance_target"] = "PERFORMANCE_OPTIMIZED"
    else:
        # Pin the task to the classic cluster.
        payload["tasks"][0]["job_cluster_key"] = f"{job_name}_compute"
        payload["job_clusters"] = [{
            "job_cluster_key": f"{job_name}_compute",
            "new_cluster": cluster_spec,
        }]

    return payload
