"""Builder for the Spark distributed data-generation workflow.

Multi-task DAG (mirrors the augmented_staging Stage-0 layout but writes
files instead of Delta):

  data_gen (entry: schema+volume init, wipe on regenerate=YES)
    ├── gen_reference / gen_hr / gen_finwire / gen_prospect   (wave 1)
    │   ├── copy_hr / copy_finwire (parallel with downstream gens)
    │   └── gen_customer (← gen_hr)
    │       └── copy_customer
    │   └── gen_daily_market / gen_trade (← gen_finwire)
    │       └── copy_daily_market / copy_trade
    │   └── copy_prospect (← gen_prospect)
    └── gen_watch_history (← gen_finwire + gen_customer)
        └── copy_watch_history
  audit_emit (ALL_SUCCESS — aggregates record_counts task values from all gens)
  cleanup_intermediates (ALL_SUCCESS — drops _gen_* + _dc_* in {wh_db}_{sf}_stage)

Each gen_* task self-skips when its output is intact and
``regenerate_data=NO``, so a re-trigger only redoes the missing
datasets. copy_* tasks are decoupled from their gen so they run in
parallel with downstream generation (avoiding the single-task daemon-
thread race when large parts cross the 50 MB threshold).

By default runs on serverless. If the user picks non-serverless on the
Driver, builds a classic Photon cluster sized by scale_factor and pins
every task to it (single shared cluster for the whole DAG):

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


_DEFAULT_NOTIF = {
    "no_alert_for_skipped_runs": False,
    "no_alert_for_canceled_runs": False,
    "alert_on_last_attempt": False,
}


def _make_task(*, task_key: str, notebook_path: str,
               depends_on: list | None = None,
               base_params: dict | None = None,
               run_if: str = "ALL_SUCCESS",
               job_cluster_key: str | None = None) -> dict:
    notebook_task = {"notebook_path": notebook_path, "source": "WORKSPACE"}
    if base_params is not None:
        notebook_task["base_parameters"] = base_params
    task = {"task_key": task_key}
    if depends_on:
        task["depends_on"] = [
            d if isinstance(d, dict) else {"task_key": d} for d in depends_on
        ]
    task["run_if"] = run_if
    task["notebook_task"] = notebook_task
    task["timeout_seconds"] = 0
    task["email_notifications"] = {}
    task["notification_settings"] = dict(_DEFAULT_NOTIF)
    if job_cluster_key is not None:
        task["job_cluster_key"] = job_cluster_key
    return task


def build(*, job_name: str, scale_factor: int, catalog: str,
          regenerate_data: str, log_level: str, repo_src_path: str,
          serverless: str = "YES",
          node_types: dict | None = None,
          cloud_provider: str | None = None,
          default_worker_type: str | None = None,
          default_dbr_version: str | None = None,
          datagen_choice: str = "spark",
          **_unused) -> dict:
    """``datagen_choice`` flips the augmented_incremental widget on each
    task — ``"spark"`` writes raw files (standard mode);
    ``"augmented_incremental"`` writes Delta tables to
    ``tpcdi_raw_data.{dataset}{sf}`` and skips Batch2/Batch3.
    """
    # Spark output goes under spark_datagen/ subdir to keep DIGen and Spark
    # outputs separate (DIGen writes to the unprefixed sf=N/ path).
    tpcdi_directory = f"/Volumes/{catalog}/tpcdi_raw_data/tpcdi_volume/spark_datagen/"
    is_serverless = (serverless or "YES").upper() == "YES"
    is_augmented = datagen_choice == "augmented_incremental"

    if is_serverless:
        cluster_blurb = "serverless"
        job_cluster_key = None
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

    description = (
        f"TPC-DI **Spark** data-generation workflow (SF={scale_factor}, "
        f"{cluster_blurb}). Distributed PySpark generator decomposed into "
        f"per-dataset tasks: each gen_* writes its output, copy_* tasks run "
        f"in parallel with downstream gens, and audit_emit aggregates "
        f"record_counts task values into `*_audit.csv` files. "
        f"`regenerate_data=YES` wipes and rebuilds; `NO` skips per-task "
        f"when output is intact."
    )

    # Common per-task base_params. tpcdi_directory + augmented_incremental
    # are baked once; wh_db lands on the temp `_stage` schema by default
    # (cleanup_intermediates drops `_gen_*` and `_dc_*` from there).
    _base = {
        "tpcdi_directory": tpcdi_directory,
        "augmented_incremental": "true" if is_augmented else "false",
        "wh_db": "tpcdi_incremental_staging",
    }
    _wh_only = {"wh_db": _base["wh_db"]}
    _dgt = f"{repo_src_path}/tools/data_gen_tasks"

    tasks: list[dict] = []
    tasks.append(_make_task(
        task_key="data_gen",
        notebook_path=f"{_dgt}/data_gen",
        base_params=_base,
        job_cluster_key=job_cluster_key,
    ))
    # Wave 1 — no upstream gen.
    for _name in ("gen_reference", "gen_hr", "gen_finwire", "gen_prospect"):
        tasks.append(_make_task(
            task_key=_name,
            notebook_path=f"{_dgt}/{_name}",
            depends_on=["data_gen"],
            base_params=_base,
            job_cluster_key=job_cluster_key,
        ))
    # Wave 2 — depend on a wave-1 producer.
    tasks.append(_make_task(
        task_key="gen_customer", notebook_path=f"{_dgt}/gen_customer",
        depends_on=["gen_hr"], base_params=_base, job_cluster_key=job_cluster_key,
    ))
    tasks.append(_make_task(
        task_key="gen_daily_market", notebook_path=f"{_dgt}/gen_daily_market",
        depends_on=["gen_finwire"], base_params=_base, job_cluster_key=job_cluster_key,
    ))
    # gen_trade is decomposed into base + 4 leaves (V6). The base task
    # materializes _gen_trade_df Delta with the minimum cross-task col
    # set; each leaf reads it in parallel and writes its dataset's
    # outputs. Leaf-level repair-run granularity + the 4 long writes run
    # truly concurrently instead of sharing one task's executors.
    tasks.append(_make_task(
        task_key="gen_trade_base", notebook_path=f"{_dgt}/gen_trade_base",
        depends_on=["gen_finwire"], base_params=_base, job_cluster_key=job_cluster_key,
    ))
    for _leaf in ("gen_trade", "gen_tradehistory",
                  "gen_cashtransaction", "gen_holdinghistory"):
        tasks.append(_make_task(
            task_key=_leaf, notebook_path=f"{_dgt}/{_leaf}",
            depends_on=["gen_trade_base"], base_params=_base,
            job_cluster_key=job_cluster_key,
        ))
    # Wave 3.
    tasks.append(_make_task(
        task_key="gen_watch_history", notebook_path=f"{_dgt}/gen_watch_history",
        depends_on=["gen_finwire", "gen_customer"], base_params=_base,
        job_cluster_key=job_cluster_key,
    ))

    # Copy tasks — run in parallel with downstream gens. Trade family is
    # split into 4 sibling tasks (one per dataset) so each gets repair-run
    # granularity and the long-pole copies run concurrently across separate
    # processes (independent UC Volumes Files API throttle slots).
    _copy_specs = [
        ("copy_hr",              ["gen_hr"]),
        ("copy_finwire",         ["gen_finwire"]),
        ("copy_customer",        ["gen_customer"]),
        ("copy_daily_market",    ["gen_daily_market"]),
        ("copy_trade",           ["gen_trade"]),
        ("copy_tradehistory",    ["gen_tradehistory"]),
        ("copy_cashtransaction", ["gen_cashtransaction"]),
        ("copy_holdinghistory",  ["gen_holdinghistory"]),
        ("copy_watch_history",   ["gen_watch_history"]),
        ("copy_prospect",        ["gen_prospect"]),
    ]
    _copy_keys: list[str] = []
    for _name, _deps in _copy_specs:
        tasks.append(_make_task(
            task_key=_name, notebook_path=f"{_dgt}/{_name}",
            depends_on=_deps, base_params=_base, job_cluster_key=job_cluster_key,
        ))
        _copy_keys.append(_name)

    _gen_keys = ["gen_reference", "gen_hr", "gen_finwire", "gen_prospect",
                 "gen_customer", "gen_daily_market",
                 "gen_trade_base", "gen_trade", "gen_tradehistory",
                 "gen_cashtransaction", "gen_holdinghistory",
                 "gen_watch_history"]

    # audit_emit: aggregates record_counts task values from all gens.
    tasks.append(_make_task(
        task_key="audit_emit", notebook_path=f"{_dgt}/audit_emit",
        depends_on=_gen_keys, run_if="ALL_SUCCESS",
        base_params=_base, job_cluster_key=job_cluster_key,
    ))

    # cleanup_intermediates: depends on every gen + copy + audit_emit so
    # disk_cache temps are still readable up to that point.
    tasks.append(_make_task(
        task_key="cleanup_intermediates",
        notebook_path=f"{_dgt}/cleanup_intermediates",
        depends_on=_gen_keys + _copy_keys + ["audit_emit"],
        run_if="ALL_SUCCESS",
        base_params=_wh_only, job_cluster_key=job_cluster_key,
    ))

    payload = {
        "name": job_name,
        "description": description,
        "tags": {"data_generator": "spark"},
        "email_notifications": {"no_alert_for_skipped_runs": False},
        "webhook_notifications": {},
        "timeout_seconds": 0,
        "max_concurrent_runs": 1,
        "parameters": [
            {"name": "data_gen_type", "default": datagen_choice},
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
        payload["job_clusters"] = [{
            "job_cluster_key": job_cluster_key,
            "new_cluster": cluster_spec,
        }]

    return payload
