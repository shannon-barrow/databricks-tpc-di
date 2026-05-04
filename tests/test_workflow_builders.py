"""Smoke tests for workflow builders — verify naming + tags + schema labels.

Run: python tests/test_workflow_builders.py
"""
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src" / "tools"))
sys.path.insert(0, str(ROOT / "src" / "tools" / "workflow_builders"))

from workflow_builders import (
    datagen_spark, datagen_digen,
    workflows_single_batch, workflows_incremental,
    sdp_pipeline, sdp_workflow,
    augmented_classic, augmented_sdp,
    augmented_staging,
)


# Realistic args mimicking what the Driver passes
COMMON = {
    "scale_factor": 10,
    "catalog": "main",
    "repo_src_path": "/Workspace/Users/test@test.com/repo/src",
    "tpcdi_directory": "/Volumes/main/tpcdi_raw_data/tpcdi_volume/spark_datagen/",
    "wh_target": "test_wh",
    "cloud_provider": "Azure",
    "node_types": {"Standard_D8pds_v6": {"num_cores": 8}},
    "default_dbr_version": "16.4.x-photon-scala2.12",
    "default_worker_type": "Standard_D8pds_v6",
    "worker_node_type": "Standard_D8pds_v6",
    "driver_node_type": "Standard_D8pds_v6",
    "worker_node_count": 0,
    "dbr": "16.4.x-photon-scala2.12",
    "wh_id": "abc123",
}


def _ok(msg): print(f"  ✓ {msg}")
def _fail(msg): raise AssertionError(f"✗ {msg}")


def test_datagen_spark():
    print("\ndatagen_spark.build()")
    out = datagen_spark.build(
        job_name="Smoke-TPCDI-SF10-SparkGen",
        scale_factor=10,
        catalog="main",
        regenerate_data="NO",
        log_level="INFO",
        repo_src_path=COMMON["repo_src_path"],
        default_dbr_version=COMMON["default_dbr_version"],
        default_worker_type=COMMON["default_worker_type"],
        serverless="YES",
        node_types=COMMON["node_types"],
        cloud_provider="Azure",
    )
    assert out["name"] == "Smoke-TPCDI-SF10-SparkGen", out["name"]
    _ok(f"name = {out['name']}")
    assert out["tags"] == {"data_generator": "spark"}, out["tags"]
    _ok(f"tag = {out['tags']}")
    assert "_DataGen" not in out["name"], "old _DataGen suffix leaked"
    _ok("no _DataGen suffix")


def test_datagen_native():
    print("\ndatagen_digen.build()")
    out = datagen_digen.build(
        job_name="Smoke-TPCDI-SF10-NativeGen",
        scale_factor=10,
        catalog="main",
        regenerate_data="NO",
        log_level="INFO",
        repo_src_path=COMMON["repo_src_path"],
        default_dbr_version=COMMON["default_dbr_version"],
        default_worker_type=COMMON["default_worker_type"],
        serverless="NO",
        node_types=COMMON["node_types"],
        cloud_provider="Azure",
    )
    assert out["name"] == "Smoke-TPCDI-SF10-NativeGen", out["name"]
    _ok(f"name = {out['name']}")
    assert out["tags"] == {"data_generator": "native_jar"}, out["tags"]
    _ok(f"tag = {out['tags']}")


def _bench_args(name, exec_type, batched_label, datagen_label, data_generator):
    return {
        "job_name": name,
        "catalog": "main",
        "wh_target": COMMON["wh_target"],
        "tpcdi_directory": COMMON["tpcdi_directory"],
        "scale_factor": 10,
        "repo_src_path": COMMON["repo_src_path"],
        "cloud_provider": "Azure",
        "exec_type": exec_type,
        "serverless": "YES",
        "pred_opt": "DISABLE",
        "exec_folder": "SQL" if exec_type == "DBSQL" else exec_type,
        "null_constraint": "",
        "perf_opt_flg": True,
        "opt_write": "'delta.autoOptimize.optimizeWrite'=False",
        "index_cols": ", 'delta.dataSkippingNumIndexedCols' = 0",
        "data_generator": data_generator,
        "datagen_label": datagen_label,
        "batched_label": batched_label,
        "dbr": COMMON["dbr"],
        "worker_node_type": COMMON["worker_node_type"],
        "driver_node_type": COMMON["driver_node_type"],
        "worker_node_count": 0,
        "wh_id": COMMON["wh_id"],
    }


def test_cluster_incremental_spark():
    print("\nworkflows_incremental.build() — Cluster Spark")
    name = "Smoke-TPCDI-SF10-Incremental-Cluster-SparkGen"
    out = workflows_incremental.build(**_bench_args(
        name, "CLUSTER", "incremental", "spark_data_gen", "spark"))
    assert out["name"] == name, out["name"]
    _ok(f"name = {out['name']}")
    assert out["tags"] == {"data_generator": "spark"}, out["tags"]
    _ok(f"tag = {out['tags']}")
    blob = json.dumps(out)
    assert "_DLT_" not in blob, "_DLT_ schema label leaked"
    _ok("no _DLT_ schema labels")


def test_dbsql_singlebatch_spark():
    print("\nworkflows_single_batch.build() — DBSQL Spark")
    name = "Smoke-TPCDI-SF10-SingleBatch-DBSQL-SparkGen"
    out = workflows_single_batch.build(**_bench_args(
        name, "DBSQL", "single_batch", "spark_data_gen", "spark"))
    assert out["name"] == name, out["name"]
    _ok(f"name = {out['name']}")
    assert out["tags"] == {"data_generator": "spark"}, out["tags"]
    _ok(f"tag = {out['tags']}")


def test_cluster_incremental_native():
    print("\nworkflows_incremental.build() — Cluster NativeJAR")
    name = "Smoke-TPCDI-SF10-Incremental-Cluster-NativeGen"
    out = workflows_incremental.build(**_bench_args(
        name, "CLUSTER", "incremental", "native_data_gen", "digen"))
    assert out["name"] == name, out["name"]
    _ok(f"name = {out['name']}")
    assert out["tags"] == {"data_generator": "native_jar"}, out["tags"]
    _ok(f"tag = {out['tags']}")


def test_sdp_workflow():
    print("\nsdp_workflow.build() — SDP-CORE Spark")
    name = "Smoke-TPCDI-SF10-SDP-CORE-SparkGen"
    out = sdp_workflow.build(
        job_name=name,
        catalog="main",
        wh_target=COMMON["wh_target"],
        edition="CORE",
        datagen_label="spark_data_gen",
        scale_factor=10,
        repo_src_path=COMMON["repo_src_path"],
        tpcdi_directory=COMMON["tpcdi_directory"],
        data_generator="spark",
        pipeline_id=999,
        dbr=COMMON["dbr"],
        driver_node_type=COMMON["driver_node_type"],
        worker_node_type=COMMON["worker_node_type"],
    )
    assert out["name"] == name, out["name"]
    _ok(f"name = {out['name']}")
    assert out["tags"] == {"data_generator": "spark"}, out["tags"]
    _ok(f"tag = {out['tags']}")

    blob = json.dumps(out)
    assert "_SDP_" in blob, "_SDP_ schema label missing"
    _ok("schema label uses _SDP_")
    assert "_DLT_" not in blob, "_DLT_ leaked"
    _ok("no _DLT_ leak")
    assert "TPC-DI-SDP-PIPELINE" in blob, "task_key not renamed"
    _ok("task_key = TPC-DI-SDP-PIPELINE")
    assert "TPC-DI-DLT-PIPELINE" not in blob, "old task_key leaked"
    _ok("no TPC-DI-DLT-PIPELINE leak")


def test_sdp_pipeline():
    print("\nsdp_pipeline.build() — SDP-CORE pipeline definition")
    out = sdp_pipeline.build(
        job_name="Smoke-TPCDI-SF10-SDP-CORE-SparkGen",
        catalog="main",
        wh_target=COMMON["wh_target"],
        edition="CORE",
        datagen_label="spark_data_gen",
        scale_factor=10,
        repo_src_path=COMMON["repo_src_path"],
        tpcdi_directory=COMMON["tpcdi_directory"],
        serverless="YES",
    )
    blob = json.dumps(out)
    assert "_SDP_" in blob, "_SDP_ target schema missing"
    _ok("target schema uses _SDP_")
    assert "spark_declarative_pipelines" in blob, "library path not renamed"
    _ok("library paths use spark_declarative_pipelines/")
    assert "delta_live_tables" not in blob, "old delta_live_tables path leaked"
    _ok("no delta_live_tables/ leak")


def test_sdp_pipeline_spark_high_sf():
    print("\nsdp_pipeline.build() — Spark datagen at SF=10000 still includes CustomerMgmtRaw")
    out = sdp_pipeline.build(
        job_name="Smoke-TPCDI-SF10000-SDP-CORE-SparkGen",
        catalog="main", wh_target=COMMON["wh_target"], edition="CORE",
        datagen_label="spark_data_gen", scale_factor=10000,
        repo_src_path=COMMON["repo_src_path"],
        tpcdi_directory=COMMON["tpcdi_directory"],
        serverless="YES", data_generator="spark",
    )
    lib_paths = [lib["notebook"]["path"] for lib in out["libraries"]]
    assert any("CustomerMgmtRaw" in p for p in lib_paths), \
        f"CustomerMgmtRaw missing from Spark+SF10000: {lib_paths}"
    _ok("CustomerMgmtRaw present at SF=10000 (Spark)")
    assert out["configuration"]["cust_mgmt_schema"] == "LIVE", \
        f"cust_mgmt_schema should be LIVE for Spark, got {out['configuration']['cust_mgmt_schema']}"
    _ok("cust_mgmt_schema = LIVE")


def test_sdp_pipeline_digen_high_sf():
    print("\nsdp_pipeline.build() — DIGen at SF=10000 routes through staging schema")
    out = sdp_pipeline.build(
        job_name="Smoke-TPCDI-SF10000-SDP-CORE-NativeGen",
        catalog="main", wh_target=COMMON["wh_target"], edition="CORE",
        datagen_label="native_data_gen", scale_factor=10000,
        repo_src_path=COMMON["repo_src_path"],
        tpcdi_directory=COMMON["tpcdi_directory"],
        serverless="YES", data_generator="digen",
    )
    lib_paths = [lib["notebook"]["path"] for lib in out["libraries"]]
    assert not any("CustomerMgmtRaw" in p for p in lib_paths), \
        f"CustomerMgmtRaw must NOT be in DIGen+SF10000 pipeline: {lib_paths}"
    _ok("CustomerMgmtRaw absent at SF=10000 (DIGen → upstream maven path)")
    assert "_stage" in out["configuration"]["cust_mgmt_schema"], \
        f"cust_mgmt_schema must point to staging for DIGen+SF10000"
    _ok("cust_mgmt_schema → staging schema")


# -------- AUGMENTED INCREMENTAL --------

_AUG_COMMON = {
    "catalog": "main",
    "scale_factor": 20000,
    "tpcdi_directory": "/Volumes/main/tpcdi_raw_data/tpcdi_volume/",
    "wh_db": "Shannon_Barrow_TPCDI_AugmentedIncremental_Cluster",
    "repo_src_path": COMMON["repo_src_path"],
}


def test_augmented_classic_child():
    print("\naugmented_classic.build_child() — 17-task per-date job")
    out = augmented_classic.build_child(
        job_name="Smoke-TPCDI-SF20000-AugmentedIncremental-Cluster",
        **_AUG_COMMON,
    )
    assert len(out["tasks"]) == 17, f"expected 17 tasks, got {len(out['tasks'])}"
    _ok(f"task count = 17")
    keys = [t["task_key"] for t in out["tasks"]]
    for required in ("simulate_filedrops", "DimCustomer_Incremental",
                      "DimAccount_Incremental", "DimTrade_Incremental",
                      "FactCashBalances_Incremental",
                      "FactMarketHistory_Incremental",
                      "FactHoldings_Incremental", "FactWatches_Incremental",
                      "currentaccountbalances_Incremental",
                      "account_updates_from_customer"):
        assert required in keys, f"missing task_key: {required}"
    _ok("all dim/fact/staging task keys present")
    assert sum(1 for k in keys if k.startswith("bronze")) == 7
    _ok("7 bronze fan-out tasks")
    assert out["tags"] == {"data_generator": "spark"}
    _ok(f"tag = {out['tags']}")
    assert out.get("performance_target") == "PERFORMANCE_OPTIMIZED"
    _ok("performance_target = PERFORMANCE_OPTIMIZED")
    # batch_date is in parameters so for_each can override
    param_names = [p["name"] for p in out["parameters"]]
    assert "batch_date" in param_names
    _ok(f"batch_date in parameters: {param_names}")


def test_augmented_classic_parent():
    print("\naugmented_classic.build_parent() — for_each + cleanup gate")
    out = augmented_classic.build_parent(
        job_name="Smoke-TPCDI-SF20000-AugmentedIncremental-Cluster-Parent",
        child_job_id=99999,
        **_AUG_COMMON,
    )
    keys = [t["task_key"] for t in out["tasks"]]
    assert keys == ["setup", "loop_incremental_tpcdi",
                     "delete_when_finished_TRUE_FALSE", "cleanup"], keys
    _ok(f"task order = {keys}")

    loop = out["tasks"][1]
    assert "for_each_task" in loop, "loop task missing for_each_task"
    assert loop["for_each_task"]["inputs"] == "{{tasks.setup.values.batch_date_ls}}"
    _ok("for_each consumes batch_date_ls from setup task value")
    assert loop["for_each_task"]["task"]["run_job_task"]["job_id"] == 99999
    _ok("for_each calls child job_id")

    gate = out["tasks"][2]
    assert gate["condition_task"]["right"] == "TRUE"
    assert gate["run_if"] == "ALL_DONE"
    _ok("gate: condition EQUAL_TO TRUE, run_if=ALL_DONE")

    cleanup = out["tasks"][3]
    assert cleanup["depends_on"][0] == {
        "task_key": "delete_when_finished_TRUE_FALSE", "outcome": "true"}
    _ok("cleanup depends on gate outcome=true")

    # delete_tables_when_finished defaults FALSE for augmented (long runs)
    finished_param = next(p for p in out["parameters"]
                           if p["name"] == "delete_tables_when_finished")
    assert finished_param["default"] == "FALSE"
    _ok("delete_tables_when_finished default = FALSE (long-run safe)")


def test_augmented_sdp_pipeline():
    print("\naugmented_sdp.build_pipeline() — SDP pipeline def")
    out = augmented_sdp.build_pipeline(
        name="Smoke-Pipeline",
        catalog="main", scale_factor=20000,
        tpcdi_directory="/Volumes/main/tpcdi_raw_data/tpcdi_volume/",
        wh_db="test_aug_sdp",
        repo_src_path=COMMON["repo_src_path"],
    )
    assert out["target"] == "test_aug_sdp_20000"
    _ok(f"target schema = {out['target']}")
    libs = [lib["notebook"]["path"] for lib in out["libraries"]]
    assert any("dlt_ingest_bronze" in p for p in libs)
    assert any("dlt_incremental" in p for p in libs)
    _ok("libraries: dlt_ingest_bronze + dlt_incremental")
    assert out["serverless"] is True and out["photon"] is True
    _ok("serverless + Photon")


def test_augmented_sdp_child():
    print("\naugmented_sdp.build_child() — 2-task per-date job")
    out = augmented_sdp.build_child(
        job_name="Smoke-Child-SDP",
        pipeline_id="abc-123",
        **_AUG_COMMON,
    )
    keys = [t["task_key"] for t in out["tasks"]]
    assert keys == ["simulate_filedrops", "DLT_Pipeline_Incremental_TPCDI"]
    _ok(f"task order = {keys}")
    pipeline_task = out["tasks"][1]["pipeline_task"]
    assert pipeline_task["pipeline_id"] == "abc-123"
    assert pipeline_task["full_refresh"] is False
    _ok("pipeline_task references pipeline_id, full_refresh=False")


def test_augmented_sdp_parent():
    print("\naugmented_sdp.build_parent() — historical-then-incremental orchestration")
    out = augmented_sdp.build_parent(
        job_name="Smoke-Parent-SDP",
        child_job_id=88888,
        pipeline_id="abc-123",
        **_AUG_COMMON,
    )
    keys = [t["task_key"] for t in out["tasks"]]
    assert keys == ["setup", "set_pipeline_historical",
                     "run_historical_pipeline", "set_pipeline_incremental",
                     "loop_incremental_tpcdi",
                     "delete_when_finished_TRUE_FALSE", "cleanup"], keys
    _ok(f"task order = {keys}")

    set_hist = out["tasks"][1]
    assert set_hist["notebook_task"]["base_parameters"] == {
        "pipeline_id": "abc-123", "historical_flag": "true"}
    _ok("set_pipeline_historical passes historical_flag=true")

    run_hist = out["tasks"][2]
    assert run_hist["pipeline_task"]["full_refresh"] is True
    _ok("run_historical_pipeline uses full_refresh=True")

    set_inc = out["tasks"][3]
    assert set_inc["notebook_task"]["base_parameters"]["historical_flag"] == "false"
    _ok("set_pipeline_incremental swaps back to historical_flag=false")


def test_augmented_staging_dag():
    print("\naugmented_staging.build() — Stage 0 data_gen DAG present")
    out = augmented_staging.build(
        job_name="Smoke-TPCDI-SF10-AugmentedGen",
        scale_factor=10,
        catalog="main",
        regenerate_data="NO",
        log_level="INFO",
        repo_src_path=COMMON["repo_src_path"],
        serverless="YES",
    )
    keys = {t["task_key"] for t in out["tasks"]}
    expected_gen_keys = {
        "data_gen",
        "gen_reference", "gen_hr", "gen_finwire", "gen_customer",
        "gen_daily_market",
        "gen_trade_base", "gen_trade", "gen_tradehistory",
        "gen_cashtransaction", "gen_holdinghistory",
        "gen_watch_history",
        "cleanup_intermediates",
    }
    missing = expected_gen_keys - keys
    assert not missing, f"missing tasks in augmented_staging DAG: {missing}"
    _ok(f"all data_gen tasks present ({len(expected_gen_keys)})")
    # data_gen is the unified entry — verify it's wired as the root.
    by_key0 = {t["task_key"]: t for t in out["tasks"]}
    assert "depends_on" not in by_key0["data_gen"] or not by_key0["data_gen"].get("depends_on"), \
        "data_gen should be the root task with no upstream deps"
    _ok("data_gen is the root entry task")
    # cleanup_intermediates depends on every gen.
    by_key = {t["task_key"]: t for t in out["tasks"]}
    cleanup_deps = {d["task_key"] for d in by_key["cleanup_intermediates"]["depends_on"]}
    expected_gens = {"gen_reference", "gen_hr", "gen_finwire", "gen_customer",
                     "gen_daily_market",
                     "gen_trade_base", "gen_trade", "gen_tradehistory",
                     "gen_cashtransaction", "gen_holdinghistory",
                     "gen_watch_history"}
    assert expected_gens.issubset(cleanup_deps), \
        f"cleanup_intermediates missing deps: {expected_gens - cleanup_deps}"
    _ok("cleanup_intermediates depends on all gens")
    assert by_key["cleanup_intermediates"]["run_if"] == "ALL_SUCCESS"
    _ok("cleanup_intermediates runs ALL_SUCCESS")
    # staging_check is gone — Stage 1 tasks wire directly to gen/copy.
    assert "staging_check" not in keys, \
        "staging_check should be removed; Stage 1 tasks wire to gen/copy directly"
    _ok("staging_check removed")
    # Spot-check Stage 1 wiring: stage_files_DailyMarket → gen_daily_market.
    sf_dm_deps = {d["task_key"] for d in by_key["stage_files_DailyMarket"]["depends_on"]}
    assert sf_dm_deps == {"gen_daily_market"}, \
        f"stage_files_DailyMarket should depend on gen_daily_market only, got {sf_dm_deps}"
    _ok("stage_files_DailyMarket → gen_daily_market only")
    # ingest_FinWire → copy_finwire.
    ifw_deps = {d["task_key"] for d in by_key["ingest_FinWire"]["depends_on"]}
    assert ifw_deps == {"copy_finwire"}, \
        f"ingest_FinWire should depend on copy_finwire only, got {ifw_deps}"
    _ok("ingest_FinWire → copy_finwire only")


def main():
    tests = [
        test_datagen_spark,
        test_datagen_native,
        test_cluster_incremental_spark,
        test_dbsql_singlebatch_spark,
        test_cluster_incremental_native,
        test_sdp_workflow,
        test_sdp_pipeline,
        test_sdp_pipeline_spark_high_sf,
        test_sdp_pipeline_digen_high_sf,
        test_augmented_classic_child,
        test_augmented_classic_parent,
        test_augmented_sdp_pipeline,
        test_augmented_sdp_child,
        test_augmented_sdp_parent,
        test_augmented_staging_dag,
    ]
    for t in tests:
        t()
    print(f"\nAll {len(tests)} tests passed.")


if __name__ == "__main__":
    main()
