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
    ]
    for t in tests:
        t()
    print(f"\nAll {len(tests)} tests passed.")


if __name__ == "__main__":
    main()
