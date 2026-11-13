"""Local tests for the EMR variant (no AWS, no Spark, no Java).

Run: python -m pytest tests/test_emr_variant.py
  or python tests/test_emr_variant.py
"""
import ast
import difflib
import json
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parent.parent
AUG = ROOT / "src" / "incremental_batches" / "augmented_incremental"
EMR = AUG / "spark_competitors" / "emr"
FAB = AUG / "spark_competitors" / "fabric" / "notebooks"
for p in (EMR, EMR / "jobs", ROOT / "src" / "tools"):
    sys.path.insert(0, str(p))

import _emr_conn as c  # noqa: E402
import _emr_staging as stg  # noqa: E402
import emr_bench  # noqa: E402
import emr_runner  # noqa: E402
import tpcdi_dag  # noqa: E402
import tpcdi_sql as q  # noqa: E402
from botocore.stub import Stubber  # noqa: E402
import boto3  # noqa: E402

D = "2016-07-06"
TRANSFORMS = ["dimcustomer", "dimaccount", "dimtrade", "factwatches",
              "currentaccountbalances", "factholdings"]


# --- SQL ---------------------------------------------------------------------

def _norm(sql):
    sql = re.sub(r"--[^\n]*", " ", sql)
    return [t.lower() for t in re.findall(
        r"[A-Za-z_][A-Za-z0-9_.]*|'[^']*'|\"[^\"]*\"|\d+|\S", sql)]


def _fabric_sql(nb, table):
    src = (FAB / nb).read_text()
    block = re.findall(r'f"""(.*?)"""', src, re.S)[0]
    other = "bronzedailymarket" if table == "factmarkethistory" else "currentaccountbalances"
    return (block.replace("{tgt_table}", f"db.{table}").replace("{src_table}", f"db.{other}")
            .replace("{tgt_db}", "db").replace("{batch_date}", D))


def _ours(table):
    if table == "factmarkethistory":
        return q.factmarkethistory_src("db", D)
    if table == "factcashbalances":
        return q.factcashbalances_src("db")
    if table == "bronzeaccount":
        return q.account_updates_from_customer("db", D)
    return getattr(q, table)("db", D)


FABRIC_NOTEBOOKS = {
    "dimcustomer": "incremental/DimCustomer Incremental.py",
    "dimaccount": "incremental/DimAccount Incremental.py",
    "dimtrade": "incremental/DimTrade Incremental.py",
    "factwatches": "incremental/FactWatches Incremental.py",
    "currentaccountbalances": "incremental/currentaccountbalances Incremental.py",
    "factholdings": "incremental/FactHoldings Incremental.py",
    "factmarkethistory": "incremental/FactMarketHistory Incremental.py",
    "factcashbalances": "incremental/FactCashBalances Incremental.py",
    "bronzeaccount": "account_updates_from_customer.py",
}


def test_sql_matches_fabric_port_token_for_token():
    """The EMR SQL is the Fabric SQL; the only allowed change is DimAccount's
    star-except, which OSS Spark 3.5 lacks."""
    for table, nb in FABRIC_NOTEBOOKS.items():
        a, b = _norm(_fabric_sql(nb, table)), _norm(_ours(table))
        diff = [x for x in difflib.unified_diff(a, b, lineterm="", n=0)
                if not x.startswith(("---", "+++", "@@"))]
        if table == "dimaccount":
            removed = [x[1:] for x in diff if x.startswith("-")]
            assert removed == ["*", "except", "(", "cdc_dsn", ",", "_rn", ")"], diff
        else:
            assert not diff, f"{table} drifted from the Fabric port: {diff[:20]}"


def test_bronze_schemas_match_fabric():
    tree = ast.parse((FAB / "ingest_bronze.py").read_text())
    fab = next(ast.literal_eval(n.value) for n in tree.body
               if isinstance(n, ast.Assign) and getattr(n.targets[0], "id", "") == "schemas")
    assert fab == q.BRONZE_SCHEMAS
    tree = ast.parse((FAB / "setup.py").read_text())
    fab_staging = [x for n in tree.body if isinstance(n, ast.Assign)
                   and getattr(n.targets[0], "id", "") in ("shallow_tbls", "deep_tbls")
                   for x in ast.literal_eval(n.value)]
    assert fab_staging == q.STAGING_TABLES


def test_sql_avoids_non_oss_spark_syntax():
    import sqlglot
    sqls = [_ours(t) for t in FABRIC_NOTEBOOKS] + [
        q.create_bronze("db", s) for s in q.EMPTY_BRONZE] + [
        q.shallow_clone("db", "dimdate", "s3://b/p/_emr/staging_delta/sf=10/", "s3://b/w/db.db")]
    for sql in sqls:
        low = sql.lower()
        for banned in ("except(", "qualify", "replace using", "cluster by", "notebookutils",
                       "{", "}"):
            assert banned not in low, (banned, sql[:200])
        if "SHALLOW CLONE" not in sql:  # sqlglot's spark dialect has no CLONE
            sqlglot.parse_one(sql, read="spark")  # raises on a syntax error
    assert sqls[-1] == ("CREATE TABLE db.dimdate SHALLOW CLONE "
                        "delta.`s3://b/p/_emr/staging_delta/sf=10/dimdate` "
                        "LOCATION 's3://b/w/db.db/dimdate'")
    assert "dataSkippingNumIndexedCols' = '34'" in q.create_bronze("db", "customer")


def test_delete_dateids():
    assert q.delete_dateids("db", "t", []) is None
    assert q.delete_dateids("db", "t", [None]) is None
    assert q.delete_dateids("db", "t", [20160707, 20160706, 20160706]) == \
        "DELETE FROM db.t WHERE sk_dateid IN (20160706,20160707)"


# --- DAG ---------------------------------------------------------------------

def test_dag_matches_fabric_batch_runner_edges():
    order = tpcdi_dag.topo_order(tpcdi_dag.DAG)
    assert len(order) == 16 and set(order) == set(tpcdi_dag.DAG)
    pos = {n: i for i, n in enumerate(order)}
    for n, deps in tpcdi_dag.DAG.items():
        assert all(pos[d] < pos[n] for d in deps)
    assert "bronzeaccount" in tpcdi_dag.DAG["account_updates_from_customer"]
    # every non-bronze node is a SQL builder or a special-cased selective overwrite
    for n in tpcdi_dag.DAG:
        assert n.startswith("bronze") or hasattr(q, n) or hasattr(q, n + "_src"), n


def test_run_dag_order_failure_and_concurrency():
    import threading
    seen, lock, live, peak = [], threading.Lock(), [0], [0]

    def run(n):
        with lock:
            live[0] += 1
            peak[0] = max(peak[0], live[0])
            seen.append(n)
        try:
            if n == "dimtrade":
                raise ValueError("boom")
        finally:
            with lock:
                live[0] -= 1

    recs = {r["node"]: r for r in tpcdi_dag.run_dag(tpcdi_dag.DAG, run, concurrency=3)}
    assert peak[0] <= 3
    assert recs["dimtrade"]["status"] == "FAILED" and "boom" in recs["dimtrade"]["error"]
    assert recs["factholdings"]["status"] == "SKIPPED" and "factholdings" not in seen
    assert recs["factwatches"]["status"] == "SUCCESS"
    for n, deps in tpcdi_dag.DAG.items():
        if n in seen:
            assert all(seen.index(d) < seen.index(n) for d in deps)


def test_run_dag_rejects_cycles():
    try:
        tpcdi_dag.run_dag({"a": ["b"], "b": ["a"]}, lambda n: None)
    except ValueError as e:
        assert "cycle" in str(e)
    else:
        raise AssertionError("cycle not detected")


# --- runner with a fake SparkSession ----------------------------------------

class FakeSpark:
    def __init__(self, dateids=(20160706,)):
        self.sqls, self.read, self.sparkContext = [], mock.MagicMock(), mock.MagicMock()
        self._dateids = dateids

    def sql(self, s):
        self.sqls.append(s)
        df = mock.MagicMock()
        df.select.return_value.distinct.return_value.collect.return_value = [(d,) for d in self._dateids]
        return df


def test_runner_batch_runs_every_node():
    spark = FakeSpark()
    res = emr_runner.run_batch(spark, db="db", batch_date=D, batch_dir=f"s3://b/x/{D}",
                               concurrency=4, metrics_path=None)
    assert res["status"] == "SUCCESS" and len(res["tasks"]) == 16
    joined = "\n".join(spark.sqls)
    assert joined.count("INSERT INTO db.bronze") == 7 + 1  # 7 ingests + customer-driven accounts
    assert "DELETE FROM db.factmarkethistory WHERE sk_dateid IN (20160706)" in joined
    assert "DELETE FROM db.factcashbalances WHERE sk_dateid IN (20160706)" in joined
    assert joined.count("MERGE INTO") == 4
    loads = [cl.args[0] for cl in spark.read.format.return_value.schema.return_value.option
             .return_value.option.return_value.option.return_value.load.call_args_list]
    assert loads == [f"s3://b/x/{D}"] * 7


def test_runner_setup_shallow_clones_staging():
    spark = FakeSpark()
    with mock.patch.object(emr_runner, "_delete_path") as rm:
        emr_runner.run_setup(spark, db="db", db_location="s3://b/w/db.db",
                             staging_delta="s3://b/s/sf=10/")
    rm.assert_called_once_with(spark, "s3://b/w/db.db")
    assert spark.sqls[:2] == ["DROP DATABASE IF EXISTS db CASCADE",
                              "CREATE DATABASE db LOCATION 's3://b/w/db.db'"]
    clones = [x for x in spark.sqls if "SHALLOW CLONE" in x]
    assert sorted(clones) == sorted(
        q.shallow_clone("db", t, "s3://b/s/sf=10", "s3://b/w/db.db") for t in q.STAGING_TABLES)
    assert [x for x in spark.sqls if x.startswith("CREATE TABLE db.bronze") and "CLONE" not in x] \
        == [q.create_bronze("db", s) for s in q.EMPTY_BRONZE]
    assert not any("parquet" in x for x in spark.sqls)


def test_runner_cli_parses_each_mode():
    with mock.patch.object(emr_runner, "_spark", return_value=FakeSpark()), \
         mock.patch.object(emr_runner, "_delete_path"):
        assert emr_runner.main(["setup", "--db", "db", "--db-location", "s3://b/w/db.db",
                                "--staging-delta", "s3://b/s/sf=10"]) == 0
        assert emr_runner.main(["drop", "--db", "db", "--db-location", "s3://b/w/db.db"]) == 0
        assert emr_runner.main(["batch", "--db", "db", "--batch-date", D,
                                "--batch-dir", "s3://b/d"]) == 0


# --- control plane -----------------------------------------------------------

ROOT_URI = "s3://bucket/prefix/"
P = c.paths(ROOT_URI, "Test_Emr", 10, "r1")


def _spec(**kw):
    return c.cluster_spec(name="t", subnet_id="subnet-1", master_sg="sg-m", slave_sg="sg-s",
                          log_uri=P["logs"], core_count=kw.pop("core_count", 2), **kw)


def test_paths_and_dates():
    assert P["db"] == "test_emr_10"
    assert P["dailybatches"] == "s3://bucket/prefix/augmented_incremental/_dailybatches/test_emr_10"
    assert P["staging_delta"] == "s3://bucket/prefix/_emr/staging_delta/sf=10"
    assert P["metrics"] == "s3://bucket/prefix/_emr/metrics/test_emr_10/r1"
    assert c.batch_dates(2) == ["2016-07-06", "2016-07-07"]
    assert len(c.batch_dates(999)) == 365 and c.batch_dates(999)[-1] == "2017-07-05"
    assert c.batch_dates(5, start_index=363) == ["2017-07-04", "2017-07-05"]
    assert c.to_volume(P["code"], ROOT_URI, "/Volumes/main/tpcdi_raw_data/tpcdi_volume/") == \
        "/Volumes/main/tpcdi_raw_data/tpcdi_volume/_emr/code/"


def test_cluster_spec_is_valid_run_job_flow_input():
    spec = _spec(core_count=16, tags={"Owner": "x"})
    c.validate_cluster_spec(spec)
    assert c.instance_counts(spec) == {"m8gd.2xlarge": 17}
    props = {x["Classification"]: x.get("Properties") for x in spec["Configurations"]}
    assert props["spark-defaults"]["spark.executor.instances"] == "16"
    assert props["spark-defaults"]["spark.dynamicAllocation.enabled"] == "false"
    assert "AWSGlueDataCatalogHiveClientFactory" in props["spark-hive-site"][
        "hive.metastore.client.factory.class"]
    assert {"Key": "Owner", "Value": "x"} in spec["Tags"]
    bad = dict(spec, Bogus=1)
    try:
        c.validate_cluster_spec(bad)
    except ValueError:
        pass
    else:
        raise AssertionError("invalid spec accepted")


def test_steps():
    st = c.batch_step(P, D, concurrency=6)
    args = st["HadoopJarStep"]["Args"]
    assert st["HadoopJarStep"]["Jar"] == "command-runner.jar"
    assert args[:3] == ["spark-submit", "--deploy-mode", "client"]
    assert args[args.index("--batch-dir") + 1] == f"{P['dailybatches']}/{D}"
    assert args[args.index("--metrics-path") + 1] == f"{P['metrics']}/batch={D}.json"
    assert args[args.index("--concurrency") + 1] == "6"
    setup = c.setup_step(P)["HadoopJarStep"]["Args"]
    assert setup[setup.index("--staging-delta") + 1] == P["staging_delta"]
    assert c.sync_code_step(P["code"])["HadoopJarStep"]["Args"][0] == "bash"
    # emr_runner's argparse accepts exactly what the step builders emit
    for step in (c.batch_step(P, D), c.setup_step(P), c.drop_step(P)):
        with mock.patch.object(emr_runner, "_spark", return_value=FakeSpark()), \
             mock.patch.object(emr_runner, "_delete_path"), \
             mock.patch.object(emr_runner, "_write_text"):
            assert emr_runner.main(step["HadoopJarStep"]["Args"][4:]) == 0


def _emr_client():
    return boto3.client("emr", region_name="us-west-2", aws_access_key_id="x",
                        aws_secret_access_key="x")


def test_run_step_polls_until_complete():
    emr = _emr_client()
    t0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
    with Stubber(emr) as st:
        st.add_response("add_job_flow_steps", {"StepIds": ["s-1"]})
        st.add_response("describe_step", {"Step": {"Id": "s-1", "Status": {"State": "RUNNING"}}})
        st.add_response("describe_step", {"Step": {"Id": "s-1", "Status": {
            "State": "COMPLETED", "Timeline": {"StartDateTime": t0,
                                               "EndDateTime": t0 + timedelta(seconds=95)}}}})
        step = c.run_step(emr, "j-1", c.batch_step(P, D), sleep=lambda s: None)
    assert c.step_seconds(step) == 95


def test_run_step_raises_with_step_record():
    emr = _emr_client()
    with Stubber(emr) as st:
        st.add_response("add_job_flow_steps", {"StepIds": ["s-2"]})
        st.add_response("describe_step", {"Step": {"Id": "s-2", "Status": {
            "State": "FAILED", "FailureDetails": {"Message": "exit 1", "LogFile": "s3://l"}}}})
        try:
            c.run_step(emr, "j-1", c.setup_step(P), sleep=lambda s: None)
        except c.StepFailed as e:
            assert e.step["Id"] == "s-2" and "exit 1" in str(e)
        else:
            raise AssertionError("no StepFailed")


def test_wait_cluster_ready_raises_on_termination():
    emr = _emr_client()
    with Stubber(emr) as st:
        st.add_response("describe_cluster", {"Cluster": {"Id": "j-1", "Status": {"State": "STARTING"}}})
        st.add_response("describe_cluster", {"Cluster": {"Id": "j-1", "Status": {
            "State": "TERMINATED_WITH_ERRORS", "StateChangeReason": {"Message": "capacity"}}}})
        try:
            c.wait_cluster_ready(emr, "j-1", sleep=lambda s: None)
        except RuntimeError as e:
            assert "capacity" in str(e)
        else:
            raise AssertionError("termination not surfaced")


PRICES = json.loads((EMR / "prices_snapshot.json").read_text())


class StagingSpark:
    """Databricks for _emr_staging: rejects SQL on paths starting with
    `reject`, and reports `features[table]` from DESCRIBE DETAIL."""

    def __init__(self, reject=(), features=None):
        self.sqls, self.reject, self.features = [], tuple(reject), dict(features or {})

    def sql(self, s):
        ref = re.search(r"delta\.`([^`]*)`", s)
        if ref and self.reject and ref.group(1).startswith(self.reject):
            raise RuntimeError("path not allowed")
        self.sqls.append(s)
        if s.startswith("DESCRIBE DETAIL"):
            t = ref.group(1).rsplit("/", 1)[1]
            row = mock.MagicMock()
            row.asDict.return_value = {"tableFeatures": self.features.get(t, ["columnMapping"])}
            df = mock.MagicMock()
            df.collect.return_value = [row]
            return df
        if s.startswith("CREATE TABLE") and " AS SELECT " in s:  # the rewrite: plain protocol
            self.features[ref.group(1).rsplit("/", 1)[1]] = []
        return mock.MagicMock()


def _materialize(spark, present=(), **kw):
    removed = []
    out = stg.materialize(spark, src_db="main.st_10", tables=["a", "b", "c"],
                          roots=["/Volumes/m/s/v/x", "s3://b/x"],
                          has_table=lambda t: t in present, remove=removed.append,
                          log=lambda m: None, **kw)
    return out, removed


def test_materialize_clones_missing_tables_only():
    spark = StagingSpark()
    out, removed = _materialize(spark, present=("b",))
    assert sorted(out["cloned"]) == ["a", "c"] and out["rewritten"] == [] and removed == []
    assert out["root"] == "/Volumes/m/s/v/x"
    clones = [x for x in spark.sqls if "DEEP CLONE" in x]
    assert sorted(clones) == [
        f"CREATE OR REPLACE TABLE delta.`/Volumes/m/s/v/x/{t}` DEEP CLONE main.st_10.{t} "
        "TBLPROPERTIES ('delta.enableDeletionVectors' = 'false')" for t in ("a", "c")]
    assert sorted(x for x in spark.sqls if x.startswith("REORG")) == [
        f"REORG TABLE delta.`/Volumes/m/s/v/x/{t}` APPLY (PURGE)" for t in ("a", "c")]
    # every copy, kept or new, gets its features checked
    assert sum(x.startswith("DESCRIBE DETAIL") for x in spark.sqls) == 3


def test_materialize_force_and_nothing_to_do():
    spark = StagingSpark()
    out, _ = _materialize(spark, present=("a", "b", "c"))
    assert out["cloned"] == [] and not any("CLONE" in x for x in spark.sqls)
    spark = StagingSpark()
    out, _ = _materialize(spark, present=("a", "b", "c"), force=True)
    assert sorted(out["cloned"]) == ["a", "b", "c"]


def test_materialize_falls_back_to_the_s3_uri():
    spark = StagingSpark(reject=("/Volumes",))
    out, _ = _materialize(spark)
    assert out["root"] == "s3://b/x"
    assert all("delta.`s3://b/x/" in x for x in spark.sqls)
    spark = StagingSpark(reject=("/Volumes", "s3://"))
    try:
        _materialize(spark)
    except RuntimeError as e:
        assert "/Volumes/m/s/v/x" in str(e) and "s3://b/x" in str(e)
    else:
        raise AssertionError("no path form worked, yet materialize returned")


def test_materialize_rewrites_copies_oss_delta_cannot_read():
    spark = StagingSpark(features={"b": ["deletionVectors", "checkpointProtection"],
                                   "c": ["v2Checkpoint", "rowTracking", "clustering",
                                         "domainMetadata", "columnMapping"]})
    out, removed = _materialize(spark)
    assert out["rewritten"] == ["b"] and removed == ["b"]
    rw = [x for x in spark.sqls if " AS SELECT " in x]
    assert rw == ["CREATE TABLE delta.`/Volumes/m/s/v/x/b` USING DELTA TBLPROPERTIES ("
                  "'delta.enableDeletionVectors' = 'false', 'delta.enableRowTracking' = 'false', "
                  "'delta.enableInCommitTimestamps' = 'false', "
                  "'delta.checkpointPolicy' = 'classic', 'delta.columnMapping.mode' = 'none') "
                  "AS SELECT * FROM main.st_10.b"]
    assert stg.unsupported_features({}) == []
    assert stg.unsupported_features({"tableFeatures": ["variantType", "appendOnly"]}) == \
        ["variantType"]


def test_materialize_sf_checks_and_removes_through_the_volume():
    dbu = mock.MagicMock()
    logf = mock.MagicMock()
    logf.name = "00000000000000000000.json"
    dbu.fs.ls.side_effect = lambda path: [logf] if "/dimdate/" in path else []
    spark = StagingSpark()
    with mock.patch.object(stg, "materialize", wraps=stg.materialize) as m:
        stg.materialize_sf(spark, dbu, catalog="main", sf="10",
                           staging_delta=P["staging_delta"],
                           staging_delta_volume="/Volumes/main/s/v/_emr/staging_delta/sf=10/",
                           tables=["dimdate", "taxrate"])
    kw = m.call_args.kwargs
    assert kw["src_db"] == "main.tpcdi_incremental_staging_10"
    assert kw["roots"] == ["/Volumes/main/s/v/_emr/staging_delta/sf=10", P["staging_delta"]]
    assert kw["has_table"]("dimdate") and not kw["has_table"]("taxrate")
    kw["remove"]("taxrate")
    dbu.fs.rm.assert_called_with("/Volumes/main/s/v/_emr/staging_delta/sf=10/taxrate", True)
    assert [x for x in spark.sqls if "DEEP CLONE" in x] == [
        "CREATE OR REPLACE TABLE delta.`/Volumes/main/s/v/_emr/staging_delta/sf=10/taxrate` "
        "DEEP CLONE main.tpcdi_incremental_staging_10.taxrate "
        "TBLPROPERTIES ('delta.enableDeletionVectors' = 'false')"]


def test_staging_missing_needs_a_committed_log():
    s3 = mock.MagicMock()
    s3.list_objects_v2.side_effect = lambda **kw: {
        "KeyCount": 0 if kw["Prefix"].endswith("taxrate/_delta_log/0") else 1}
    assert c.staging_missing(s3, P, ["dimdate", "taxrate"]) == ["taxrate"]
    assert s3.list_objects_v2.call_args_list[0].kwargs["Prefix"] == \
        "prefix/_emr/staging_delta/sf=10/dimdate/_delta_log/0"


def test_cost_math():
    r = c.node_rate(PRICES, "m8gd.2xlarge", 32)
    assert abs(r["total"] - (0.46128 + 0.11532 + 32 * 0.08 / 730)) < 1e-9
    assert abs(c.fleet_rate(PRICES, {"m8gd.2xlarge": 3}, 32) - 3 * r["total"]) < 1e-9
    emr = _emr_client()
    t0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
    inst = [{"Id": f"ci-{i}", "Ec2InstanceId": f"i-{i}", "InstanceType": "m8gd.2xlarge",
             "InstanceGroupId": "ig", "Status": {"Timeline": {
                 "CreationDateTime": t0, "EndDateTime": t0 + timedelta(seconds=secs)}}}
            for i, secs in enumerate([3600, 1800, 10])]
    with Stubber(emr) as st:
        st.add_response("list_instances", {"Instances": inst})
        rep = c.cluster_cost(emr, "j-1", PRICES, root_gb=32)
    # 1 h + 0.5 h + the 60 s minimum
    assert abs(rep["instance_hours"] - (1.5 + 60 / 3600)) < 1e-3
    assert abs(rep["total_cost"] - rep["instance_hours"] * r["total"]) < 1e-3
    est = c.estimate(PRICES, _spec(core_count=2), startup_min=10, setup_min=5,
                     batch_min=3, n_batches=2)
    assert est["minutes"] == 21 and abs(est["fleet_usd_per_hr"] - 3 * r["total"]) < 1e-3


def test_drop_files_copies_one_file_per_dataset():
    s3 = mock.MagicMock()
    stage = P["staging_files"].split("/", 3)[3]

    def pages(Bucket, Prefix):
        if "_dailybatches" in Prefix:
            return [{"Contents": [{"Key": Prefix + "2016-07-05/Trade.txt"}]}]
        if "/HoldingHistory/" in Prefix:
            return [{}]  # sparse dataset: no file for the date
        return [{"Contents": [{"Key": Prefix + "_SUCCESS"},
                              {"Key": Prefix + "part-00000-x.c000.csv"}]}]
    s3.get_paginator.return_value.paginate.side_effect = pages
    pairs = c.drop_files(s3, P, D)
    assert len(pairs) == 6
    s3.delete_objects.assert_called_once()
    srcs = [cl.args[0]["Key"] for cl in s3.copy.call_args_list]
    dsts = [cl.args[2] for cl in s3.copy.call_args_list]
    assert all(k.startswith(stage) and k.endswith(".csv") for k in srcs)
    assert f"prefix/augmented_incremental/_dailybatches/test_emr_10/{D}/Trade.txt" in dsts


def test_batch_and_task_rows():
    t0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
    step = {"Id": "s-1", "Status": {"State": "COMPLETED", "Timeline": {
        "StartDateTime": t0, "EndDateTime": t0 + timedelta(seconds=120)}}}
    metrics = {"batch_date": D, "seconds": 100.0, "status": "SUCCESS",
               "tasks": [{"node": "dimtrade", "status": "SUCCESS", "seconds": 9.5,
                          "start": "a", "end": "b", "error": None}]}
    row = c.batch_row(D, step, metrics, drop_sec=2.0, rate=1.8)
    assert row["batch_cost_usd"] == round(120 / 3600 * 1.8, 6) and row["runner_sec"] == 100.0
    csv_text = c.to_csv(c.task_rows(metrics), c.TASK_FIELDS)
    assert csv_text.splitlines()[1].startswith(f"{D},dimtrade,SUCCESS,9.5")


def test_bench_plan_is_offline(capsys=None):
    argv = ["plan", "--s3-root", ROOT_URI, "--wh-db", "t", "--sf", "10", "--subnet", "subnet-1",
            "--master-sg", "sg-m", "--slave-sg", "sg-s", "--core-count", "2"]
    with mock.patch("boto3.Session", side_effect=AssertionError("plan must not call AWS")):
        emr_bench.main(argv)


def test_bench_guards_billable_commands():
    base = ["--s3-root", ROOT_URI, "--wh-db", "t", "--subnet", "s", "--master-sg", "m",
            "--slave-sg", "x"]
    for cmd in (["create"], ["smoke"], ["run", "--cluster-id", "j-1"]):
        with mock.patch("boto3.Session"):
            try:
                emr_bench.main(cmd + base)
            except SystemExit as e:
                assert "--yes" in str(e)
            else:
                raise AssertionError(f"{cmd} ran without --yes")


# --- Databricks job builder -------------------------------------------------

EMR_JOB = {
    "repo_src_path": "/Workspace/Users/test@test.com/repo/src",
    "catalog": "main", "scale_factor": 10,
    "tpcdi_directory": "/Volumes/main/tpcdi_raw_data/tpcdi_volume/",
    "wh_db": "test_aug", "s3_volume_prefix": ROOT_URI,
}


def _tasks(job):
    for t in job["tasks"]:
        yield t
        if "for_each_task" in t:
            yield t["for_each_task"]["task"]


def test_emr_builder_shape_and_paths():
    from workflow_builders import augmented_emr as b
    child = b.build_child(job_name="c", aws_service_credential="cred", **EMR_JOB)
    parent = b.build_parent(job_name="p", child_job_id=7, emr_subnet_id="subnet-1",
                            emr_master_sg="sg-m", emr_slave_sg="sg-s", **EMR_JOB)
    assert [t["task_key"] for t in child["tasks"]] == ["simulate_filedrops_emr", "run_emr"]
    assert [t["task_key"] for t in parent["tasks"]] == ["setup_emr", "loop_incremental_tpcdi",
                                                         "teardown_emr"]
    assert parent["tasks"][2]["run_if"] == "ALL_DONE"
    for job in (child, parent):
        assert job["environments"][0]["spec"]["client"] == "5"
        names = {p["name"] for p in job["parameters"]}
        for t in _tasks(job):
            nb = t.get("notebook_task", {}).get("notebook_path")
            if nb:
                assert t["environment_key"] == "serverless_emr"
                rel = nb.replace(EMR_JOB["repo_src_path"] + "/", "")
                assert (ROOT / "src" / f"{rel}.py").exists(), rel
            params = (t.get("notebook_task", {}).get("base_parameters")
                      or t.get("run_job_task", {}).get("job_parameters") or {})
            for v in params.values():
                for ref in re.findall(r"\{\{job\.parameters\.(\w+)\}\}", v):
                    assert ref in names, (job["name"], t["task_key"], ref)
    loop = parent["tasks"][1]["for_each_task"]["task"]["run_job_task"]
    assert loop["job_id"] == 7
    assert loop["job_parameters"]["emr_cluster_id"] == "{{tasks.setup_emr.values.emr_cluster_id}}"
    child_names = {p["name"] for p in child["parameters"]}
    assert set(loop["job_parameters"]) <= child_names
    classic = b.build_child(job_name="c", interactive_cluster_id="0101-x", **EMR_JOB)
    assert "environments" not in classic
    assert all(t["existing_cluster_id"] == "0101-x" for t in classic["tasks"])


def test_competitor_dispatch_offers_emr_on_aws():
    import generate_competitor_workflow as g
    assert "emr" in g.competitors_for_cloud("AWS")
    assert "emr" not in g.competitors_for_cloud("Azure") + g.competitors_for_cloud("GCP")
    posted = []

    def api_call(body, method, endpoint):
        posted.append(body)
        return mock.Mock(status_code=200, text=json.dumps({"job_id": len(posted)}))
    pid = g.generate_competitor_workflow(
        engine="emr", scale_factor=10, catalog="main", wh_db="t",
        tpcdi_directory=EMR_JOB["tpcdi_directory"], repo_src_path=EMR_JOB["repo_src_path"],
        api_call=api_call, name_prefix="pfx",
        engine_params=dict(s3_volume_prefix=ROOT_URI, aws_region="us-west-2",
                           aws_service_credential="cred", emr_subnet_id="subnet-1",
                           emr_master_sg="sg-m", emr_slave_sg="sg-s", emr_cluster_id="",
                           emr_instance_type="m8gd.2xlarge", emr_core_count=2))
    assert pid == 2 and [j["name"] for j in posted] == [
        "pfx-SF10-AugmentedIncremental-EMR-Child", "pfx-SF10-AugmentedIncremental-EMR-Parent"]
    defaults = {p["name"]: p["default"] for p in posted[1]["parameters"]}
    assert defaults["emr_core_count"] == "2" and defaults["emr_cluster_id"] == ""
    assert defaults["aws_service_credential"] == "cred"


if __name__ == "__main__":
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    for t in tests:
        t()
        print(f"  ok {t.__name__}")
    print(f"\nAll {len(tests)} EMR tests passed.")
