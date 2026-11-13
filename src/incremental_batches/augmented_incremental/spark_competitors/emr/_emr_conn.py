"""AWS control plane for the EMR variant: cluster spec, steps, file drops,
metrics and cost.

Pure boto3 with the clients passed in, so the same code runs from the laptop
driver (`emr_bench.py`, SSO profile) and from the Databricks tasks (a UC
service credential or an instance profile). No Spark here.

S3 layout under `s3_root` (the bucket prefix behind the UC volume, e.g.
`s3://<bucket>/<prefix>/`, the same root the Redshift variant reads):
  _emr/staging_delta/sf={sf}/{table}/            Delta staging (DEEP CLONE of the UC staging, once per SF)
  augmented_incremental/_staging/sf={sf}/...     pre-staged daily part files
  augmented_incremental/_dailybatches/{db}/{date}/{Dataset}.txt   the day's drop
  _emr/code/                                     jobs/*.py uploaded for the steps
  _emr/warehouse/{db}.db/                        Delta tables (Glue database {db})
  _emr/metrics/{db}/{run_id}/                    per-batch JSON + CSV reports
  _emr/logs/                                     EMR LogUri
"""
from __future__ import annotations

import csv
import io
import json
import os
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable, Iterable

DEFAULT_RELEASE = "emr-7.14.0"
DEFAULT_INSTANCE_TYPE = "m8gd.2xlarge"
DEFAULT_ROOT_GB = 32
REMOTE_CODE_DIR = "/home/hadoop/tpcdi_emr"
FIRST_BATCH = date(2016, 7, 6)
DATASETS = ["Customer", "Account", "Trade", "CashTransaction",
            "HoldingHistory", "DailyMarket", "WatchHistory"]
JOB_FILES = ["emr_runner.py", "tpcdi_sql.py", "tpcdi_dag.py"]
HOURS_PER_MONTH = 730  # AWS's convention for prorating GB-month storage


# --- paths ------------------------------------------------------------------

def split_s3(uri: str) -> tuple[str, str]:
    if not uri.startswith("s3://"):
        raise ValueError(f"not an s3:// uri: {uri}")
    bucket, _, key = uri[5:].partition("/")
    return bucket, key


def db_name(wh_db: str, scale_factor: int | str) -> str:
    # Glue lower-cases names; the Redshift file-drop notebook lower-cases too.
    return f"{wh_db}_{scale_factor}".lower()


def paths(s3_root: str, wh_db: str, scale_factor: int | str, run_id: str = "") -> dict:
    root = s3_root.rstrip("/") + "/"
    if not root.startswith("s3://"):
        raise ValueError("s3_root must be an s3:// uri")
    db = db_name(wh_db, scale_factor)
    p = {
        "db": db,
        "staging_delta":   f"{root}_emr/staging_delta/sf={scale_factor}",
        "staging_files":   f"{root}augmented_incremental/_staging/sf={scale_factor}",
        "dailybatches":    f"{root}augmented_incremental/_dailybatches/{db}",
        "code":            f"{root}_emr/code/",
        "db_location":     f"{root}_emr/warehouse/{db}.db",
        "metrics":         f"{root}_emr/metrics/{db}/{run_id}" if run_id else f"{root}_emr/metrics/{db}",
        "logs":            f"{root}_emr/logs/",
    }
    return p


def batch_dates(n: int, start_index: int = 0) -> list[str]:
    n = max(1, min(365, int(n)))
    return [(FIRST_BATCH + timedelta(days=i)).isoformat()
            for i in range(start_index, min(365, start_index + n))]


# --- cluster spec -----------------------------------------------------------

def cluster_spec(*, name: str, subnet_id: str, master_sg: str, slave_sg: str,
                 log_uri: str, core_count: int,
                 instance_type: str = DEFAULT_INSTANCE_TYPE,
                 master_instance_type: str | None = None,
                 release_label: str = DEFAULT_RELEASE,
                 service_role: str = "EMR_DefaultRole",
                 job_flow_role: str = "EMR_EC2_DefaultRole",
                 root_gb: int = DEFAULT_ROOT_GB,
                 idle_timeout_secs: int = 3600,
                 tags: dict[str, str] | None = None) -> dict:
    """RunJobFlow kwargs. Fixed-size on-demand cluster: no managed scaling and
    no dynamic allocation, one whole-node executor per core node, so every
    batch runs on the same compute (the EMR analog of Fabric's fixed pool).
    The idle auto-termination is only a safety net; the drivers terminate
    the cluster explicitly."""
    if core_count < 1:
        raise ValueError("core_count must be >= 1")
    return {
        "Name": name,
        "ReleaseLabel": release_label,
        "LogUri": log_uri,
        "Applications": [{"Name": "Spark"}, {"Name": "Hadoop"}],
        "ServiceRole": service_role,
        "JobFlowRole": job_flow_role,
        "VisibleToAllUsers": True,
        "StepConcurrencyLevel": 1,
        "ScaleDownBehavior": "TERMINATE_AT_TASK_COMPLETION",
        "AutoTerminationPolicy": {"IdleTimeout": int(idle_timeout_secs)},
        # No root-volume overrides: the account's EMR_DefaultRole carries the
        # v1 AmazonElasticMapReduceRole policy, which lacks the EC2 rights a
        # pinned gp3 root (IOPS/throughput) needs ("insufficient EC2
        # permissions"). EMR's default root volume is used, as the account's
        # other clusters do; the d-type instances shuffle to local NVMe.
        "Instances": {
            "InstanceGroups": [
                {"Name": "primary", "InstanceRole": "MASTER", "Market": "ON_DEMAND",
                 "InstanceType": master_instance_type or instance_type, "InstanceCount": 1},
                {"Name": "core", "InstanceRole": "CORE", "Market": "ON_DEMAND",
                 "InstanceType": instance_type, "InstanceCount": int(core_count)},
            ],
            "Ec2SubnetId": subnet_id,
            "EmrManagedMasterSecurityGroup": master_sg,
            "EmrManagedSlaveSecurityGroup": slave_sg,
            "KeepJobFlowAliveWhenNoSteps": True,
            "TerminationProtected": False,
        },
        "Configurations": [
            {"Classification": "delta-defaults", "Properties": {"delta.enabled": "true"}},
            {"Classification": "spark", "Properties": {"maximizeResourceAllocation": "true"}},
            {"Classification": "spark-defaults", "Properties": {
                "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
                "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                "spark.dynamicAllocation.enabled": "false",
                "spark.executor.instances": str(int(core_count)),
                "spark.scheduler.mode": "FAIR",
                # A cached S3 filesystem closed by one thread (Hive's DROP
                # DATABASE CASCADE) and reused by the parallel clones failed
                # setup with "Audit Manager Service ... STOPPED"; uncached
                # filesystems cost a little per open and never go stale.
                "spark.hadoop.fs.s3a.impl.disable.cache": "true",
                "spark.hadoop.fs.s3.impl.disable.cache": "true",
            }},
            {"Classification": "spark-hive-site", "Properties": {
                "hive.metastore.client.factory.class":
                    "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
            }},
        ],
        "Tags": [{"Key": k, "Value": v} for k, v in
                 {"Project": "tpcdi", "Variant": "emr", **(tags or {})}.items()],
    }


def validate_cluster_spec(spec: dict, service_model_path: str | None = None) -> None:
    """Offline check of the spec against botocore's RunJobFlow input shape
    (the same validation boto3 runs before sending). Raises on any error.
    `service_model_path` points at a newer emr service-2.json (e.g. the AWS
    CLI's bundled one) when the installed botocore is older."""
    import botocore.session
    from botocore.model import ServiceModel
    from botocore.validate import ParamValidator
    if service_model_path:
        with open(service_model_path) as f:
            model = ServiceModel(json.load(f), service_name="emr")
    else:
        model = botocore.session.get_session().get_service_model("emr")
    report = ParamValidator().validate(spec, model.operation_model("RunJobFlow").input_shape)
    if report.has_errors():
        raise ValueError(report.generate_report())


def instance_counts(spec: dict) -> dict[str, int]:
    out: dict[str, int] = {}
    for g in spec["Instances"]["InstanceGroups"]:
        out[g["InstanceType"]] = out.get(g["InstanceType"], 0) + g["InstanceCount"]
    return out


# --- steps ------------------------------------------------------------------

def _step(name: str, args: list[str]) -> dict:
    return {"Name": name, "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {"Jar": "command-runner.jar", "Args": args}}


def sync_code_step(code_uri: str) -> dict:
    return _step("tpcdi sync code", ["bash", "-c",
                 f"mkdir -p {REMOTE_CODE_DIR} && aws s3 sync --delete {code_uri} {REMOTE_CODE_DIR}/"])


def _spark_submit(mode: str, args: list[str]) -> list[str]:
    # Client mode: the driver runs on the primary node, leaving every core
    # node's single executor to the transforms.
    return ["spark-submit", "--deploy-mode", "client",
            f"{REMOTE_CODE_DIR}/emr_runner.py", mode, *args]


def setup_step(p: dict, shuffle_partitions: str = "1000") -> dict:
    return _step(f"tpcdi setup {p['db']}", _spark_submit("setup", [
        "--db", p["db"], "--db-location", p["db_location"],
        "--staging-delta", p["staging_delta"],
        "--shuffle-partitions", shuffle_partitions]))


def drop_step(p: dict) -> dict:
    return _step(f"tpcdi drop {p['db']}", _spark_submit("drop", [
        "--db", p["db"], "--db-location", p["db_location"]]))


def batch_step(p: dict, batch_date: str, *, concurrency: int = 8,
               shuffle_partitions: str = "1000") -> dict:
    return _step(f"tpcdi batch {p['db']} {batch_date}", _spark_submit("batch", [
        "--db", p["db"], "--batch-date", batch_date,
        "--batch-dir", f"{p['dailybatches']}/{batch_date}",
        "--concurrency", str(concurrency),
        "--shuffle-partitions", shuffle_partitions,
        "--metrics-path", metrics_uri(p, batch_date)]))


def metrics_uri(p: dict, batch_date: str) -> str:
    return f"{p['metrics']}/batch={batch_date}.json"


# --- cluster / step lifecycle ------------------------------------------------

def to_volume(uri: str, s3_volume_prefix: str, volume_root: str) -> str:
    """Map an s3:// uri under the volume's bucket prefix to its /Volumes path,
    so the Databricks tasks can read and write it with volume permissions."""
    pre = s3_volume_prefix.rstrip("/") + "/"
    if not uri.startswith(pre):
        raise ValueError(f"{uri} is not under {pre}")
    return volume_root.rstrip("/") + "/" + uri[len(pre):]


def boto3_session(region: str, *, dbutils=None, service_credential: str = ""):
    """A UC service credential when one is named, else the default chain
    (instance profile on classic compute, or the local AWS profile)."""
    import boto3
    if service_credential:
        return boto3.Session(
            botocore_session=dbutils.credentials.getServiceCredentialsProvider(service_credential),
            region_name=region)
    return boto3.Session(region_name=region)


def create_cluster(emr, spec: dict) -> str:
    return emr.run_job_flow(**spec)["JobFlowId"]


def _wait(poll: Callable[[], tuple[bool, Any]], *, timeout_secs: int, interval_secs: float,
          what: str, sleep: Callable[[float], None] = time.sleep):
    deadline = time.time() + timeout_secs
    while True:
        done, val = poll()
        if done:
            return val
        if time.time() > deadline:
            raise TimeoutError(f"timed out after {timeout_secs}s waiting for {what}")
        sleep(interval_secs)


def wait_cluster_ready(emr, cluster_id: str, *, timeout_secs: int = 1800,
                       interval_secs: float = 20, sleep=time.sleep) -> dict:
    def poll():
        c = emr.describe_cluster(ClusterId=cluster_id)["Cluster"]
        st = c["Status"]["State"]
        if st in ("WAITING", "RUNNING"):
            return True, c
        if st.startswith("TERMINAT"):
            reason = c["Status"].get("StateChangeReason", {})
            raise RuntimeError(f"cluster {cluster_id} {st}: {reason}")
        return False, None
    return _wait(poll, timeout_secs=timeout_secs, interval_secs=interval_secs,
                 what=f"cluster {cluster_id}", sleep=sleep)


class StepFailed(RuntimeError):
    """A step ended FAILED/CANCELLED/INTERRUPTED; `.step` is its DescribeStep record."""
    def __init__(self, msg: str, step: dict):
        super().__init__(msg)
        self.step = step


def run_step(emr, cluster_id: str, step: dict, *, timeout_secs: int = 14400,
             interval_secs: float = 10, sleep=time.sleep) -> dict:
    """Submit one step, block until it ends, return its DescribeStep record.
    Raises StepFailed when the step doesn't COMPLETE."""
    step_id = emr.add_job_flow_steps(JobFlowId=cluster_id, Steps=[step])["StepIds"][0]

    def poll():
        s = emr.describe_step(ClusterId=cluster_id, StepId=step_id)["Step"]
        st = s["Status"]["State"]
        if st == "COMPLETED":
            return True, s
        if st in ("FAILED", "CANCELLED", "INTERRUPTED"):
            fail = s["Status"].get("FailureDetails", {})
            raise StepFailed(f"step {step_id} ({step['Name']}) {st}: "
                             f"{fail.get('Message', '')} log={fail.get('LogFile', '')}", s)
        return False, None
    return _wait(poll, timeout_secs=timeout_secs, interval_secs=interval_secs,
                 what=f"step {step_id}", sleep=sleep)


def step_seconds(step: dict) -> float | None:
    tl = step["Status"].get("Timeline", {})
    if tl.get("StartDateTime") and tl.get("EndDateTime"):
        return (tl["EndDateTime"] - tl["StartDateTime"]).total_seconds()
    return None


def cluster_counts(emr, cluster_id: str) -> dict[str, int]:
    out: dict[str, int] = {}
    for g in emr.list_instance_groups(ClusterId=cluster_id)["InstanceGroups"]:
        out[g["InstanceType"]] = out.get(g["InstanceType"], 0) + g["RequestedInstanceCount"]
    return out


def terminate(emr, cluster_id: str) -> None:
    emr.terminate_job_flows(JobFlowIds=[cluster_id])


# --- code upload / file drops ------------------------------------------------

def upload_code(s3, code_uri: str, jobs_dir: str) -> list[str]:
    bucket, prefix = split_s3(code_uri)
    out = []
    for f in JOB_FILES:
        s3.upload_file(os.path.join(jobs_dir, f), bucket, prefix + f)
        out.append(f"s3://{bucket}/{prefix}{f}")
    return out


def _list(s3, uri: str) -> list[str]:
    bucket, prefix = split_s3(uri.rstrip("/") + "/")
    keys = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        keys += [o["Key"] for o in page.get("Contents", [])]
    return keys


def delete_prefix(s3, uri: str) -> int:
    bucket, _ = split_s3(uri)
    keys = _list(s3, uri)
    for i in range(0, len(keys), 1000):
        s3.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": k} for k in keys[i:i + 1000]]})
    return len(keys)


def drop_files(s3, p: dict, batch_date: str, file_ext: str = "txt") -> list[tuple[str, str]]:
    """Laptop-path twin of simulate_filedrops_rs: clear the previous day's
    drop, then server-side copy each dataset's single part file for this date
    to {dailybatches}/{date}/{Dataset}.{ext}. Sparse datasets with no file
    for the date are skipped."""
    read_ext = "csv" if file_ext == "txt" else file_ext
    delete_prefix(s3, p["dailybatches"])
    bucket, _ = split_s3(p["staging_files"])
    pairs = []
    for ds in DATASETS:
        parts = [k for k in _list(s3, f"{p['staging_files']}/{ds}/_pdate={batch_date}")
                 if k.rsplit("/", 1)[-1].startswith("part-") and k.endswith(f".{read_ext}")]
        if not parts:
            continue
        if len(parts) > 1:
            raise RuntimeError(f"{ds} {batch_date}: expected 1 part file, got {len(parts)}")
        _, dst_key = split_s3(f"{p['dailybatches']}/{batch_date}/{ds}.{file_ext}")
        # Managed copy: multipart for objects over 5 GB (large SF Trade days).
        s3.copy({"Bucket": bucket, "Key": parts[0]}, bucket, dst_key)
        pairs.append((f"s3://{bucket}/{parts[0]}", f"s3://{bucket}/{dst_key}"))
    return pairs


def staging_missing(s3, p: dict, tables: Iterable[str]) -> list[str]:
    """Tables of the Delta staging without a committed log (the materialize
    writes _delta_log/ only when a table's clone commits)."""
    bucket, prefix = split_s3(p["staging_delta"].rstrip("/") + "/")
    missing = []
    for t in tables:
        r = s3.list_objects_v2(Bucket=bucket, Prefix=f"{prefix}{t}/_delta_log/0", MaxKeys=1)
        if not r.get("KeyCount"):
            missing.append(t)
    return missing


def read_json(s3, uri: str) -> dict:
    bucket, key = split_s3(uri)
    return json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())


def put_text(s3, uri: str, text: str) -> None:
    bucket, key = split_s3(uri)
    s3.put_object(Bucket=bucket, Key=key, Body=text.encode("utf-8"))


def drop_glue_database(glue, db: str) -> bool:
    try:
        glue.delete_database(Name=db)
        return True
    except glue.exceptions.EntityNotFoundException:
        return False


# --- pricing and cost --------------------------------------------------------

def _od_price(pricing, service: str, filters: dict) -> float:
    r = pricing.get_products(
        ServiceCode=service, MaxResults=10,
        Filters=[{"Type": "TERM_MATCH", "Field": k, "Value": v} for k, v in filters.items()])
    for item in r["PriceList"]:
        d = json.loads(item) if isinstance(item, str) else item
        for term in d["terms"].get("OnDemand", {}).values():
            for dim in term["priceDimensions"].values():
                return float(dim["pricePerUnit"]["USD"])
    raise LookupError(f"no on-demand price for {service} {filters}")


def fetch_prices(pricing, region: str, instance_types: Iterable[str]) -> dict:
    """On-demand list prices from the AWS Pricing API (the client must be in
    us-east-1). EC2 Linux shared tenancy + the EMR per-instance uplift + gp3."""
    out: dict[str, Any] = {"region": region, "ec2": {}, "emr": {},
                           "source": "AWS Pricing API (on-demand)",
                           "fetched_at": datetime.now(timezone.utc).date().isoformat()}
    for it in sorted(set(instance_types)):
        out["ec2"][it] = _od_price(pricing, "AmazonEC2", {
            "regionCode": region, "instanceType": it, "operatingSystem": "Linux",
            "tenancy": "Shared", "preInstalledSw": "NA", "capacitystatus": "Used",
            "licenseModel": "No License required"})
        out["emr"][it] = _od_price(pricing, "ElasticMapReduce", {
            "regionCode": region, "instanceType": it, "softwareType": "EMR"})
    out["ebs_gp3_gb_month"] = _od_price(pricing, "AmazonEC2", {
        "regionCode": region, "productFamily": "Storage", "volumeApiName": "gp3"})
    return out


def node_rate(prices: dict, instance_type: str, root_gb: int = DEFAULT_ROOT_GB) -> dict:
    ec2, emr = prices["ec2"][instance_type], prices["emr"][instance_type]
    ebs = root_gb * prices["ebs_gp3_gb_month"] / HOURS_PER_MONTH
    return {"ec2": ec2, "emr": emr, "ebs": ebs, "total": ec2 + emr + ebs}


def fleet_rate(prices: dict, counts: dict[str, int], root_gb: int = DEFAULT_ROOT_GB) -> float:
    """$/hour for the whole cluster while it is up."""
    return sum(n * node_rate(prices, it, root_gb)["total"] for it, n in counts.items())


def cluster_cost(emr, cluster_id: str, prices: dict, *, root_gb: int = DEFAULT_ROOT_GB,
                 now: datetime | None = None) -> dict:
    """Actual cluster cost from each instance's lifetime (ListInstances),
    billed per second with EC2's 60-second minimum."""
    now = now or datetime.now(timezone.utc)
    lines, totals = [], {"ec2": 0.0, "emr": 0.0, "ebs": 0.0}
    for page in emr.get_paginator("list_instances").paginate(ClusterId=cluster_id):
        for i in page["Instances"]:
            tl = i["Status"].get("Timeline", {})
            start = tl.get("CreationDateTime")
            if not start:
                continue
            end = tl.get("EndDateTime") or now
            hrs = max(60.0, (end - start).total_seconds()) / 3600
            r = node_rate(prices, i["InstanceType"], root_gb)
            for k in totals:
                totals[k] += r[k] * hrs
            lines.append({"instance_id": i.get("Ec2InstanceId"), "instance_type": i["InstanceType"],
                          "group": i.get("InstanceGroupId"), "hours": round(hrs, 4),
                          "cost": round(r["total"] * hrs, 4)})
    return {"cluster_id": cluster_id, "instances": lines,
            "instance_hours": round(sum(x["hours"] for x in lines), 4),
            **{f"{k}_cost": round(v, 4) for k, v in totals.items()},
            "total_cost": round(sum(totals.values()), 4)}


# --- reports -----------------------------------------------------------------

BATCH_FIELDS = ["batch_date", "step_id", "step_start", "step_end", "step_sec",
                "runner_sec", "drop_sec", "status", "fleet_usd_per_hr", "batch_cost_usd"]
TASK_FIELDS = ["batch_date", "table_name", "status", "task_sec", "start", "end", "error"]


def batch_row(batch_date: str, step: dict, metrics: dict | None, *, drop_sec: float | None,
              rate: float) -> dict:
    tl = step["Status"].get("Timeline", {})
    secs = step_seconds(step)
    return {
        "batch_date": batch_date, "step_id": step.get("Id"),
        "step_start": tl.get("StartDateTime").isoformat() if tl.get("StartDateTime") else None,
        "step_end": tl.get("EndDateTime").isoformat() if tl.get("EndDateTime") else None,
        "step_sec": secs, "runner_sec": (metrics or {}).get("seconds"),
        "drop_sec": None if drop_sec is None else round(drop_sec, 3),
        "status": (metrics or {}).get("status", step["Status"]["State"]),
        "fleet_usd_per_hr": round(rate, 6),
        "batch_cost_usd": None if secs is None else round(secs / 3600 * rate, 6),
    }


def task_rows(metrics: dict) -> list[dict]:
    return [{"batch_date": metrics["batch_date"], "table_name": t["node"], "status": t["status"],
             "task_sec": t["seconds"], "start": t["start"], "end": t["end"], "error": t["error"]}
            for t in metrics.get("tasks", [])]


def to_csv(rows: list[dict], fields: list[str]) -> str:
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=fields, extrasaction="ignore")
    w.writeheader()
    w.writerows(rows)
    return buf.getvalue()


def estimate(prices: dict, spec: dict, *, startup_min: float, setup_min: float,
             batch_min: float, n_batches: int, root_gb: int = DEFAULT_ROOT_GB) -> dict:
    rate = fleet_rate(prices, instance_counts(spec), root_gb)
    minutes = startup_min + setup_min + batch_min * n_batches
    return {"fleet_usd_per_hr": round(rate, 4), "minutes": round(minutes, 1),
            "cost_usd": round(rate * minutes / 60, 2)}
