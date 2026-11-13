"""Laptop driver for the EMR variant (boto3 + an AWS CLI profile).

Does what the Databricks parent/child jobs do, from any machine with AWS
credentials for the account: create the cluster, run setup, then per batch
drop the day's files into S3 and run that day's EMR step, recording step
timings, per-table timings and cost. Use it when the Databricks workspace has
no AWS identity that may call EMR.

  plan       print + validate the cluster spec and the cost estimate (no AWS writes)
  create     create the cluster                                  (needs --yes)
  setup      upload jobs/, sync them onto the cluster, reset the run database
  run        run batches on an existing cluster                  (needs --yes)
  report     write the cost report for a cluster
  terminate  terminate a cluster
  cleanup    drop the Glue database and the run's S3 tables + file drops
  smoke      setup + N batches                                    (needs --yes)

`smoke` defaults to --cluster-mode job: setup and every batch each run on
their own EMR job cluster, created with its steps and terminated by EMR when
they end, as a daily Databricks classic job cluster would. A batch's cost is
then its whole cluster's (boot + step + shutdown). --cluster-mode shared runs
setup and all batches on one cluster (create -> setup -> run N -> terminate).

Every command takes the same infrastructure flags; nothing account-specific
is defaulted. Example (plan only):
  python emr_bench.py plan --profile <p> --s3-root s3://<bucket>/<prefix>/ \
      --wh-db <prefix>_emr --sf 10 --subnet subnet-... --master-sg sg-... \
      --slave-sg sg-... --core-count 2
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _HERE)
sys.path.insert(0, os.path.join(_HERE, "jobs"))
import _emr_conn as emr_c  # noqa: E402
import tpcdi_sql  # noqa: E402

PRICES_SNAPSHOT = os.path.join(_HERE, "prices_snapshot.json")


def _args(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("command", choices=["plan", "create", "setup", "run", "report",
                                        "terminate", "cleanup", "smoke"])
    ap.add_argument("--profile", help="AWS CLI profile (default: the standard credential chain)")
    ap.add_argument("--region", default="us-west-2")
    ap.add_argument("--s3-root", required=True, help="s3:// root behind the UC volume")
    ap.add_argument("--wh-db", required=True, help="run prefix; Glue database = {wh_db}_{sf}")
    ap.add_argument("--sf", type=int, default=10)
    ap.add_argument("--cluster-id")
    ap.add_argument("--run-id", help="metrics folder name (default: UTC timestamp)")
    ap.add_argument("--yes", action="store_true", help="confirm commands that start billable compute")
    # cluster
    ap.add_argument("--subnet")
    ap.add_argument("--master-sg")
    ap.add_argument("--slave-sg")
    ap.add_argument("--instance-type", default=emr_c.DEFAULT_INSTANCE_TYPE)
    ap.add_argument("--master-instance-type")
    ap.add_argument("--core-count", type=int, default=2)
    ap.add_argument("--release", default=emr_c.DEFAULT_RELEASE)
    ap.add_argument("--service-role", default="EMR_DefaultRole")
    ap.add_argument("--instance-profile", default="EMR_EC2_DefaultRole")
    ap.add_argument("--root-gb", type=int, default=emr_c.DEFAULT_ROOT_GB)
    ap.add_argument("--idle-timeout", type=int, default=3600)
    ap.add_argument("--tag", action="append", default=[], help="Key=Value, repeatable")
    ap.add_argument("--service-model", help="newer emr service-2.json for `plan` validation")
    # run
    ap.add_argument("--cluster-mode", choices=["job", "shared"], default="job",
                    help="smoke: a job cluster per setup/batch (default) or one shared cluster")
    ap.add_argument("--continue-run", action="store_true",
                    help="job mode: skip setup and continue the existing database's batches "
                         "(use with --start-batch)")
    ap.add_argument("--batches", type=int, default=2)
    ap.add_argument("--start-batch", type=int, default=0, help="0-based index of the first batch")
    ap.add_argument("--concurrency", type=int, default=8)
    ap.add_argument("--shuffle-partitions", default="1000")
    ap.add_argument("--prices", default=PRICES_SNAPSHOT,
                    help="prices JSON; 'live' fetches from the AWS Pricing API")
    # plan-only runtime assumptions for the estimate
    ap.add_argument("--est-startup-min", type=float, default=10)
    ap.add_argument("--est-setup-min", type=float, default=5)
    ap.add_argument("--est-batch-min", type=float, default=3)
    return ap.parse_args(argv)


def _session(a):
    import boto3
    return boto3.Session(profile_name=a.profile, region_name=a.region)


def _spec(a) -> dict:
    missing = [f for f in ("subnet", "master_sg", "slave_sg") if not getattr(a, f)]
    if missing:
        raise SystemExit(f"missing cluster flags: {['--' + m.replace('_', '-') for m in missing]}")
    tags = dict(t.split("=", 1) for t in a.tag)
    tags.setdefault("RunName", f"tpcdi-emr-{emr_c.db_name(a.wh_db, a.sf)}")
    p = emr_c.paths(a.s3_root, a.wh_db, a.sf)
    return emr_c.cluster_spec(
        name=tags["RunName"], subnet_id=a.subnet, master_sg=a.master_sg, slave_sg=a.slave_sg,
        log_uri=p["logs"], core_count=a.core_count, instance_type=a.instance_type,
        master_instance_type=a.master_instance_type, release_label=a.release,
        service_role=a.service_role, job_flow_role=a.instance_profile, root_gb=a.root_gb,
        idle_timeout_secs=a.idle_timeout, tags=tags)


def _prices(a, sess, types) -> dict:
    if a.prices == "live":
        return emr_c.fetch_prices(sess.client("pricing", region_name="us-east-1"), a.region, types)
    with open(a.prices) as f:
        pr = json.load(f)
    unknown = [t for t in types if t not in pr["ec2"]]
    if unknown:
        raise SystemExit(f"{a.prices} has no price for {unknown}; pass --prices live")
    return pr


def _out(obj):
    print(json.dumps(obj, indent=1, default=str))


def cmd_plan(a):
    spec = _spec(a)
    emr_c.validate_cluster_spec(spec, a.service_model)
    print("RunJobFlow spec (validated offline against the EMR API model):")
    _out(spec)
    p = emr_c.paths(a.s3_root, a.wh_db, a.sf, a.run_id or "<run_id>")
    print("\nS3 / Glue layout:")
    _out(p)
    print("\nFirst batch step:")
    _out(emr_c.batch_step(p, emr_c.batch_dates(1)[0], concurrency=a.concurrency,
                          shuffle_partitions=a.shuffle_partitions))
    pr = _prices(a, _session(a) if a.prices == "live" else None, emr_c.instance_counts(spec))
    est = emr_c.estimate(pr, spec, startup_min=a.est_startup_min, setup_min=a.est_setup_min,
                         batch_min=a.est_batch_min, n_batches=a.batches, root_gb=a.root_gb)
    print(f"\nPrices: {pr['source']} fetched {pr['fetched_at']}")
    for it in emr_c.instance_counts(spec):
        print(f"  {it}: {emr_c.node_rate(pr, it, a.root_gb)}")
    print(f"Estimate for {a.batches} batches (startup {a.est_startup_min}m + setup "
          f"{a.est_setup_min}m + {a.est_batch_min}m/batch):")
    _out(est)


def cmd_create(a, sess) -> str:
    if not a.yes:
        raise SystemExit("create starts billable EC2 + EMR; re-run with --yes")
    spec = _spec(a)
    emr_c.validate_cluster_spec(spec, a.service_model)
    emr = sess.client("emr")
    cid = emr_c.create_cluster(emr, spec)
    print(f"[create] {cid}; waiting for WAITING...", flush=True)
    emr_c.wait_cluster_ready(emr, cid)
    print(f"[create] {cid} ready", flush=True)
    return cid


def cmd_setup(a, sess, cid: str):
    s3, emr = sess.client("s3"), sess.client("emr")
    p = emr_c.paths(a.s3_root, a.wh_db, a.sf)
    missing = emr_c.staging_missing(s3, p, tpcdi_sql.STAGING_TABLES)
    if missing:
        raise SystemExit(f"Delta staging {p['staging_delta']} lacks {missing}; run the "
                         f"Databricks materialize_staging_emr notebook (or setup_emr) "
                         f"once for SF={a.sf}")
    print(f"[setup] upload jobs -> {p['code']}: {emr_c.upload_code(s3, p['code'], os.path.join(_HERE, 'jobs'))}")
    emr_c.run_step(emr, cid, emr_c.sync_code_step(p["code"]))
    # One retry: a transient S3 filesystem error (e.g. "Audit Manager Service
    # ... STOPPED") must not cost a whole cluster start.
    try:
        st = emr_c.run_step(emr, cid, emr_c.setup_step(p, a.shuffle_partitions))
    except emr_c.StepFailed as e:
        print(f"[setup] retrying once after: {str(e)[:200]}", flush=True)
        st = emr_c.run_step(emr, cid, emr_c.setup_step(p, a.shuffle_partitions))
    print(f"[setup] {p['db']} reset in {emr_c.step_seconds(st)}s (step {st['Id']})")


def cmd_run(a, sess, cid: str, run_id: str) -> list[dict]:
    if not a.yes:
        raise SystemExit("run submits EMR steps on a billable cluster; re-run with --yes")
    s3, emr = sess.client("s3"), sess.client("emr")
    p = emr_c.paths(a.s3_root, a.wh_db, a.sf, run_id)
    c = emr.describe_cluster(ClusterId=cid)["Cluster"]
    counts = emr_c.cluster_counts(emr, cid)
    rate = emr_c.fleet_rate(_prices(a, sess, counts), counts, a.root_gb)
    print(f"[run] cluster {cid} ({c['Status']['State']}) {counts} at ${rate:.4f}/hr; "
          f"metrics -> {p['metrics']}", flush=True)
    batches, tasks = [], []
    for d in emr_c.batch_dates(a.batches, a.start_batch):
        t0 = time.time()
        drops = emr_c.drop_files(s3, p, d)
        drop_sec = time.time() - t0
        metrics, err = None, None
        try:
            step = emr_c.run_step(emr, cid, emr_c.batch_step(
                p, d, concurrency=a.concurrency, shuffle_partitions=a.shuffle_partitions))
        except emr_c.StepFailed as e:
            err, step = e, e.step
        try:
            metrics = emr_c.read_json(s3, emr_c.metrics_uri(p, d))
        except s3.exceptions.NoSuchKey:
            pass  # the runner died before writing its metrics
        row = emr_c.batch_row(d, step, metrics, drop_sec=drop_sec, rate=rate)
        batches.append(row)
        tasks += emr_c.task_rows(metrics) if metrics else []
        print(f"[run] {d}: {len(drops)} files in {drop_sec:.1f}s, step {row['step_sec']}s, "
              f"runner {row['runner_sec']}s, ${row['batch_cost_usd']} {row['status']}", flush=True)
        emr_c.put_text(s3, f"{p['metrics']}/batches.csv", emr_c.to_csv(batches, emr_c.BATCH_FIELDS))
        emr_c.put_text(s3, f"{p['metrics']}/task_times.csv", emr_c.to_csv(tasks, emr_c.TASK_FIELDS))
        if err:
            raise SystemExit(f"[run] batch {d} failed: {err}")
    return batches


def cmd_report(a, sess, cid: str, run_id: str) -> dict:
    s3, emr = sess.client("s3"), sess.client("emr")
    counts = emr_c.cluster_counts(emr, cid)
    rep = emr_c.cluster_cost(emr, cid, _prices(a, sess, counts), root_gb=a.root_gb)
    p = emr_c.paths(a.s3_root, a.wh_db, a.sf, run_id)
    emr_c.put_text(s3, f"{p['metrics']}/cluster_cost.json", json.dumps(rep, indent=1, default=str))
    print(f"[report] {cid}: {rep['instance_hours']} instance-hours, ${rep['total_cost']} "
          f"(EC2 {rep['ec2_cost']} + EMR {rep['emr_cost']} + EBS {rep['ebs_cost']}) -> {p['metrics']}")
    return rep


def _job_cluster(a, sess, prices: dict, name: str, steps: list[dict]):
    """Run `steps` on a fresh job cluster; return (result, cost, error)."""
    emr = sess.client("emr")
    spec = emr_c.job_cluster_spec(_spec(a), steps)
    spec["Name"] = name
    emr_c.validate_cluster_spec(spec, a.service_model)
    err = None
    try:
        res = emr_c.run_job_cluster(emr, spec)
    except emr_c.StepFailed as e:
        err, res = e, e.result
    cost = emr_c.cluster_cost(emr, res["cluster_id"], prices, root_gb=a.root_gb)
    return res, cost, err


def cmd_smoke_jobs(a, sess, run_id: str) -> list[dict]:
    """Setup, then each batch, on its own job cluster."""
    s3 = sess.client("s3")
    p = emr_c.paths(a.s3_root, a.wh_db, a.sf, run_id)
    missing = emr_c.staging_missing(s3, p, tpcdi_sql.STAGING_TABLES)
    if missing:
        raise SystemExit(f"Delta staging {p['staging_delta']} lacks {missing}; run the "
                         f"Databricks materialize_staging_emr notebook once for SF={a.sf}")
    print(f"[setup] upload jobs -> {p['code']}: "
          f"{emr_c.upload_code(s3, p['code'], os.path.join(_HERE, 'jobs'))}", flush=True)
    counts = emr_c.instance_counts(_spec(a))
    prices = _prices(a, sess, counts)
    rate = emr_c.fleet_rate(prices, counts, a.root_gb)
    base = f"tpcdi-emr-{p['db']}"
    costs = {}
    # One retry: a transient S3 filesystem error must not fail the run.
    for attempt in (() if a.continue_run else (1, 2)):
        res, cost, err = _job_cluster(a, sess, prices, f"{base}-setup", [
            emr_c.sync_code_step(p["code"]), emr_c.setup_step(p, a.shuffle_partitions)])
        costs[f"setup{'' if attempt == 1 else '_retry'}"] = cost
        t = emr_c.cluster_timing(res["cluster"])
        print(f"[setup] {res['cluster_id']}: boot {t['boot_sec']}s, cluster {t['cluster_sec']}s, "
              f"${cost['total_cost']}{'' if not err else ' FAILED: ' + str(err)[:200]}", flush=True)
        if not err:
            break
    else:
        if not a.continue_run:
            raise SystemExit(f"[setup] failed twice: {err}")
        print(f"[setup] skipped: continuing {p['db']} from batch index {a.start_batch}", flush=True)
    print(f"[run] {counts} at ${rate:.4f}/hr while up; metrics -> {p['metrics']}", flush=True)
    batches, tasks = [], []
    for d in emr_c.batch_dates(a.batches, a.start_batch):
        t0 = time.time()
        drops = emr_c.drop_files(s3, p, d)
        drop_sec = time.time() - t0
        res, cost, err = _job_cluster(a, sess, prices, f"{base}-{d}", [
            emr_c.sync_code_step(p["code"]),
            emr_c.batch_step(p, d, concurrency=a.concurrency,
                             shuffle_partitions=a.shuffle_partitions)])
        costs[d] = cost
        step = res["steps"][-1] if res["steps"] else err.step
        try:
            metrics = emr_c.read_json(s3, emr_c.metrics_uri(p, d))
        except s3.exceptions.NoSuchKey:
            metrics = None  # the runner died before writing its metrics
        row = emr_c.batch_row(d, step, metrics, drop_sec=drop_sec, rate=rate,
                              cluster=res["cluster"], cost=cost)
        batches.append(row)
        tasks += emr_c.task_rows(metrics) if metrics else []
        print(f"[run] {d}: {len(drops)} files in {drop_sec:.1f}s, {row['cluster_id']} boot "
              f"{row['boot_sec']}s, step {row['step_sec']}s, cluster {row['cluster_sec']}s, "
              f"${row['batch_cost_usd']} (step alone ${row['step_cost_usd']}) {row['status']}",
              flush=True)
        emr_c.put_text(s3, f"{p['metrics']}/batches.csv", emr_c.to_csv(batches, emr_c.BATCH_FIELDS))
        emr_c.put_text(s3, f"{p['metrics']}/task_times.csv", emr_c.to_csv(tasks, emr_c.TASK_FIELDS))
        if err:
            break
    total = round(sum(c["total_cost"] for c in costs.values()), 4)
    emr_c.put_text(s3, f"{p['metrics']}/cluster_cost.json",
                   json.dumps({"mode": "job", "clusters": costs, "total_cost": total},
                              indent=1, default=str))
    print(f"[report] {len(costs)} job clusters, ${total} -> {p['metrics']}", flush=True)
    if err:
        raise SystemExit(f"[run] batch {d} failed: {err}")
    return batches


def cmd_cleanup(a, sess):
    s3, glue = sess.client("s3"), sess.client("glue")
    p = emr_c.paths(a.s3_root, a.wh_db, a.sf)
    print(f"[cleanup] glue {p['db']}: {'dropped' if emr_c.drop_glue_database(glue, p['db']) else 'absent'}")
    for k in ("db_location", "dailybatches"):
        print(f"[cleanup] {p[k]}: {emr_c.delete_prefix(s3, p[k])} objects deleted")


def main(argv=None):
    a = _args(argv)
    if a.command == "plan":
        return cmd_plan(a)
    sess = _session(a)
    run_id = a.run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    if a.command == "create":
        print(cmd_create(a, sess))
    elif a.command == "cleanup":
        cmd_cleanup(a, sess)
    elif a.command == "smoke":
        if not a.yes:
            raise SystemExit("smoke creates billable clusters; re-run with --yes")
        if a.cluster_mode == "job":
            cmd_smoke_jobs(a, sess, run_id)
            return
        cid = cmd_create(a, sess)
        try:
            cmd_setup(a, sess, cid)
            cmd_run(a, sess, cid, run_id)
        finally:
            emr_c.terminate(sess.client("emr"), cid)
            print(f"[smoke] terminate requested for {cid}; waiting for TERMINATED", flush=True)
            sess.client("emr").get_waiter("cluster_terminated").wait(ClusterId=cid)
            cmd_report(a, sess, cid, run_id)
    else:
        if not a.cluster_id:
            raise SystemExit(f"{a.command} needs --cluster-id")
        if a.command == "setup":
            cmd_setup(a, sess, a.cluster_id)
        elif a.command == "run":
            cmd_run(a, sess, a.cluster_id, run_id)
        elif a.command == "report":
            cmd_report(a, sess, a.cluster_id, run_id)
        elif a.command == "terminate":
            emr_c.terminate(sess.client("emr"), a.cluster_id)
            print(f"[terminate] requested for {a.cluster_id}")


if __name__ == "__main__":
    main()
