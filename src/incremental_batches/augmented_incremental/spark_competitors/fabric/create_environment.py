"""Create (or reuse) an isolated Fabric **Environment** item pinned to Runtime 2.0
(Spark 4.1 / Delta 4.2), then publish it. Attaching this environment to the port's
notebooks overrides the workspace default runtime for JUST our notebooks — it does
NOT change the shared pmt_fabric_ws default (per the isolation requirement).

Flow (all headless via `az` token):
  1. POST /environments                          create the item (idempotent by name)
  2. PATCH /staging/sparkcompute  runtimeVersion  stage the 2.0 runtime
  3. POST  /staging/publish                       publish (async LRO, ~minutes)

Then: `python3 deploy_notebooks.py --environment-id <printed id>` to bind the notebooks.

Usage:  python3 create_environment.py [--name tpcdi_fabric_rt2] [--runtime 2.0]
"""
from __future__ import annotations
import argparse, json, subprocess, time, urllib.request, urllib.error

WS_DEFAULT = "4f119fd7-d1f7-48bc-be77-6c41c782b541"   # pmt_fabric_ws
BASE = "https://api.fabric.microsoft.com/v1"


def _token():
    return subprocess.run(
        ["az", "account", "get-access-token", "--resource",
         "https://api.fabric.microsoft.com", "--query", "accessToken", "-o", "tsv"],
        capture_output=True, text=True, check=True).stdout.strip()


def _req(method, url, tok, body=None):
    data = json.dumps(body).encode() if body is not None else b""
    req = urllib.request.Request(url, data=data, method=method, headers={
        "Authorization": f"Bearer {tok}", "Content-Type": "application/json",
        "Content-Length": str(len(data))})
    try:
        with urllib.request.urlopen(req, timeout=90) as r:
            raw = r.read(); return r.status, (json.loads(raw) if raw else None), dict(r.headers)
    except urllib.error.HTTPError as e:
        raw = e.read()
        try: p = json.loads(raw)
        except Exception: p = {"raw": raw.decode(errors="replace")}
        return e.code, p, dict(e.headers)


def _poll(loc, tok, tries=120, delay=15):
    for _ in range(tries):
        time.sleep(delay)
        s, b, _ = _req("GET", loc, tok)
        st = (b or {}).get("status")
        if st in ("Succeeded", "Completed"):
            return b
        if st in ("Failed", "Cancelled"):
            raise RuntimeError(f"publish LRO {st}: {b}")
    raise TimeoutError("publish LRO poll timeout")


def find_env(tok, ws, name):
    s, b, _ = _req("GET", f"{BASE}/workspaces/{ws}/environments", tok)
    if s != 200:
        raise RuntimeError(f"list environments: {s} {b}")
    for e in b.get("value", []):
        if e.get("displayName") == name:
            return e["id"]
    return None


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--workspace-id", default=WS_DEFAULT)
    ap.add_argument("--name", default="tpcdi_fabric_rt2")
    ap.add_argument("--runtime", default="2.0", help="Spark runtime version (2.0 = Spark 4.1/Delta 4.2)")
    ap.add_argument("--spark-conf", action="append", default=[],
                    help="spark property k=v (repeatable), e.g. spark.native.enabled=true for NEE")
    args = ap.parse_args()
    spark_props = dict(kv.split("=", 1) for kv in args.spark_conf)
    tok = _token(); ws = args.workspace_id

    env_id = find_env(tok, ws, args.name)
    if env_id:
        print(f"[exists] environment {args.name} → {env_id}")
    else:
        s, b, h = _req("POST", f"{BASE}/workspaces/{ws}/environments", tok,
                       {"displayName": args.name,
                        "description": f"Isolated Runtime {args.runtime} env for the TPC-DI Fabric SS port"})
        if s == 202:
            b = _poll(h.get("Location") or h.get("location"), tok, tries=40, delay=3)
        elif s not in (200, 201):
            raise RuntimeError(f"create env: {s} {b}")
        env_id = b["id"]
        print(f"[create] environment {args.name} → {env_id}")

    # Stage Runtime 2.0 (+ any spark props, e.g. NEE acceleration). PATCH is partial.
    patch_body = {"runtimeVersion": args.runtime}
    if spark_props:
        patch_body["sparkProperties"] = spark_props
    s, b, _ = _req("PATCH",
                   f"{BASE}/workspaces/{ws}/environments/{env_id}/staging/sparkcompute?beta=False",
                   tok, patch_body)
    if s not in (200, 202):
        raise RuntimeError(f"stage runtime: {s} {b}")
    print(f"[stage]  runtimeVersion = {args.runtime}" + (f", sparkProperties = {spark_props}" if spark_props else ""))

    # Publish (async).
    s, b, h = _req("POST",
                   f"{BASE}/workspaces/{ws}/environments/{env_id}/staging/publish?beta=False", tok)
    if s == 202:
        print("[publish] triggered (LRO, ~minutes)...")
        loc = h.get("Location") or h.get("location")
        if loc:
            _poll(loc, tok)
        print("[publish] complete")
    elif s in (200, 201):
        print("[publish] accepted")
    else:
        raise RuntimeError(f"publish: {s} {b}")

    print(f"\n[done] environment id = {env_id}")
    print(f"       next: python3 deploy_notebooks.py --environment-id {env_id}")


if __name__ == "__main__":
    main()
