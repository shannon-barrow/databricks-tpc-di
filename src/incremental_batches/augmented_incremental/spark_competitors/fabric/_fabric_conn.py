"""Fabric REST connection, provisioning + orchestration helpers.

Runs on the Databricks orchestration side (imported by setup_fabric /
run_fabric). Mirrors the role of _sf_conn.py / _bq_conn.py / _rs_conn.py in
the dbt ports, but instead of a warehouse connection it wraps the Microsoft
Fabric REST API: mint a token, provision the Fabric-side compute (Spark pool +
Environment) and notebooks, trigger a notebook via the Job Scheduler, and poll.

Auth:
  1. Service principal (client-credentials). tenant_id / client_id are plain
     values; the client secret is a UC secret passed as its full path
     ("catalog.schema.key"). This is the job path.
  2. `az account get-access-token` — dev fallback for a laptop that has already
     `az login`ed (when no SP inputs are given).

The Fabric REST base is https://api.fabric.microsoft.com/v1 and the token
resource/scope is https://api.fabric.microsoft.com/.default.
"""

from __future__ import annotations
import base64, json, os, re, subprocess, time
import urllib.request, urllib.parse, urllib.error

FABRIC_BASE = "https://api.fabric.microsoft.com/v1"
FABRIC_RESOURCE = "https://api.fabric.microsoft.com"
_TERMINAL = {"Completed", "Failed", "Cancelled", "Deduped"}


# --------------------------------------------------------------------------- #
# Token acquisition
# --------------------------------------------------------------------------- #
def _token_via_spn(tenant_id: str, client_id: str, client_secret: str) -> str:
    """Client-credentials grant against Entra ID for a Fabric-scoped token."""
    url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    body = urllib.parse.urlencode({
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": f"{FABRIC_RESOURCE}/.default",
    }).encode()
    req = urllib.request.Request(url, data=body,
                                 headers={"Content-Type": "application/x-www-form-urlencoded"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.load(r)["access_token"]


def _token_via_az() -> str:
    """Dev fallback: reuse an existing `az login` session."""
    out = subprocess.run(
        ["az", "account", "get-access-token", "--resource", FABRIC_RESOURCE,
         "--query", "accessToken", "-o", "tsv"],
        capture_output=True, text=True, check=True)
    return out.stdout.strip()


def secret_from_path(dbutils, path: str) -> str:
    """Resolve a full UC secret path "catalog.schema.key" to its value."""
    catalog, schema, key = path.split(".", 2)
    return dbutils.secrets.get(catalog=catalog, schema=schema, key=key)


def get_token(*, dbutils=None, tenant_id: str = "", client_id: str = "",
              client_secret_secret: str = "") -> str:
    """Return a Fabric bearer token: the SP when its inputs are given (job
    path), else the local `az login` session (dev path)."""
    if dbutils is not None and tenant_id and client_id and client_secret_secret:
        return _token_via_spn(tenant_id, client_id,
                              secret_from_path(dbutils, client_secret_secret))
    return _token_via_az()


_ONELAKE = "onelake.dfs.fabric.microsoft.com"


def onelake_conf(spark, dbutils, *, tenant_id: str, client_id: str,
                 client_secret_secret: str) -> None:
    """Point the session's ABFS driver at OneLake as the SP.

    Sets both spark.conf (Spark reads/writes) and the JVM Hadoop conf
    (dbutils.fs.* uses that one). Needs classic compute: serverless doesn't
    allow setting fs.azure.* confs.
    """
    conf = {
        f"fs.azure.account.auth.type.{_ONELAKE}": "OAuth",
        f"fs.azure.account.oauth.provider.type.{_ONELAKE}":
            "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        f"fs.azure.account.oauth2.client.id.{_ONELAKE}": client_id,
        f"fs.azure.account.oauth2.client.secret.{_ONELAKE}":
            secret_from_path(dbutils, client_secret_secret),
        f"fs.azure.account.oauth2.client.endpoint.{_ONELAKE}":
            f"https://login.microsoftonline.com/{tenant_id}/oauth2/token",
    }
    hconf = spark._jsc.hadoopConfiguration()
    for k, v in conf.items():
        spark.conf.set(k, v)
        hconf.set(k, v)


# --------------------------------------------------------------------------- #
# Low-level REST
# --------------------------------------------------------------------------- #
def _req(method: str, path: str, token: str, body: dict | None = None):
    """Return (status_code, parsed_json_or_None, response_headers)."""
    url = path if path.startswith("http") else f"{FABRIC_BASE}{path}"
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method, headers={
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    })
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            raw = r.read()
            parsed = json.loads(raw) if raw else None
            return r.status, parsed, dict(r.headers)
    except urllib.error.HTTPError as e:
        raw = e.read()
        try:
            parsed = json.loads(raw)
        except Exception:
            parsed = {"raw": raw.decode(errors="replace")}
        return e.code, parsed, dict(e.headers)


# --------------------------------------------------------------------------- #
# Item lookup + notebook execution (Job Scheduler API)
# --------------------------------------------------------------------------- #
def find_item(token: str, workspace_id: str, display_name: str, item_type: str | None = None):
    """Return the item dict matching display_name (optionally filtered by type), or None."""
    status, body, _ = _req("GET", f"/workspaces/{workspace_id}/items", token)
    if status != 200:
        raise RuntimeError(f"list items failed: {status} {body}")
    for it in body.get("value", []):
        if it.get("displayName") == display_name and (item_type is None or it.get("type") == item_type):
            return it
    return None


def run_notebook(token: str, workspace_id: str, notebook_id: str,
                 parameters: dict | None = None,
                 default_lakehouse_id: str | None = None) -> str:
    """Trigger a Fabric notebook on demand. Returns the job-instance id.

    `parameters` are passed as Fabric parameterization (typed string values here;
    extend with typed values if numeric/bool params are needed).
    """
    payload: dict = {"executionData": {}}
    if parameters:
        payload["executionData"]["parameters"] = {
            k: {"value": str(v), "type": "string"} for k, v in parameters.items()
        }
    if default_lakehouse_id:
        payload["executionData"]["defaultLakehouse"] = {"id": default_lakehouse_id}
    status, body, headers = _req(
        "POST",
        f"/workspaces/{workspace_id}/items/{notebook_id}/jobs/instances?jobType=RunNotebook",
        token, payload)
    if status not in (200, 201, 202):
        raise RuntimeError(f"run_notebook failed: {status} {body}")
    # 202 Accepted returns the instance URL in the Location header.
    loc = headers.get("Location") or headers.get("location")
    if loc:
        return loc.rstrip("/").split("/")[-1]
    if body and body.get("id"):
        return body["id"]
    raise RuntimeError(f"run_notebook: no job-instance id in response ({status} {headers})")


def get_job_instance(token: str, workspace_id: str, item_id: str, instance_id: str) -> dict:
    # ?beta=true so the response includes the notebook's exitValue (validated:
    # it is absent from the default response shape).
    status, body, _ = _req(
        "GET",
        f"/workspaces/{workspace_id}/items/{item_id}/jobs/instances/{instance_id}?beta=true",
        token)
    if status != 200:
        raise RuntimeError(f"get_job_instance failed: {status} {body}")
    return body


def wait_for_job(token: str, workspace_id: str, item_id: str, instance_id: str,
                 poll_secs: int = 15, timeout_secs: int = 7200) -> dict:
    """Poll a job instance to a terminal state. Returns the final instance dict.
    Raises on Failed/Cancelled so the Databricks task fails loudly."""
    deadline = time.time() + timeout_secs
    while True:
        inst = get_job_instance(token, workspace_id, item_id, instance_id)
        state = inst.get("status")
        if state in _TERMINAL:
            if state != "Completed":
                raise RuntimeError(f"Fabric job {instance_id} ended {state}: "
                                   f"{inst.get('failureReason')}")
            return inst
        if time.time() > deadline:
            raise TimeoutError(f"Fabric job {instance_id} still {state} after {timeout_secs}s")
        time.sleep(poll_secs)


def run_and_wait(token: str, workspace_id: str, notebook_id: str,
                 parameters: dict | None = None, default_lakehouse_id: str | None = None,
                 **wait_kw) -> dict:
    """Convenience: trigger a notebook and block until it completes."""
    inst_id = run_notebook(token, workspace_id, notebook_id, parameters, default_lakehouse_id)
    return wait_for_job(token, workspace_id, notebook_id, inst_id, **wait_kw)


# --------------------------------------------------------------------------- #
# Provisioning: capacity check, Spark pool, Environment, notebook deploy
# --------------------------------------------------------------------------- #
def _wait_lro(token: str, headers: dict, *, tries: int = 120, delay: int = 5):
    """Poll a Fabric long-running operation (202 + Location) to completion and
    return its /result body (None when the operation has no result)."""
    loc = headers.get("Location") or headers.get("location")
    if not loc:
        return None
    for _ in range(tries):
        time.sleep(delay)
        status, body, _ = _req("GET", loc, token)
        state = (body or {}).get("status")
        if state in ("Succeeded", "Completed"):
            s2, b2, _ = _req("GET", loc.rstrip("/") + "/result", token)
            return b2 if s2 == 200 else body
        if state in ("Failed", "Cancelled"):
            raise RuntimeError(f"Fabric operation {state}: {body}")
    raise TimeoutError(f"Fabric operation still running after {tries * delay}s: {loc}")


def check_capacity(token: str, workspace_id: str) -> None:
    """Fail fast if the workspace's capacity is paused. A service principal
    can't always list a capacity it doesn't administer; then just warn."""
    s, ws, _ = _req("GET", f"/workspaces/{workspace_id}", token)
    if s != 200:
        raise RuntimeError(f"workspace {workspace_id} not reachable as this principal: {s} {ws}")
    cap_id = ws.get("capacityId")
    s, caps, _ = _req("GET", "/capacities", token)
    cap = next((c for c in (caps or {}).get("value", []) if c.get("id") == cap_id), None)
    if cap is None:
        print(f"[capacity] {cap_id} not visible to this principal — skipping the paused check")
        return
    if cap.get("state") != "Active":
        raise RuntimeError(f"Fabric capacity {cap.get('displayName')} ({cap_id}) is "
                           f"{cap.get('state')}; resume it before running the benchmark")
    print(f"[capacity] {cap.get('displayName')} {cap.get('sku')} is Active")


# Fabric Spark nodes are Memory Optimized only; executors take a whole node.
_NODE_SHAPE = {"Small": (4, "28g"), "Medium": (8, "56g"), "Large": (16, "112g"),
               "XLarge": (32, "224g"), "XXLarge": (64, "400g")}


def ensure_pool(token: str, workspace_id: str, *, name: str, node_size: str,
                node_count: int) -> str:
    """Create or resize a fixed-size custom Spark pool. Fixed size (no
    autoscale, no dynamic allocation) keeps every batch on the same compute,
    the Fabric analog of picking a warehouse size. Returns the pool id."""
    want = {"nodeFamily": "MemoryOptimized", "nodeSize": node_size,
            "autoScale": {"enabled": False, "minNodeCount": node_count,
                          "maxNodeCount": node_count},
            "dynamicExecutorAllocation": {"enabled": False}}
    s, pools, _ = _req("GET", f"/workspaces/{workspace_id}/spark/pools", token)
    if s != 200:
        raise RuntimeError(f"list spark pools: {s} {pools}")
    pool = next((p for p in pools.get("value", []) if p.get("name") == name), None)
    if pool is None:
        s, body, _ = _req("POST", f"/workspaces/{workspace_id}/spark/pools", token,
                          {"name": name, **want})
        if s not in (200, 201):
            raise RuntimeError(f"create spark pool {name}: {s} {body} — creating a "
                               f"pool needs workspace Admin; pre-create it in the "
                               f"workspace Spark settings or grant the SP Admin")
        print(f"[pool] created {name}: {node_count} x {node_size}")
        return body["id"]
    have = {k: pool.get(k) for k in want}
    if have != want:
        s, body, _ = _req("PATCH", f"/workspaces/{workspace_id}/spark/pools/{pool['id']}",
                          token, want)
        if s != 200:
            raise RuntimeError(f"resize spark pool {name}: {s} {body}")
        print(f"[pool] resized {name} to {node_count} x {node_size}")
    else:
        print(f"[pool] {name} already {node_count} x {node_size}")
    return pool["id"]


def ensure_environment(token: str, workspace_id: str, *, name: str, runtime: str,
                       pool_name: str, node_size: str) -> str:
    """Create (or reuse) an Environment item bound to the custom pool at the
    given runtime, publishing only when the published config differs.
    Returns the environment id."""
    cores, mem = _NODE_SHAPE[node_size]
    want = {"instancePool": {"name": pool_name, "type": "Workspace"},
            "driverCores": cores, "driverMemory": mem,
            "executorCores": cores, "executorMemory": mem,
            "dynamicExecutorAllocation": {"enabled": False},
            "runtimeVersion": runtime}
    s, envs, _ = _req("GET", f"/workspaces/{workspace_id}/environments", token)
    if s != 200:
        raise RuntimeError(f"list environments: {s} {envs}")
    env = next((e for e in envs.get("value", []) if e.get("displayName") == name), None)
    if env is None:
        s, body, h = _req("POST", f"/workspaces/{workspace_id}/environments", token,
                          {"displayName": name,
                           "description": f"TPC-DI Fabric Spark/NEE: runtime {runtime} on pool {pool_name}"})
        if s == 202:
            body = _wait_lro(token, h)
        elif s not in (200, 201):
            raise RuntimeError(f"create environment {name}: {s} {body}")
        env_id = body["id"]
        print(f"[env] created {name} → {env_id}")
    else:
        env_id = env["id"]

    s, pub, _ = _req("GET", f"/workspaces/{workspace_id}/environments/{env_id}/sparkcompute", token)
    have = {k: (pub or {}).get(k) for k in want} if s == 200 else {}
    if have.get("instancePool"):
        have["instancePool"] = {k: have["instancePool"].get(k) for k in ("name", "type")}
    if have == want:
        print(f"[env] {name} already published with runtime {runtime} on {pool_name}")
        return env_id

    s, body, _ = _req("PATCH", f"/workspaces/{workspace_id}/environments/{env_id}"
                      f"/staging/sparkcompute?beta=False", token, want)
    if s not in (200, 202):
        raise RuntimeError(f"stage environment compute: {s} {body}")
    s, body, h = _req("POST", f"/workspaces/{workspace_id}/environments/{env_id}"
                      f"/staging/publish?beta=False", token)
    if s == 202:
        print(f"[env] publishing {name} (takes a few minutes)...")
        _wait_lro(token, h, tries=120, delay=15)
    elif s not in (200, 201):
        raise RuntimeError(f"publish environment: {s} {body}")
    print(f"[env] {name} published: runtime {runtime}, pool {pool_name}")
    return env_id


def lakehouse_name(token: str, workspace_id: str, lakehouse_id: str) -> str:
    s, body, _ = _req("GET", f"/workspaces/{workspace_id}/lakehouses/{lakehouse_id}", token)
    if s != 200:
        raise RuntimeError(f"lakehouse {lakehouse_id} not found in {workspace_id}: {s} {body}")
    return body["displayName"]


# Display name -> repo file under notebooks/. Names must match the batch_runner
# DAG `path` values and the setup_fabric / run_fabric triggers.
NOTEBOOKS = {
    "setup":                              "setup.py",
    "batch_runner":                       "batch_runner.py",
    "ingest_bronze":                      "ingest_bronze.py",
    "account_updates_from_customer":      "account_updates_from_customer.py",
    "DimCustomer_Incremental":            "incremental/DimCustomer Incremental.py",
    "DimAccount_Incremental":             "incremental/DimAccount Incremental.py",
    "DimTrade_Incremental":               "incremental/DimTrade Incremental.py",
    "currentaccountbalances_Incremental": "incremental/currentaccountbalances Incremental.py",
    "FactCashBalances_Incremental":       "incremental/FactCashBalances Incremental.py",
    "FactMarketHistory_Incremental":      "incremental/FactMarketHistory Incremental.py",
    "FactHoldings_Incremental":           "incremental/FactHoldings Incremental.py",
    "FactWatches_Incremental":            "incremental/FactWatches Incremental.py",
}

_DASH_ONLY = re.compile(r"^#\s*-{5,}\s*$")
_COMMAND = "# COMMAND ----------"


def to_fabric_content(src: str, *, workspace_id: str, lakehouse_id: str,
                      lakehouse_name: str, environment_id: str | None) -> str:
    """Convert a repo notebook to Fabric's notebook-content.py format.

    Cell 0 (start of file through the PARAMETER CELL close line) becomes the
    PARAMETERS CELL: without that header Fabric does not inject the
    runMultiple / Job Scheduler args and the notebook runs on its defaults.
    The rest splits on `# COMMAND ----------` into normal cells.
    """
    lines = src.splitlines()
    try:
        open_i = next(i for i, l in enumerate(lines) if "PARAMETER CELL" in l)
        close_i = next(i for i in range(open_i + 1, len(lines)) if _DASH_ONLY.match(lines[i]))
    except StopIteration:
        raise ValueError("notebook has no '# --- PARAMETER CELL ---' block closed by a dash-only line")
    param_cell = "\n".join(lines[: close_i + 1]).strip("\n")

    cells, cur = [], []
    for l in lines[close_i + 1:]:
        if l.strip() == _COMMAND:
            cells.append("\n".join(cur)); cur = []
        else:
            cur.append(l)
    cells.append("\n".join(cur))
    cells = [c.strip("\n") for c in cells if c.strip()]

    deps: dict = {"lakehouse": {
        "default_lakehouse": lakehouse_id,
        "default_lakehouse_name": lakehouse_name,
        "default_lakehouse_workspace_id": workspace_id,
        "known_lakehouses": [{"id": lakehouse_id}],
    }}
    if environment_id:
        deps["environment"] = {"environmentId": environment_id, "workspaceId": workspace_id}
    meta = {"kernel_info": {"name": "synapse_pyspark"}, "dependencies": deps}
    meta_block = "\n".join("# META " + l for l in json.dumps(meta, indent=2).splitlines())

    out = ["# Fabric notebook source", "", "# METADATA ********************", "", meta_block, ""]
    out += ["# PARAMETERS CELL ********************", "", param_cell, ""]
    for c in cells:
        out += ["# CELL ********************", "", c, ""]
    return "\n".join(out).rstrip() + "\n"


def deploy_notebooks(token: str, workspace_id: str, src_dir: str, *, lakehouse_id: str,
                     lakehouse_name: str, environment_id: str | None) -> dict:
    """Create-or-update every Fabric notebook from the repo copy, bound to the
    lakehouse + environment. Re-deploying on every setup keeps the Fabric side
    in lockstep with the repo. Returns {display_name: item_id}."""
    s, body, _ = _req("GET", f"/workspaces/{workspace_id}/items?type=Notebook", token)
    if s != 200:
        raise RuntimeError(f"list notebooks: {s} {body}")
    existing = {i["displayName"]: i["id"] for i in body.get("value", [])}
    ids = {}
    for name, rel in NOTEBOOKS.items():
        with open(os.path.join(src_dir, rel)) as f:
            content = to_fabric_content(f.read(), workspace_id=workspace_id,
                                        lakehouse_id=lakehouse_id,
                                        lakehouse_name=lakehouse_name,
                                        environment_id=environment_id)
        part = {"path": "notebook-content.py", "payloadType": "InlineBase64",
                "payload": base64.b64encode(content.encode()).decode()}
        if name in existing:
            # Only the content part: kernel/lakehouse/environment live in its
            # METADATA block (updateMetadata=true would also demand .platform).
            s, b, h = _req("POST", f"/workspaces/{workspace_id}/items/{existing[name]}"
                           f"/updateDefinition", token, {"definition": {"parts": [part]}})
            if s == 202:
                _wait_lro(token, h, delay=3)
            elif s not in (200, 201):
                raise RuntimeError(f"update notebook {name}: {s} {b}")
            ids[name] = existing[name]
        else:
            s, b, h = _req("POST", f"/workspaces/{workspace_id}/items", token,
                           {"displayName": name, "type": "Notebook",
                            "definition": {"parts": [part]}})
            if s == 202:
                b = _wait_lro(token, h, delay=3)
            elif s not in (200, 201):
                raise RuntimeError(f"create notebook {name}: {s} {b} — the SP needs "
                                   f"Contributor (or higher) on the workspace")
            ids[name] = b["id"]
        print(f"[deploy] {name:36s} → {ids[name]}")
    return ids
