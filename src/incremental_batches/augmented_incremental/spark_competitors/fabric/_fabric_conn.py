"""Fabric REST connection + orchestration helpers.

Runs on the Databricks orchestration side (Python task in the parent/child job).
Mirrors the role of _sf_conn.py / _bq_conn.py / _rs_conn.py in the other ports,
but instead of a JDBC/warehouse connection it wraps the Microsoft Fabric REST API:
mint a token, trigger a Fabric notebook via the Job Scheduler, and poll it.

Auth precedence:
  1. Service principal (client-credentials) from a Databricks secret scope
     `tpcdi_fabric` (keys: tenant_id, client_id, client_secret) — for unattended
     job runs. This is the production path.
  2. `az account get-access-token` — for interactive/dev use on a laptop that has
     already `az login`ed. Fallback only.

The Fabric REST base is https://api.fabric.microsoft.com/v1 and the token
resource/scope is https://api.fabric.microsoft.com/.default.
"""

from __future__ import annotations
import json, subprocess, time
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


def get_token(dbutils=None, secret_scope: str = "tpcdi_fabric") -> str:
    """Return a Fabric bearer token. Prefers the SPN in `secret_scope`
    (when `dbutils` is provided, i.e. running on Databricks); falls back to az."""
    if dbutils is not None:
        try:
            g = lambda k: dbutils.secrets.get(secret_scope, k)
            return _token_via_spn(g("tenant_id"), g("client_id"), g("client_secret"))
        except Exception:
            pass  # scope not set up yet — fall through to az for dev
    return _token_via_az()


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
