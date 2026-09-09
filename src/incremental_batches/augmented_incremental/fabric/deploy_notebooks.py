"""Deploy the Fabric-side notebooks (notebooks/*) as Fabric Notebook items.

Converts each repo `.py` to the Fabric `notebook-content.py` source format and
pushes it via the Items API (create-or-update by display name). Run headless from
a laptop with an `az login` session (Fabric-scoped token); no Databricks needed.

Critical format rule (pinned empirically 2026-09-05, see project memory): the
notebook's FIRST cell must carry the header `# PARAMETERS CELL ********************`
or Fabric will NOT inject the runMultiple/Job-Scheduler `args` — the child would
run on its defaults (wh_db="" → ValueError). Cell 0 (everything through the
`# --- PARAMETER CELL --- ... # -----` block) becomes the parameters cell; the
rest is split on `# COMMAND ----------` into normal `# CELL` blocks.

Display names match the batch_runner DAG's `path` values + the setup/run entry
points, so the deployed items resolve by name.

Usage:
    python3 deploy_notebooks.py                       # deploy all, no env (default runtime)
    python3 deploy_notebooks.py --environment-id <guid>   # attach a Runtime-2.0 Environment item
    python3 deploy_notebooks.py --only setup,batch_runner
"""
from __future__ import annotations

import argparse, base64, json, os, re, subprocess, time
import urllib.request, urllib.error

WS_DEFAULT = "4f119fd7-d1f7-48bc-be77-6c41c782b541"   # pmt_fabric_ws
LH_DEFAULT = "3f5b1c43-a52c-4a2e-90d7-4de7504e6122"   # tpcdi_fabric lakehouse
BASE = "https://api.fabric.microsoft.com/v1"
_HERE = os.path.dirname(os.path.abspath(__file__))
_NB = os.path.join(_HERE, "notebooks")

# display name -> repo file. Names must match batch_runner.py DAG paths + the
# setup_fabric/run_fabric REST triggers.
NOTEBOOKS = {
    "setup":                          f"{_NB}/setup.py",
    "batch_runner":                   f"{_NB}/batch_runner.py",
    "dq_lean":                        f"{_NB}/dq_lean.py",
    "dq_repro":                       f"{_NB}/dq_repro.py",
    "dq_dupcur":                      f"{_NB}/dq_dupcur.py",
    "dq_offload":                     f"{_NB}/dq_offload.py",
    "ingest_bronze":                  f"{_NB}/ingest_bronze.py",
    "account_updates_from_customer":  f"{_NB}/account_updates_from_customer.py",
    "DimCustomer_Incremental":            f"{_NB}/incremental/DimCustomer Incremental.py",
    "DimAccount_Incremental":             f"{_NB}/incremental/DimAccount Incremental.py",
    "DimTrade_Incremental":               f"{_NB}/incremental/DimTrade Incremental.py",
    "currentaccountbalances_Incremental": f"{_NB}/incremental/currentaccountbalances Incremental.py",
    "FactCashBalances_Incremental":       f"{_NB}/incremental/FactCashBalances Incremental.py",
    "FactMarketHistory_Incremental":      f"{_NB}/incremental/FactMarketHistory Incremental.py",
    "FactHoldings_Incremental":           f"{_NB}/incremental/FactHoldings Incremental.py",
    "FactWatches_Incremental":            f"{_NB}/incremental/FactWatches Incremental.py",
}

_DASH_ONLY = re.compile(r"^#\s*-{5,}\s*$")
_COMMAND   = "# COMMAND ----------"


# --------------------------------------------------------------------------- #
# .py  ->  Fabric notebook-content.py
# --------------------------------------------------------------------------- #
def to_fabric_content(src: str, *, lakehouse_id: str, workspace_id: str,
                      environment_id: str | None) -> str:
    """Convert a repo notebook to Fabric notebook-content.py.

    Cell 0 = start of file through the PARAMETER-CELL close line → PARAMETERS CELL.
    Remaining lines split on `# COMMAND ----------` → CELL blocks.
    """
    lines = src.splitlines()
    try:
        open_i = next(i for i, l in enumerate(lines) if "PARAMETER CELL" in l)
    except StopIteration:
        raise ValueError("no '# --- PARAMETER CELL --- ' marker found; cannot locate the parameters cell")
    try:
        close_i = next(i for i in range(open_i + 1, len(lines)) if _DASH_ONLY.match(lines[i]))
    except StopIteration:
        raise ValueError("PARAMETER CELL open marker has no dash-only close line")

    param_cell = "\n".join(lines[: close_i + 1]).strip("\n")

    code_cells, cur = [], []
    for l in lines[close_i + 1:]:
        if l.strip() == _COMMAND:
            code_cells.append("\n".join(cur))
            cur = []
        else:
            cur.append(l)
    code_cells.append("\n".join(cur))
    code_cells = [c.strip("\n") for c in code_cells if c.strip()]

    # metadata (notebook-level)
    deps: dict = {
        "lakehouse": {
            "default_lakehouse": lakehouse_id,
            "default_lakehouse_name": "tpcdi_fabric",
            "default_lakehouse_workspace_id": workspace_id,
            "known_lakehouses": [{"id": lakehouse_id}],
        }
    }
    if environment_id:
        deps["environment"] = {"environmentId": environment_id, "workspaceId": workspace_id}
    meta = {"kernel_info": {"name": "synapse_pyspark"}, "dependencies": deps}
    meta_block = "\n".join("# META " + l for l in json.dumps(meta, indent=2).splitlines())

    out = ["# Fabric notebook source", "", "# METADATA ********************", "", meta_block, ""]
    out += ["# PARAMETERS CELL ********************", "", param_cell, ""]
    for c in code_cells:
        out += ["# CELL ********************", "", c, ""]
    return "\n".join(out).rstrip() + "\n"


# --------------------------------------------------------------------------- #
# REST
# --------------------------------------------------------------------------- #
def _token() -> str:
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


def _poll_op(loc, tok):
    for _ in range(80):
        time.sleep(3)
        s, b, _ = _req("GET", loc, tok)
        st = (b or {}).get("status")
        if st == "Succeeded":
            s2, b2, _ = _req("GET", loc.rstrip("/") + "/result", tok)
            return b2 if b2 else b
        if st == "Failed":
            raise RuntimeError(f"LRO failed: {b}")
    raise TimeoutError("LRO poll timeout")


def _find(tok, ws, name):
    s, b, _ = _req("GET", f"{BASE}/workspaces/{ws}/items?type=Notebook", tok)
    if s != 200:
        raise RuntimeError(f"list items: {s} {b}")
    for it in b.get("value", []):
        if it.get("displayName") == name:
            return it["id"]
    return None


def deploy_one(tok, ws, name, path, *, lakehouse_id, environment_id):
    with open(path) as f:
        src = f.read()
    content = to_fabric_content(src, lakehouse_id=lakehouse_id, workspace_id=ws,
                                environment_id=environment_id)
    payload_part = {"path": "notebook-content.py",
                    "payload": base64.b64encode(content.encode()).decode(),
                    "payloadType": "InlineBase64"}
    item_id = _find(tok, ws, name)
    if item_id:
        # No updateMetadata=true: the kernel/lakehouse/environment dependencies live
        # in notebook-content.py's METADATA block (part of the definition), so we
        # replace just that part. updateMetadata=true would additionally require a
        # .platform part (400 otherwise) and only governs displayName/type.
        s, b, h = _req("POST",
                       f"{BASE}/workspaces/{ws}/items/{item_id}/updateDefinition",
                       tok, {"definition": {"parts": [payload_part]}})
        if s == 202:
            _poll_op(h.get("Location") or h.get("location"), tok)
        elif s not in (200, 201):
            raise RuntimeError(f"update {name}: {s} {b}")
        print(f"[update] {name} → {item_id}")
    else:
        s, b, h = _req("POST", f"{BASE}/workspaces/{ws}/items", tok,
                       {"displayName": name, "type": "Notebook",
                        "definition": {"parts": [payload_part]}})
        if s == 202:
            item_id = _poll_op(h.get("Location") or h.get("location"), tok)["id"]
        elif s in (200, 201) and b:
            item_id = b["id"]
        else:
            raise RuntimeError(f"create {name}: {s} {b}")
        print(f"[create] {name} → {item_id}")
    return item_id


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--workspace-id", default=WS_DEFAULT)
    ap.add_argument("--lakehouse-id", default=LH_DEFAULT)
    ap.add_argument("--environment-id", default=None,
                    help="Runtime-2.0 Environment item GUID to attach (default: none = workspace default runtime)")
    ap.add_argument("--only", default=None, help="comma-separated display names to deploy (default: all)")
    ap.add_argument("--source-dir", default=None,
                    help="deploy from this notebooks dir instead of notebooks/ (e.g. notebooks_nee for the NEE variant)")
    args = ap.parse_args()

    # Remap the NOTEBOOKS file paths to an alternate source dir (e.g. notebooks_nee/) if asked.
    src = dict(NOTEBOOKS)
    if args.source_dir:
        alt = os.path.join(_HERE, args.source_dir)
        src = {name: path.replace(_NB, alt, 1) for name, path in NOTEBOOKS.items()}

    tok = _token()
    names = [n.strip() for n in args.only.split(",")] if args.only else list(src)
    for name in names:
        if name not in src:
            raise SystemExit(f"unknown notebook {name!r}; known: {list(src)}")
        deploy_one(tok, args.workspace_id, name, src[name],
                   lakehouse_id=args.lakehouse_id, environment_id=args.environment_id)
    print(f"\n[done] deployed {len(names)} notebook(s) to workspace {args.workspace_id}")


if __name__ == "__main__":
    main()
