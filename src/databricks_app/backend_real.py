"""Real backend — writes secrets into the target Databricks secret scope and
emits benchmark workflows by calling each port's create_jobs.create().

Auth: uses the Databricks SDK's Config()/WorkspaceClient, which auto-detects
the app's service-principal credentials in a deployed Databricks App (and a
CLI profile when run locally with USE_MOCK_BACKEND=false).

v1 is single-workspace: it targets the workspace the app authenticates to.
Cross-workspace competitor runs (e.g. a separate AWS-bench workspace) are a
later phase — the create_jobs `profile` param is the seam for that.
"""
from __future__ import annotations

import importlib.util
import os
import sys
from pathlib import Path

from databricks.sdk import WorkspaceClient

from models import Engine, EngineSpec

def _find_dir(marker: str) -> Path:
    """Walk up from this file to find `marker` (a relative dir path).

    Robust to both layouts: the repo (marker under src/) and a deployed app
    where the whole src/ tree is uploaded (marker under the app root). Returns
    the first ancestor that contains `marker`.
    """
    here = Path(__file__).resolve()
    for base in here.parents:
        cand = base / marker
        if cand.is_dir():
            return cand
    raise RuntimeError(f"could not locate '{marker}' above {here}")


_PORT_DIR = _find_dir("incremental_batches/augmented_incremental")
_TOOLS_DIR = _find_dir("tools")


def _load_create(engine: Engine):
    """Import the port's create_jobs module and return its create() fn.

    Loaded by file path (the ports aren't a package) so the app doesn't
    depend on a particular sys.path layout.
    """
    sub = {Engine.REDSHIFT: "redshift", Engine.BIGQUERY: "bigquery"}[engine]
    path = _PORT_DIR / sub / "create_jobs.py"
    # create_jobs imports from workflow_builders, which lives under tools/.
    sys.path.insert(0, str(_TOOLS_DIR))
    spec = importlib.util.spec_from_file_location(f"{sub}_create_jobs", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.create


class RealBackend:
    def __init__(self) -> None:
        self.w = WorkspaceClient()

    def write_secret(self, scope: str, key: str, value: str) -> None:
        """Ensure the scope exists, then put the secret. The app keeps only
        the scope/key reference afterward — never the value."""
        try:
            self.w.secrets.create_scope(scope=scope)
        except Exception:
            pass  # already exists
        self.w.secrets.put_secret(scope=scope, key=key, string_value=value)

    def create_workflow(self, spec: EngineSpec, values: dict) -> dict:
        """Emit the parent+child workflow for one engine via its create()."""
        if spec.engine in (Engine.REDSHIFT, Engine.BIGQUERY):
            create = _load_create(spec.engine)
            kwargs = self._create_kwargs(spec, values)
            child_id, parent_id = create(**kwargs)
            return {"engine": spec.engine.value, "child_id": child_id,
                    "parent_id": parent_id, "mock": False}
        # Native Databricks + Snowflake wire-ups land in later phases.
        raise NotImplementedError(f"{spec.engine.value} not wired yet")

    def _create_kwargs(self, spec: EngineSpec, values: dict) -> dict:
        """Map form values -> create() keyword args (params only; secrets are
        already in the scope by the time this runs).

        incremental_batches_to_run is excluded: it's a run-now parameter set
        when the parent job is *triggered*, not a build-time create() arg.
        """
        skip = {"incremental_batches_to_run"}
        p = {f.key: values[f.key] for f in spec.param_fields()
             if values.get(f.key) and f.key not in skip}
        p["scale_factor"] = int(p.pop("scale_factor"))
        return p

    def list_recent_runs(self, engine: Engine) -> list[dict]:
        return []  # diagnose mode is a later phase
