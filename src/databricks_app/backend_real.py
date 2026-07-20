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

# Repo root, relative to this file: src/databricks_app/ -> repo root.
_REPO_ROOT = Path(__file__).resolve().parents[2]
_PORT_DIR = _REPO_ROOT / "src/incremental_batches/augmented_incremental"


def _load_create(engine: Engine):
    """Import the port's create_jobs module and return its create() fn.

    Loaded by file path (the ports aren't a package) so the app doesn't
    depend on sys.path layout beyond the repo root.
    """
    sub = {Engine.REDSHIFT: "redshift", Engine.BIGQUERY: "bigquery"}[engine]
    path = _PORT_DIR / sub / "create_jobs.py"
    # create_jobs adds ../../../tools to sys.path for its builder import.
    sys.path.insert(0, str(_REPO_ROOT / "src/tools"))
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
        already in the scope by the time this runs)."""
        p = {f.key: values[f.key] for f in spec.param_fields()
             if values.get(f.key)}
        p["scale_factor"] = int(p.pop("scale_factor"))
        return p

    def list_recent_runs(self, engine: Engine) -> list[dict]:
        return []  # diagnose mode is a later phase
