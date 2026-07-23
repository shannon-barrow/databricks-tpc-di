"""Real backend — emits benchmark workflows by calling each port's
create_jobs.create(). It never handles credentials: the form passes a Unity
Catalog secret location (catalog + schema) that the job reads at run time.

Auth: workflow creation runs through each port's create(), which authenticates
via its own Databricks CLI profile. A WorkspaceClient is constructed lazily
(only diagnose mode, a later phase, needs it) via the SDK's Config(), which
auto-detects the app's service-principal credentials when deployed.

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

from models import Engine, EngineSpec, derived_from_email

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
        # Lazy: workflow creation goes through each port's create() (which
        # auths via its own CLI profile), so a WorkspaceClient isn't needed
        # for the Create path. Diagnose mode (a later phase) will use it.
        self._w = None

    @property
    def w(self) -> WorkspaceClient:
        if self._w is None:
            self._w = WorkspaceClient()
        return self._w

    def derived_defaults(self) -> dict:
        """Ghost-fill values for DERIVED fields, mirroring the driver notebook.

        The driver takes current_user()'s email local-part, lowercased with
        non-word runs collapsed to spaces (e.g. "shannon barrow"), then builds
        a wh_db prefix "Shannon_Barrow_TPCDI". Returns {} if the user can't be
        resolved so the form just shows empty inputs.
        """
        try:
            email = self.w.current_user.me().user_name or ""
        except Exception:
            return {}
        return derived_from_email(email)

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
        """Map form values -> create() keyword args.

        Spec fields (PARAM / REGION / SECRET_PATH) flow through by key. The
        run-now trigger param (incremental_batches_to_run) is excluded — it's
        set when the parent job is *triggered*, not built. The app-level
        job_name is translated to parent_name (the port derives the child name
        from it).

        raw_schema is collected by the form but NOT passed here: the ports
        currently hardcode the tpcdi_raw_data volume path, so there's no
        create() param for it yet. Wiring a custom raw schema through the ports
        is a follow-up.
        """
        skip = {"incremental_batches_to_run"}
        p = {f.key: values[f.key] for f in spec.fields
             if values.get(f.key) and f.key not in skip}
        p["scale_factor"] = int(p.pop("scale_factor"))
        if values.get("job_name"):
            p["parent_name"] = values["job_name"]
        return p

    def list_recent_runs(self, engine: Engine) -> list[dict]:
        return []  # diagnose mode is a later phase
