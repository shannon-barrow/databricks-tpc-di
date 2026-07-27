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


def _short(err: Exception, limit: int = 140) -> str:
    """Compact a Databricks SDK exception to a single readable line."""
    msg = str(err).strip().splitlines()[0] if str(err).strip() else type(err).__name__
    return msg[:limit]


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

    # --- Existence checks (Databricks-side; SDK only, no egress) -------------
    # Each returns {"exists": bool, "detail": str}. Errors are reported as
    # exists=False with the reason, never raised — validation must not crash
    # the form.

    def check_catalog(self, name: str) -> dict:
        if not name:
            return {"exists": False, "detail": "no catalog given"}
        try:
            self.w.catalogs.get(name)
            return {"exists": True, "detail": f"catalog `{name}` exists"}
        except Exception as e:
            return {"exists": False, "detail": _short(e)}

    def check_schema(self, catalog: str, schema: str) -> dict:
        if not (catalog and schema):
            return {"exists": False, "detail": "catalog + schema required"}
        full = f"{catalog}.{schema}"
        try:
            self.w.schemas.get(full)
            return {"exists": True, "detail": f"schema `{full}` exists"}
        except Exception as e:
            return {"exists": False, "detail": _short(e)}

    def check_warehouse(self, name: str) -> dict:
        if not name:
            return {"exists": False, "detail": "no warehouse name given"}
        try:
            for wh in self.w.warehouses.list():
                if wh.name == name:
                    return {"exists": True, "detail": f"warehouse `{name}` exists"}
            return {"exists": False, "detail": f"no warehouse named `{name}`"}
        except Exception as e:
            return {"exists": False, "detail": _short(e)}

    def check_external_location_for(self, url: str) -> dict:
        """Tell-only: is `url` (s3://… / gs://…) covered by a UC external
        location? We report but never offer to create one."""
        if not url:
            return {"exists": False, "detail": "no volume prefix given"}
        try:
            best = None
            for loc in self.w.external_locations.list():
                lu = (loc.url or "").rstrip("/")
                if lu and url.rstrip("/").startswith(lu):
                    if best is None or len(lu) > len(best):
                        best = lu
            if best:
                return {"exists": True,
                        "detail": f"covered by external location `{best}`"}
            return {"exists": False,
                    "detail": "no UC external location covers this prefix"}
        except Exception as e:
            return {"exists": False, "detail": _short(e)}

    def check_secret(self, path: str) -> dict:
        """Tell-only existence check for a UC secret at catalog.schema.key.
        Never reads or writes the value (consumer-only)."""
        parts = (path or "").split(".")
        if len(parts) < 3:
            return {"exists": False, "detail": "expected catalog.schema.key"}
        catalog, schema, key = parts[0], parts[1], ".".join(parts[2:])
        try:
            # List secrets in the schema and match the key — never fetch value.
            secrets = self.w.secrets.list_secrets(
                scope=f"{catalog}.{schema}")  # UC secret scope form
            for s in secrets:
                if getattr(s, "key", None) == key:
                    return {"exists": True, "detail": f"secret `{path}` exists"}
            return {"exists": False, "detail": f"no secret `{key}` in {catalog}.{schema}"}
        except Exception as e:
            return {"exists": False, "detail": _short(e)}

    # --- Create-if-missing (only the 3 objects we're allowed to create) ------

    def create_catalog(self, name: str) -> dict:
        try:
            self.w.catalogs.create(name=name)
            return {"ok": True, "detail": f"created catalog `{name}`"}
        except Exception as e:
            return {"ok": False, "detail": _short(e)}

    def create_schema(self, catalog: str, schema: str) -> dict:
        try:
            self.w.schemas.create(name=schema, catalog_name=catalog)
            return {"ok": True, "detail": f"created schema `{catalog}.{schema}`"}
        except Exception as e:
            return {"ok": False, "detail": _short(e)}

    def create_warehouse(self, name: str, size: str) -> dict:
        try:
            w = self.w.warehouses.create(name=name, cluster_size=size,
                                         enable_serverless_compute=True,
                                         max_num_clusters=1).result()
            return {"ok": True, "detail": f"created warehouse `{name}` ({w.id})"}
        except Exception as e:
            return {"ok": False, "detail": _short(e)}

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
