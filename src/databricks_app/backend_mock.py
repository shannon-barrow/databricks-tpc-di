"""Mock backend — lets the whole app run via `databricks apps run-local`
without a workspace, secrets, or spend.

Records what *would* have happened (secret writes, workflow creation) and
returns plausible fake job ids, so the form + Create flow can be exercised
end-to-end locally.
"""
from __future__ import annotations

from models import Engine, EngineSpec


class MockBackend:
    def __init__(self) -> None:
        self.written_secrets: list[tuple[str, str]] = []
        self.created_jobs: list[dict] = []

    def write_secret(self, scope: str, key: str, value: str) -> None:
        # Never store the value — just record that it would be written.
        self.written_secrets.append((scope, key))

    def create_workflow(self, spec: EngineSpec, values: dict) -> dict:
        # Fake but shaped like the real (child_id, parent_id) result.
        n = len(self.created_jobs)
        result = {
            "engine": spec.engine.value,
            "child_id": 100000 + n,
            "parent_id": 200000 + n,
            "params": {k: v for k, v in values.items()
                       if not k.startswith(("rs_", "sf_", "sa_", "dbx_"))},
            "mock": True,
        }
        self.created_jobs.append(result)
        return result

    def list_recent_runs(self, engine: Engine) -> list[dict]:
        return []  # diagnose mode is a later phase
