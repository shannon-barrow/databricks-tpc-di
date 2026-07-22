"""Mock backend — lets the whole app run via `databricks apps run-local`
without a workspace or spend.

Records what *would* have happened (workflow creation) and returns plausible
fake job ids, so the form + Create flow can be exercised end-to-end locally.
No credentials pass through the app: only a secret-scope *name* is collected,
so the params are safe to echo back verbatim.
"""
from __future__ import annotations

from models import Engine, EngineSpec


class MockBackend:
    def __init__(self) -> None:
        self.created_jobs: list[dict] = []

    def create_workflow(self, spec: EngineSpec, values: dict) -> dict:
        # Fake but shaped like the real (child_id, parent_id) result.
        n = len(self.created_jobs)
        result = {
            "engine": spec.engine.value,
            "child_id": 100000 + n,
            "parent_id": 200000 + n,
            "params": dict(values),
            "mock": True,
        }
        self.created_jobs.append(result)
        return result

    def list_recent_runs(self, engine: Engine) -> list[dict]:
        return []  # diagnose mode is a later phase
