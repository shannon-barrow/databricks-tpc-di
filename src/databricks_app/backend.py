"""Backend toggle for the TPC-DI benchmark app.

USE_MOCK_BACKEND=true  -> MockBackend (no workspace, no spend; for run-local)
USE_MOCK_BACKEND=false -> RealBackend (writes secrets + emits workflows)

Both implement the same small surface the UI calls, so the Streamlit layer
never branches on which backend is active.
"""
from __future__ import annotations

import os

USE_MOCK = os.getenv("USE_MOCK_BACKEND", "true").lower() == "true"

if USE_MOCK:
    from backend_mock import MockBackend as Backend
else:
    from backend_real import RealBackend as Backend

backend = Backend()
