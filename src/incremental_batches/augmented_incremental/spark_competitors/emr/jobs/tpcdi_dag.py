"""The per-batch bronze -> silver/gold DAG and a small dependency scheduler.

The DAG is the one the Fabric batch_runner hands to runMultiple, including
the batch-only edge account_updates_from_customer <- bronzeaccount (it appends
to bronzeaccount, so it has to run after that day's ingest). Here the nodes
run as threads sharing one SparkSession, which is what runMultiple does on
Fabric. No Spark import, so the scheduler is unit-testable locally.
"""
from __future__ import annotations

import concurrent.futures
import time
from typing import Callable

BRONZE_SOURCES = ["customer", "account", "trade", "cashtransaction",
                  "holdings", "watches", "dailymarket"]

# node -> dependencies
DAG: dict[str, list[str]] = {
    **{f"bronze{s}": [] for s in BRONZE_SOURCES},
    "account_updates_from_customer": ["bronzecustomer", "bronzeaccount"],
    "dimcustomer":            ["bronzecustomer"],
    "dimaccount":             ["bronzeaccount", "account_updates_from_customer", "dimcustomer"],
    "dimtrade":               ["dimaccount", "bronzetrade"],
    "currentaccountbalances": ["bronzecashtransaction"],
    "factcashbalances":       ["currentaccountbalances", "dimaccount"],
    "factmarkethistory":      ["bronzedailymarket"],
    "factholdings":           ["bronzeholdings", "dimtrade"],
    "factwatches":            ["bronzewatches", "dimcustomer"],
}


def topo_order(dag: dict[str, list[str]]) -> list[str]:
    """Stable topological order; raises on a cycle or an unknown dependency."""
    order, state = [], {}

    def visit(n, path):
        if state.get(n) == "done":
            return
        if state.get(n) == "active":
            raise ValueError(f"cycle in DAG: {' -> '.join(path + [n])}")
        if n not in dag:
            raise ValueError(f"unknown DAG node {n!r} (from {path[-1] if path else '?'})")
        state[n] = "active"
        for d in dag[n]:
            visit(d, path + [n])
        state[n] = "done"
        order.append(n)

    for n in dag:
        visit(n, [])
    return order


def run_dag(dag: dict[str, list[str]], run_node: Callable[[str], None], *,
            concurrency: int = 8, clock: Callable[[], float] = time.time) -> list[dict]:
    """Run every node once its dependencies succeed, up to `concurrency` at a
    time. A failed node skips its descendants (like runMultiple); the others
    keep going. Returns one timing record per node, in topological order."""
    topo_order(dag)  # validate before starting anything
    rec: dict[str, dict] = {n: {"node": n, "status": "PENDING", "start": None,
                                "end": None, "seconds": None, "error": None} for n in dag}
    remaining = dict(dag)
    running: dict[concurrent.futures.Future, str] = {}

    def _timed(n):
        rec[n]["start"] = clock()
        try:
            run_node(n)
        finally:
            rec[n]["end"] = clock()

    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, concurrency)) as ex:
        while remaining or running:
            for n in list(remaining):
                deps = [rec[d]["status"] for d in remaining[n]]
                if any(s in ("FAILED", "SKIPPED") for s in deps):
                    rec[n]["status"] = "SKIPPED"
                    del remaining[n]
                elif all(s == "SUCCESS" for s in deps) and len(running) < max(1, concurrency):
                    rec[n]["status"] = "RUNNING"
                    running[ex.submit(_timed, n)] = n
                    del remaining[n]
            if not running:
                continue
            done, _ = concurrent.futures.wait(running, return_when=concurrent.futures.FIRST_COMPLETED)
            for f in done:
                n = running.pop(f)
                exc = f.exception()
                rec[n]["status"] = "FAILED" if exc else "SUCCESS"
                if exc:
                    rec[n]["error"] = f"{type(exc).__name__}: {str(exc)[:1000]}"
                rec[n]["seconds"] = round(rec[n]["end"] - rec[n]["start"], 3)
    return [rec[n] for n in topo_order(dag)]
