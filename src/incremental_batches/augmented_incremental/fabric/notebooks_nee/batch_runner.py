# Fabric Spark NEE notebook — per-batch DRIVER (runs IN Fabric, triggered by the
# Databricks child's run_fabric task via REST, once per batch date).
#
# BATCH/NEE variant: runs the bronze -> silver/gold DAG in ONE Fabric session via
# `notebookutils.notebook.runMultiple`. Unlike fabric_ss (where runMultiple cancels the
# session because Structured Streaming + concurrency is incompatible — gotcha #17), the
# NEE variant is pure BATCH, so runMultiple is usable again and gives real parallelism
# (parallel bronze + fan-out facts) on the shared session. See PORT_NOTES.md §7.
# MUST run on the NEE-accelerated environment (tpcdi_fabric_nee, spark.native.enabled=true).
#
# All child notebooks are the notebooks_nee/* batch versions (transform SQL identical to
# fabric_ss; only the driver differs). `path` values below are the deployed Fabric item
# display names — deploy the NEE notebooks under these names (in their own workspace, or
# suffix to coexist with the fabric_ss items).

# --- PARAMETER CELL (Fabric injects overrides per batch) ---
wh_db           = ""
scale_factor    = "10"
batch_date      = ""       # NEE: bronze loads exactly this date's files; FMH keys its
                           #      52-week lookback off it
concurrency     = 8        # runMultiple max parallel activities in the shared session
# -----------------------------------------------------------

import time

if not wh_db:
    raise ValueError("wh_db is required")
concurrency = int(concurrency)

# FMH's 52-week window aggregation shuffles ~110 GiB / 3.1B rows at SF=20k. With the
# default 200 shuffle partitions each task is too large to fit a 56 GB executor — the
# local-disk spill overflows, the node is lost, and the lost shuffle blocks cascade into
# FetchFailedException + stage retries. 1000 partitions (~110 MB each) avoids the node loss.
# Session-wide, so it applies to every child notebook launched in this session.
try:
    spark.conf.set("spark.sql.shuffle.partitions", "1000")
except Exception as _e:
    print(f"[warn] could not set shuffle.partitions: {_e}")

# Enable the Native Execution Engine for this session (Velox/Gluten). Fabric docs: the
# session-level toggle takes effect immediately (no new session) and, since the children
# run via runMultiple in THIS session, they inherit it. This is what makes the variant
# "NEE" — the batch transforms offload to the vectorized C++ path. Verify offload via the
# Spark UI (*Transformer / NativeFileScan / VeloxColumnarToRowExec) or df.explain().
try:
    spark.conf.set("spark.native.enabled", "true")
except Exception as _e:
    print(f"[warn] could not enable NEE: {_e}")

# Use the catalog statistics setup's ANALYZE computed (else the CBO mis-estimates the
# cloned dims and broadcasts an oversized side — same failure class as fabric_ss #18/#20).
try:
    spark.conf.set("spark.microsoft.delta.stats.injection.catalog.enabled", "true")
except Exception as _e:
    print(f"[warn] could not set stats injection flag: {_e}")

_bronze = lambda tbl: {"table": tbl, "scale_factor": scale_factor, "wh_db": wh_db, "batch_date": batch_date}
_incr   = {"wh_db": wh_db, "scale_factor": scale_factor, "batch_date": batch_date}

# name -> (Fabric notebook item path, args, [dependency names]). Same DAG as fabric_ss,
# with ONE batch-specific change: account_updates_from_customer depends ALSO on
# bronzeaccount. In batch mode ingest OVERWRITES bronzeaccount, so the customer-derived
# append must run AFTER that overwrite (streaming didn't need the extra edge).
DAG = {
    "bronzecustomer":        ("ingest_bronze", _bronze("customer"),        []),
    "bronzeaccount":         ("ingest_bronze", _bronze("account"),         []),
    "bronzetrade":           ("ingest_bronze", _bronze("trade"),           []),
    "bronzecashtransaction": ("ingest_bronze", _bronze("cashtransaction"), []),
    "bronzeholdings":        ("ingest_bronze", _bronze("holdings"),        []),
    "bronzewatches":         ("ingest_bronze", _bronze("watches"),         []),
    "bronzedailymarket":     ("ingest_bronze", _bronze("dailymarket"),     []),
    "account_updates_from_customer":     ("account_updates_from_customer",      _incr, ["bronzecustomer", "bronzeaccount"]),
    "DimCustomer_Incremental":           ("DimCustomer_Incremental",            _incr, ["bronzecustomer"]),
    "DimAccount_Incremental":            ("DimAccount_Incremental",             _incr, ["bronzeaccount", "account_updates_from_customer", "DimCustomer_Incremental"]),
    "DimTrade_Incremental":              ("DimTrade_Incremental",               _incr, ["DimAccount_Incremental", "bronzetrade"]),
    "currentaccountbalances_Incremental":("currentaccountbalances_Incremental", _incr, ["bronzecashtransaction"]),
    "FactCashBalances_Incremental":      ("FactCashBalances_Incremental",       _incr, ["currentaccountbalances_Incremental", "DimAccount_Incremental"]),
    "FactMarketHistory_Incremental":     ("FactMarketHistory_Incremental",      _incr, ["bronzedailymarket"]),
    "FactHoldings_Incremental":          ("FactHoldings_Incremental",           _incr, ["bronzeholdings", "DimTrade_Incremental"]),
    "FactWatches_Incremental":           ("FactWatches_Incremental",            _incr, ["bronzewatches", "DimCustomer_Incremental"]),
}

# topological order (stable log order only — runMultiple honors `dependencies` itself)
_TOPO = [
    "bronzecustomer","bronzeaccount","bronzetrade","bronzecashtransaction","bronzeholdings",
    "bronzewatches","bronzedailymarket","account_updates_from_customer","DimCustomer_Incremental",
    "DimAccount_Incremental","DimTrade_Incremental","currentaccountbalances_Incremental",
    "FactCashBalances_Incremental","FactMarketHistory_Incremental","FactHoldings_Incremental",
    "FactWatches_Incremental",
]

# Build the runMultiple DAG spec.
activities = [
    {"name": name, "path": path, "args": args, "dependencies": deps, "timeoutPerCellInSeconds": 7200}
    for name, (path, args, deps) in DAG.items()
]
run_dag = {"activities": activities, "timeoutInSeconds": 43200, "concurrency": concurrency}

t0 = time.time()
# runMultiple returns a dict keyed by activity name; each value exposes the child's exit
# value and any exception. Its exact shape has drifted across Fabric releases, so read it
# defensively (attribute OR dict access) and DON'T assume it raises on child failure.
raw = notebookutils.notebook.runMultiple(run_dag, {"displayDAGViaGraphviz": False})

def _field(v, key):
    if v is None: return None
    if isinstance(v, dict): return v.get(key)
    return getattr(v, key, None)

results = {}   # name -> exception-repr or None
for name in _TOPO:
    v = raw.get(name) if hasattr(raw, "get") else None
    exc = _field(v, "exception")
    status = _field(v, "executionStatus") or _field(v, "status")
    if exc:
        results[name] = str(exc)[:400]
    elif status and str(status).lower() not in ("succeeded", "success", "completed", "none"):
        results[name] = f"status={status}"
    else:
        results[name] = None

# --- report ---
failed = [n for n in _TOPO if results.get(n) is not None]
lines = []
for n in _TOPO:
    r = results.get(n)
    lines.append(f"[{'ok  ' if r is None else 'FAIL'}] {n}" + (f": {r}" if r else ""))
summary = (f"batch {batch_date} mode=runMultiple(concurrency={concurrency}) "
           f"finished in {time.time()-t0:.1f}s; {len(failed)} failed\n" + "\n".join(lines))
print(summary)
try:
    notebookutils.fs.put(f"Files/_diag/batch_runner_nee_{batch_date or 'run'}.txt", summary, True)
except Exception as _e:
    print(f"[warn] could not write diag summary: {_e}")

if failed:
    raise RuntimeError(f"batch {batch_date} failed at: {failed}")
notebookutils.notebook.exit(f"batch_ok:{batch_date}")
