# Fabric Spark notebook — per-batch DRIVER (runs IN Fabric, triggered by the
# Databricks child's run_fabric task via REST, once per batch date).
#
# Runs the bronze -> silver/gold DAG in ONE Fabric session. Two modes (max_parallel):
#   max_parallel <= 1  : SEQUENTIAL notebook.run in topological order (the proven-green
#                        fallback; runMultiple can't be used — it cancels the session with
#                        streaming at any concurrency).
#   max_parallel  > 1  : THREAD-BASED DAG executor — each activity runs on its own thread,
#                        waits on its dependencies' completion events, then acquires a
#                        semaphore (caps concurrent streams to max_parallel) and calls
#                        notebook.run. This is the EXPERIMENT: does Fabric tolerate N
#                        concurrent Structured Streaming queries in one session when they're
#                        launched via threads+notebook.run (as opposed to runMultiple)?
#
# All child notebooks are UNCHANGED (streaming, awaitTermination); only the driver differs.

# --- PARAMETER CELL (Fabric injects overrides per batch) ---
wh_db           = ""
scale_factor    = "10"
batch_date      = ""       # availableNow streams pick up new files via checkpoint;
                           # FactMarketHistory keys its 52-week lookback off this date
max_parallel    = 8        # >1 = threaded parallel DAG; 1 = sequential fallback
child_timeout   = 7200     # per-child notebook.run timeout (s). FMH at SF=20k runs ~1900s;
                           # the old 1800s cap killed it mid-write (interpGroup shutdown ->
                           # clean SparkContext teardown, exitCode 0, mislabeled a failure).
# -----------------------------------------------------------

import time, threading

if not wh_db:
    raise ValueError("wh_db is required")
max_parallel = int(max_parallel)
child_timeout = int(child_timeout)

# FMH's 52-week window aggregation shuffles ~110 GiB / 3.1B rows at SF=20k. With the
# default 200 shuffle partitions each task is too large to fit a 56 GB executor — the
# local-disk spill overflows, the node is lost, and the lost shuffle blocks cascade into
# FetchFailedException + stage retries. 1000 partitions (~110 MB each) avoids the node loss.
# Session-wide, so it applies to every child notebook launched in this session.
try:
    spark.conf.set("spark.sql.shuffle.partitions", "1000")
except Exception as _e:
    print(f"[warn] could not set shuffle.partitions: {_e}")

# Use the catalog statistics that setup's ANALYZE computed. Without this Fabric flag the
# CBO ignores them and mis-estimates the cloned dims at ~KiB, broadcasting an 8.2 GiB side
# in the silver MERGEs (hard 8 GiB cap -> DimCustomer fails at SF=20000). Session-wide, so
# every child notebook launched here inherits it.
try:
    spark.conf.set("spark.microsoft.delta.stats.injection.catalog.enabled", "true")
except Exception as _e:
    print(f"[warn] could not set stats injection flag: {_e}")

_bronze = lambda tbl: {"table": tbl, "scale_factor": scale_factor, "wh_db": wh_db}
_incr   = {"wh_db": wh_db, "scale_factor": scale_factor, "batch_date": batch_date}

# name -> (Fabric notebook item, args, [dependency names]). Same DAG as the Databricks
# Cluster child job (SF10 942583323456833).
DAG = {
    "bronzecustomer":        ("ingest_bronze", _bronze("customer"),        []),
    "bronzeaccount":         ("ingest_bronze", _bronze("account"),         []),
    "bronzetrade":           ("ingest_bronze", _bronze("trade"),           []),
    "bronzecashtransaction": ("ingest_bronze", _bronze("cashtransaction"), []),
    "bronzeholdings":        ("ingest_bronze", _bronze("holdings"),        []),
    "bronzewatches":         ("ingest_bronze", _bronze("watches"),         []),
    "bronzedailymarket":     ("ingest_bronze", _bronze("dailymarket"),     []),
    "account_updates_from_customer":     ("account_updates_from_customer",      _incr, ["bronzecustomer"]),
    "DimCustomer_Incremental":           ("DimCustomer_Incremental",            _incr, ["bronzecustomer"]),
    "DimAccount_Incremental":            ("DimAccount_Incremental",             _incr, ["bronzeaccount", "account_updates_from_customer", "DimCustomer_Incremental"]),
    "DimTrade_Incremental":              ("DimTrade_Incremental",               _incr, ["DimAccount_Incremental", "bronzetrade"]),
    "currentaccountbalances_Incremental":("currentaccountbalances_Incremental", _incr, ["bronzecashtransaction"]),
    "FactCashBalances_Incremental":      ("FactCashBalances_Incremental",       _incr, ["currentaccountbalances_Incremental", "DimAccount_Incremental"]),
    "FactMarketHistory_Incremental":     ("FactMarketHistory_Incremental",      _incr, ["bronzedailymarket"]),
    "FactHoldings_Incremental":          ("FactHoldings_Incremental",           _incr, ["bronzeholdings", "DimTrade_Incremental"]),
    "FactWatches_Incremental":           ("FactWatches_Incremental",            _incr, ["bronzewatches", "DimCustomer_Incremental"]),
}

# topological order (for the serial fallback + a stable log order)
_TOPO = [
    "bronzecustomer","bronzeaccount","bronzetrade","bronzecashtransaction","bronzeholdings",
    "bronzewatches","bronzedailymarket","account_updates_from_customer","DimCustomer_Incremental",
    "DimAccount_Incremental","DimTrade_Incremental","currentaccountbalances_Incremental",
    "FactCashBalances_Incremental","FactMarketHistory_Incremental","FactHoldings_Incremental",
    "FactWatches_Incremental",
]

results, timings = {}, {}   # name -> exception-repr or None ; name -> seconds
_lock = threading.Lock()

def _run_one(name):
    nb, args, _ = DAG[name]
    s = time.time()
    try:
        notebookutils.notebook.run(nb, child_timeout, args)
        err = None
    except Exception as e:
        # notebook.run wraps the child's real error in a Py4J throwExceptionIfHave;
        # repr(e)[:400] truncated the actual SQL exception. Keep a short repr for the
        # summary line, but write the FULL error (which contains the child SQL error
        # text) to a per-activity OneLake file so failures are diagnosable.
        import traceback
        err = repr(e)[:400]
        try:
            notebookutils.fs.put(f"Files/_diag/error_{name}_{batch_date or 'run'}.txt",
                                 f"{repr(e)}\n\n{traceback.format_exc()}", True)
        except Exception:
            pass
    with _lock:
        results[name] = err
        timings[name] = time.time() - s

t0 = time.time()

if max_parallel <= 1:
    # --- serial fallback (topological) ---
    for name in _TOPO:
        _run_one(name)
        if results.get(name) is not None:
            break
else:
    # --- threaded DAG executor ---
    events = {n: threading.Event() for n in DAG}
    sem = threading.Semaphore(max_parallel)   # cap concurrent notebook.run (concurrent streams)
    def _worker(name):
        _, _, deps = DAG[name]
        for d in deps:
            events[d].wait()
        with _lock:
            dep_failed = any(results.get(d) is not None for d in deps)
        if dep_failed:
            with _lock:
                results[name] = f"skipped (upstream failed: {[d for d in deps if results.get(d) is not None]})"
            events[name].set()
            return
        with sem:
            _run_one(name)
        events[name].set()
    threads = [threading.Thread(target=_worker, args=(n,), name=n) for n in DAG]
    for th in threads: th.start()
    for th in threads: th.join()

# --- report ---
failed = [n for n in _TOPO if results.get(n) is not None and not str(results.get(n)).startswith("skipped")]
lines = []
for n in _TOPO:
    r = results.get(n)
    tag = "ok  " if r is None else ("FAIL" if not str(r).startswith("skipped") else "SKIP")
    lines.append(f"[{tag}] {n} ({timings.get(n,0):.1f}s)" + (f": {r}" if r else ""))
summary = (f"batch {batch_date} mode={'parallel(%d)'%max_parallel if max_parallel>1 else 'serial'} "
           f"finished in {time.time()-t0:.1f}s; {len(failed)} failed\n" + "\n".join(lines))
print(summary)
try:
    notebookutils.fs.put(f"Files/_diag/batch_runner_{batch_date or 'run'}.txt", summary, True)
except Exception as _e:
    print(f"[warn] could not write diag summary: {_e}")

if failed:
    raise RuntimeError(f"batch {batch_date} failed at: {failed}")
notebookutils.notebook.exit(f"batch_ok:{batch_date}")
