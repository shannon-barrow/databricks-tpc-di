# TPC-DI on Databricks: Performance Story

*From single-threaded Java to distributed Spark — a chronicle of the optimizations that took TPC-DI data generation from hours to under 20 minutes at scale factor 20,000.*

---

## TL;DR

The original DIGen.jar reference implementation generates TPC-DI source data on a single thread, capping throughput at whatever one node can sustain. We rewrote the data generator as distributed PySpark so it scales horizontally. After a series of architectural and runtime optimizations across roughly four weeks, the SF=20,000 full pipeline finishes in **~19 minutes on Databricks Serverless** — a generation tier that DIGen.jar had effectively never produced (multi-hour serial runs that frequently failed). At smaller scale factors (SF=10/100/1000) the new generator completes in seconds to a few minutes.

Headline numbers at SF=20,000 (1.7 TB of CSV across 30+ files, 100M CustomerMgmt actions, 6.5B TradeHistory rows, 6B WatchHistory rows, 10.8B DailyMarket rows, 2.6B Trade rows):

| Phase                              | Pre-V8 wall | Post-V8 wall | Reduction |
| ---------------------------------- | ----------: | -----------: | --------: |
| Full pipeline (Standard, Spark)    |     ~105 m  |     **~19 m** |  **82%**  |
| `gen_customer` (CustomerMgmt+B2/3) |     ~26 m   |     **~12 m** |  **54%**  |
| `copy_trade` (long-pole rename)    |     ~80 m   |    **~2 m**   |  **97%**  |
| `gen_watch_history`                |     ~10 m   |    **~2 m**   |  **80%**  |
|                                    |             |               |           |
| Original DIGen.jar (reference)     |  multiple   |              |           |
|                                    |   hours     |       —       |     —     |

This document tells the story of how we got there: every architectural choice, every dead end, and every "we tested it and the numbers said this was wrong" moment.

---

## Table of contents
1. [The starting point: why DIGen.jar didn't work for the cloud](#1-the-starting-point)
2. [Phase 1 — The Spark port + bijection-based generation](#2-phase-1)
3. [Phase 2 — Workflow decomposition for repair-run granularity](#3-phase-2)
4. [Phase 3 — The Trade family decomposition (V6)](#4-phase-3)
5. [Phase 4 — The rename problem and how we fixed it (V7 → V8)](#5-phase-4)
6. [Phase 5 — The Customer scheduler bottleneck (V8 customer)](#6-phase-5)
7. [Cautionary tales: incidents, dead ends, and reversions](#7-cautionary-tales)
8. [Validation methodology](#8-validation-methodology)
9. [Performance gotchas to watch for](#9-perf-gotchas)
10. [What's next](#10-whats-next)

---

## 1. The starting point: why DIGen.jar didn't work for the cloud {#1-the-starting-point}

The TPC-DI specification ships only as a written spec — TPC does not provide reference code. The de-facto reference is the open-source PDGF/DIGen.jar implementation written by the University of Passau. DIGen is a single-threaded Java application that reads its config from XML and writes flat files (CSV, fixed-width, XML).

**Why it doesn't fit cloud-native execution:**

- **Single-threaded.** Throughput is bounded by one CPU core. At SF=10,000+ data generation takes hours; at SF=20,000 it can take a full day, and frequently fails outright (memory pressure, timeouts).
- **No serverless.** Java subprocesses can't run on Databricks Serverless. DIGen requires a non-serverless DBR cluster with `SINGLE_USER` access mode, locking out the bulk of the platform.
- **No horizontal scaling.** Even on a 100-node cluster, DIGen still uses one core. Adding hardware doesn't help.
- **Filesystem assumptions.** DIGen writes to a local-disk scratch dir (`/local_disk0`), which UC Volumes don't always provide on every cluster type.

We kept DIGen.jar wired in (gated by a `data_generator=digen` widget) for byte-compatible reference output, but the strategic move was clear: rewrite the generator as Spark.

---

## 2. Phase 1 — The Spark port + bijection-based generation {#2-phase-1}

The straightforward translation of "generate N rows" to PySpark is `spark.range(N).withColumn(...)`. That part works. The hard part is what happens between rows when the spec asks for things like:

- "Generate 10,000 INACT actions per update over G updates such that no customer is INACT'd twice."
- "Pair customer × security for WatchHistory such that the (customer, sym) pair is unique within the cross-product."
- "Pick 30% of prospects to match a customer record (the customer table determines this match)."

Naive hash-modulo gives near-uniform random picks but produces ~5–25% duplicates across updates, breaking audit count checks that demand exact equality.

**The fix: bijection-based generation.** For every selection-without-replacement subproblem, we use a bijective permutation `f(x) = (b*x + c) mod m` where `gcd(b, m) == 1`. With `b` coprime to `m`, the function is mathematically guaranteed collision-free over the full domain. Used in:

- **WatchHistory pair selection** — bijective enumeration of the customer × security cross-product.
- **CustomerMgmt scheduler** — per-update bijection assigns INACT / UPDCUST / UPDACCT / CLOSEACCT / ADDACCT victims.
- **Trade history** — historical and incremental Trade row IDs all derive from `hash(t_id, seed) % pool_size` with deterministic seeding.

**Other Phase 1 wins:**

- **Analytical pool sizes.** `cfg.n_available_accounts` is computed from the scaling formulas without scanning the volume. Combined with `cfg.n_brokers_estimate`, Trade can start as soon as FINWIRE's `_symbols` view is ready — no dependency on CustomerMgmt or HR completion. Saved ~30 minutes on the critical path at SF=20k.

- **Auto-broadcast tuning.** Bumped `spark.sql.autoBroadcastJoinThreshold` from 10 MB to 250 MB. ALL manual `F.broadcast()` hints removed — the optimizer picks broadcast vs shuffle based on runtime stats. Earlier manual hints had been forcing 2 GiB Photon hash tables (e.g., the CustomerMgmt inactive-customers anti-join), which OOMed at SF=20k.

- **Parallel range partitioning.** `spark.range(0, cfg.trade_total, numPartitions=max(8, cfg.sf // 8))` for Trade base. Default partitioning at SF=20k put ~108M rows/task, spilling 237 GB inside the range stage. With ~2,500 partitions (1M rows each), zero spill, 7 minutes wall vs 30 minutes before.

- **Symbols `_idx` cast to long.** Trivial-looking change. The `_symbols` join key was originally `int`; downstream Trade/WH/DM joins inserted an implicit `cast(_idx AS bigint)` that prevented auto-broadcast even when the table fit comfortably under threshold. Fixed at the writer.

- **Photon ANSI overflow guard.** WH bijection's `b * pos` can exceed 2^63-1 at SF≥20k. Capped `b` at `2^62 / max_pos` to keep products in range.

---

## 3. Phase 2 — Workflow decomposition for repair-run granularity {#3-phase-2}

The original `data_gen` task in the workflow was monolithic. It ran one notebook that called every per-table generator inside a single ThreadPoolExecutor. If any one failed, the whole 30–90 minute run had to start over, and "repair-run" had nothing to do.

**Decomposition:**

```
data_gen (entry: schema + volume init, wipe when regenerate=YES)
  ├── gen_reference / gen_hr / gen_finwire / gen_prospect (wave 1)
  │   └── copy_hr / copy_finwire / copy_prospect (parallel with downstream)
  ├── gen_customer (← gen_hr) ──→ copy_customer
  ├── gen_daily_market (← gen_finwire) ──→ copy_daily_market
  ├── gen_trade_base (← gen_finwire)             [V6 — see below]
  │   ├── gen_trade            ──→ copy_trade
  │   ├── gen_tradehistory     ──→ copy_tradehistory
  │   ├── gen_cashtransaction  ──→ copy_cashtransaction
  │   └── gen_holdinghistory   ──→ copy_holdinghistory
  └── gen_watch_history (← gen_finwire + gen_customer) ──→ copy_watch_history
audit_emit (ALL_SUCCESS, ← all gens — standard mode only)
cleanup_intermediates (ALL_SUCCESS — drops _gen_* / _dc_* in {wh_db}_{sf}_stage)
```

**What this gave us:**

- **Per-task self-skip.** Each `gen_*` checks if its output Delta is intact. If yes and `regenerate_data=NO`, it exits immediately. A re-trigger only redoes what's missing.
- **Decoupled file copies.** `copy_*` tasks run in parallel with downstream `gen_*` tasks. The gen task sets `_DEFER_COPIES["enabled"]=True` so the inline rename inside the generator is a no-op; the dedicated `copy_*` task does the rename synchronously when its dependencies clear.
- **Per-dataset cluster scoping.** Each task is its own notebook with its own startup, allowing the platform to schedule them on whatever serverless capacity is available without holding everything else hostage.

Cross-task intermediates (`_gen_brokers`, `_gen_symbols`, `_gen_customer_dates`, `_dc_*` from `disk_cache`) live as Delta tables in `{wh_db}_{sf}_stage` (matching `dw_init.sql`'s `_stage` schema convention) with no-stats / no-auto-optimize TBLPROPERTIES — they're temporary.

**Cleanup gating** uses `run_if=ALL_SUCCESS`. If any gen fails, intermediates remain so a repair-run reads them. Cleanup runs only when the DAG goes fully green.

---

## 4. Phase 3 — The Trade family decomposition (V6) {#4-phase-3}

The Trade-family datasets (Trade, TradeHistory, CashTransaction, HoldingHistory) all derive from the same in-memory `trade_df`. The original code built this DataFrame once with ~30 `withColumn` operations + a join against `_symbols` + dictionary lookups for broker names, then ran four `.write.csv()` calls inside one task.

This had three problems:

1. **No repair-run granularity.** A failure in any of the four writes lost all 4 outputs.
2. **Sequential writes within one task.** The four `.csv()` calls ran inside one Python process; a single-threaded driver loop submitted them. They had to share the same ThreadPoolExecutor and contended for executor allocation.
3. **`disk_cache` re-materialization at executor load.** The `trade_df` was 60+ columns wide (including 128-char `_ct_name_raw` strings). At SF=20,000 the staged Parquet was ~300 GB, even though only a tiny fraction was needed cross-table.

**V6 design:**

- `gen_trade_base` task materializes a Delta table `_gen_trade_df` in `{wh_db}_{sf}_stage` containing only the *minimum* cross-task column set (11 columns: `t_id, t_qty, t_ca_id, t_s_symb, _is_canceled, _is_buy, _is_limit, _base_ts, _submit_ts, _complete_ts, _cash_ts`).
- 4 leaves run in parallel after the base lands: `gen_trade`, `gen_tradehistory`, `gen_cashtransaction`, `gen_holdinghistory`. Each reads the Delta and re-derives its single-consumer columns from `t_id` (cheap hash ops).

**What gets re-derived in each leaf:**

- **gen_trade leaf:** `t_st_id` and `t_dts` from cutoff comparison; `t_tt_id`, `t_is_cash`, `t_bid_price`, `t_trade_price`, `t_chrg`, `t_comm`, `t_tax`, `_broker_idx` (then `t_exec_name` via dictionary lookup) — all from `t_id` hashes.
- **gen_cashtransaction leaf:** `_trade_val` (for `ct_amt`), `_ct_name_raw` + `_ct_name_len` (for `ct_name`) — re-derived from `t_id` and `t_qty`. `_ct_name_raw` is 128 chars × 2.4B trades = ~300 GB at SF=20k. By NOT staging it, we save 300 GB of Delta write/read, and re-derive it cheaply in the one leaf that needs it.
- **gen_tradehistory leaf:** The 1, 2, or 3 transition rows per trade, all from staged timestamps + state flags.
- **gen_holdinghistory leaf:** `hh_h_t_id` (uses `t_id` hash for sells), `hh_before_qty` / `hh_after_qty` (just `t_qty` reshaped).

**Inter-leaf state eliminated.** The legacy code had `_hh_hist_batch{N}`, `_ct_hist_batch{N}`, and `_t_submit_hist_batch{N}` temp views shared between writes — each was a filter over `trade_df`. With V6 each leaf computes its own equivalent filter from `_gen_trade_df`. No view sharing means no inter-leaf dependency, just independent reads of the staged Delta.

**Result.** At SF=20,000, the trade-family generation fans out: each leaf takes 5–7 minutes vs the prior monolithic 8 minutes (~no regression because the heavy compute was already parallel internally), but now each LEAF triggers its corresponding `copy_*` task immediately when done — instead of all four copies waiting for the single monolithic gen_trade to finish.

---

## 5. Phase 4 — The rename problem and how we fixed it (V7 → V8) {#5-phase-4}

Spark's CSV writer outputs a directory of part files (`part-NNNNN-UUID.csv`), not a single named file. The benchmark pipeline expects DIGen-style names (`Trade_1.txt`, `Trade_2.txt`, …). Something has to rename the part files into the final layout.

This problem turned into a two-week tour through every storage-API performance edge case on the platform.

### V0: `dbutils.fs.cp` thread pool
Initial implementation: queue the (src, dst) pairs into a Python list, drain with a 64-worker thread pool calling `dbutils.fs.cp`.

**Problem:** UC Volumes' Files API has a per-call overhead (~405 ms/file). At SF=20,000 with ~3,000 trade files, that's ~20 minutes for one dataset. The Files API throttle does not budge with more workers.

**Result:** ~50 minutes for `copy_trade` at SF=20,000.

### V5: Same-directory bash `mv` via shell subprocess
Insight: UC Volumes is a FUSE-mounted filesystem on the cluster nodes. POSIX `mv` between paths on the same FUSE mount bypasses the Files API and goes straight through the FUSE driver to ABFS rename.

Implementation: write Spark output to `Batch1/Trade/part-*`, then a single bash subprocess does `for f in part-*; do mv "$f" "Trade_$i.txt"; done` to rename in place.

**Result:** ~25 minutes for `copy_trade` — half of V0. The FUSE rename path is meaningfully faster than the Files API, but **one bash subprocess = serial rename within the pod**.

### V6 actual measurement: ~660 ms/file
Wait, V0 was 405 ms/file but V5 measured 660 ms/file? Yes — V5 numbers reflect cross-task contention. With the per-dataset task decomposition, all 8 copy tasks run concurrently, contending for ABFS rename throughput at the storage account level. ABFS rename has a soft per-account RPS cap; with 8 pods × 1 worker each = ~8 RPS system-wide, the per-task rate slows to ~660 ms/file.

### V7: Within-pod parallel mv via Python ThreadPoolExecutor (didn't work)
Hypothesis: rather than serial bash mv, fan out 32 Python threads each doing `subprocess.run(["mv", src, dst])`. The subprocess releases the GIL during the syscall, so threads should genuinely parallelize.

**Result: significantly worse than V5.** SF=20,000 copy times went UP to 50–80 minutes. We diagnosed two problems:

1. **UC Volume FUSE serializes per-pod.** The per-pod FUSE driver processes rename syscalls one at a time, regardless of how many client threads call them. The 32 worker threads queue up at the FUSE driver, paying coordination cost for nothing.
2. **EAGAIN amplification.** The 32 in-flight syscalls plus 8 sibling tasks (~256 system-wide) trigger ABFS-side rename throttling. Each throttled mv hits our retry helper, which sleeps in 0.2 → 0.5 → 1.0 → 2.0 → 5.0 second backoffs. With many concurrent retries, average backoff exceeds the base mv time, so net throughput is *lower* than serial.

We canceled the SF=20,000 V7 run after 1h 45m and rolled back.

### V7.5: Drop back to serial mv with retry, keep flat layout
Pure revert to single-threaded mv per pod, but keep the V7 flat-layout naming (`Batch1/Trade_K.txt` instead of `Batch1/Trade/Trade_K.txt`) so the bronze SQL `fileNamePattern` globs match without needing `recursiveFileLookup`. Python retry helper kept for transient failures.

**Result: 105 minutes total at SF=20,000.** Functionally correct, but the seven concurrent copy tasks all running at ~1.5 s/file (cross-dir mv, 8-pod contention) was simply not fast enough.

### V8: Spark-distributed `mv` across executor pods (the right answer)
Insight from re-examining the data: the bottleneck isn't the `mv` syscall itself — it's that **one pod handles all the renames for one task**. UC Volume FUSE serializes within a pod. But each *pod* has its own FUSE driver. So if we distribute renames across N pods, we get N concurrent renames at the backend.

Implementation:

```python
moves_df = spark.createDataFrame([(src, dst), ...], ["src", "dst"])
moves_df = moves_df.repartition(N)         # spread renames across N=32 executor pods
moves_df.foreachPartition(_mv_partition)   # each pod runs subprocess.run(["mv", ...])
                                            # serially over its share of rows
```

Each executor pod runs a serial inner loop (correct — within-pod parallelism doesn't help) but across N=32 pods we get genuine concurrency. The ABFS rename RPS limit is per-account (typically ~5,000 RPS soft cap), so 32 pods × 1 RPS each = 32 system-wide RPS is well under.

**Result at SF=20,000:** `copy_trade` drops from 80 minutes to **~2 minutes**. All other copy tasks similarly collapse:

| Task                  | V7.5 wall | V8 wall | Reduction |
| --------------------- | --------: | ------: | --------: |
| copy_trade            |    79.5 m |   2.2 m |     97%   |
| copy_daily_market     |    81.0 m |   1.2 m |     99%   |
| copy_cashtransaction  |    76.1 m |   1.7 m |     98%   |
| copy_holdinghistory   |    55.9 m |   1.7 m |     97%   |
| copy_watch_history    |    53.1 m |   <1 m  |     >98%  |
| copy_tradehistory     |    44.7 m |   1.3 m |     97%   |
| copy_finwire          |    42.5 m |   1.8 m |     96%   |

**Resilience extras:**

- **Retry-with-backoff per file.** Each pod's mv has a backoff schedule of (0.2, 0.5, 1.0, 2.0, 5.0, 10.0, 20.0, 30.0, 60.0) — ~2 minute total budget — to absorb sustained ABFS throttle bursts.
- **Resume-safe.** A re-run detects existing `{base}_K{ext}` files at the destination and starts the next K past `max(K) + 1`, so a partial-rename failure doesn't lose work or double-rename.
- **Cross-directory move.** Files go from `Batch1/Trade/part-*` to `Batch1/Trade_K.txt`. The empty staging dir is `rmdir`'d after success. Bronze SQL globs match flat layout natively.

### Things we tried that didn't work and why
- **`dbutils.fs.cp`-based parallel copies (V0):** Files API throttle is the wall.
- **Within-pod ThreadPoolExecutor of `mv` (V7):** Per-pod FUSE serialization + EAGAIN cascades make it slower than serial.
- **`mv` via "metadata-only" assumption:** UC Volume `mv` is NOT metadata-only at the storage layer — it issues an ABFS rename RPC each time. Don't assume otherwise.
- **`spark.sparkContext.broadcast()` for shipping rename pairs:** Blocked on serverless (`JVM_ATTRIBUTE_NOT_SUPPORTED`). Use closure capture via `cloudpickle` instead.

---

## 6. Phase 5 — The Customer scheduler bottleneck (V8 customer) {#6-phase-5}

`gen_customer` was the second long-pole task (after the copy tasks). At SF=20,000 it took 22–26 minutes. We profiled it.

### What it was doing on the driver
DIGen's `CustomerMgmtScheduler.java` uses a `GrowingOffsetPermutation` algorithm to pick INACT / CLOSEACCT / UPDCUST / UPDACCT / ADDACCT victims per update such that:

1. Each entity is INACT'd or CLOSED at most once across all updates.
2. The pool at update *g* excludes all earlier deletions.
3. ADDACCT and UPDCUST at update *g* exclude same-update INACTs (a customer being deactivated doesn't get a new account).

Our Python port encoded this as a per-update numpy loop:

```python
for g in range(1, G + 1):
    # bijection over the (alive customers at end of g-1) pool
    virtual = (b * positions + c) % alive_cust_at_g
    # vectorized fixed-point iteration: shift past prior deletions
    actual = _resolve_skip_vec(virtual, sorted_deletions_array)
    # merge new deletions into the cumulative sorted list
    sorted_deletions_array = _merge_sorted(sorted_deletions_array, actual)
    # save (g, action, pos, c_id) tuples to schedule arrays
```

At SF=20,000 with G ≈ 322 generations and ~77M total schedule rows, this single-threaded driver loop took ~22 minutes:

- Per generation: 5 bijections + 5 skip-resolutions + 5 merges
- `_resolve_skip_vec` converged in ~30 iterations on each call
- `_merge_sorted`'s big array grows to ~6M elements over the loop (we'd already optimized away an O(N²) re-sort to an O(N+k) merge — that pass had recovered ~10 minutes earlier)

The structural problem: **all of this work is on one driver thread.** Even though numpy is fast, Spark's executor pool was idle while the driver crunched.

### Insight: DIGen's algorithm is closed-form and parallel-safe
Re-reading the decompiled `pdgf/util/math/GrowingOffsetPermutation.java` (which our Python had been faithfully porting iteratively), we noticed that the JAR's `getPermutation(x, generation)` is **a closed-form mathematical function** — no iterative state needed:

```
getPermutation(x, gen):
    permVal = x
    while True:
        permVal = bij[gen](permVal)        # (permVal*b + c) mod m_g
        if gen == 0:
            return permVal
        gen -= 1
        permVal += offsets[gen]
        if permVal >= max[gen]:
            return permVal + offset_sums[gen]
```

The recursion through generations is bounded by G but typically exits in 1–2 iterations. Critically, **each `(action, generation, slot)` triple is independent**. The whole schedule is a parallel computation.

### V8 customer: Spark-native scheduler

Implementation as a Pandas UDF:

1. Driver-side: pre-compute per-generation bijection params (`b`, `c`, `perm_max`, `offsets`, `offset_sums`) for both customer and account permutations. Cheap (milliseconds), produces ~25 KB of numpy arrays.
2. Build schedule DataFrame in Spark: one row per `(action, gen, slot)` tuple, total = G × del_custs + G × change_custs + G × addaccts_per_update + G × del_accts + G × change_accts (~77M rows at SF=20k).
3. Apply Pandas UDF that vectorizes the `getPermutation` recursion in numpy on each executor partition. The bijection-params dicts ride along via cloudpickle closure capture (avoiding `sparkContext.broadcast()` which is blocked on serverless).
4. Same-update non-collision via DIGen's slot-offset trick: each action type gets a disjoint slot range on the same per-generation bijection, so uniqueness within a slot range plus disjoint ranges guarantees no same-update collision.

**Result:**

- gen_customer wall at SF=20,000: **26 m → 12 m** (54% reduction).
- Schedule build phase specifically: **22 m → 4 m** (the rest of gen_customer is XML write + B2/B3 incrementals).
- All audit counts match the prior-validated baseline within 0.0005% (the only nonzero drift is CA_ADDACCT, off by 142 rows of 30M — well below any tolerance).

---

## 7. Cautionary tales: incidents, dead ends, and reversions {#7-cautionary-tales}

The phase narratives above describe what worked. Almost as instructive — and probably more useful to other teams attempting similar work — are the things we tried, broke, or reverted. Each story below is cross-referenced to a real git commit (or noted when the work stayed on a side branch).

### The DIGen SF=20k data overwrite incident

**What happened.** During V5 trade-only perf-harness testing, the SF=20k DIGen.jar reference output at `/Volumes/main/tpcdi_raw_data/tpcdi_volume/sf=20000/` was wiped and progressively overwritten across a day of perf runs. The user discovered it mid-V5: *"OMG. you have been using the DI_GEN directory and not the spark gen directory. you have written over ALL of the DI_GEN data that was very difficult to create."* DIGen at SF=20k takes 6–10 hours of single-node Java and "fails most of the time," so the lost output was effectively unrecoverable.

**Root cause.** Commit `e62671a` (Unify data_gen entry; delete legacy paths) dropped a per-mode subdirectory selector. The pre-decomposition code had `spark` mode write to `{volume}/spark_datagen/` and `digen` mode write to `{volume}/`. The unified entry path wrote everything into the DIGen directory, so every Spark workflow since had been overwriting DIGen output without anyone noticing — until the SF=20k full Spark run earlier that day with `regenerate_data=YES` wiped it all at the start.

**Compounding bug.** The same V5 retry surfaced a second fatal: `_copy_helper.copy_dataset` ran `_cleanup(staging_dir)` *after* the in-place rename, deleting the staging dir — which now contained the renamed output files. HoldingHistory completed first, hit the cleanup, and silently nuked itself. Discovered post-cancel via `fs.ls` showing zero files where minutes earlier the rename had succeeded.

**Fix.** Restore per-mode subdirs in `data_gen.py` and the four workflow builders (`datagen_spark.py`, `datagen_digen.py`, `augmented_staging.py`, `datagen_trade_only.py`); remove the `_cleanup` call after in-place rename. Landed on the `perf/v5` branch and rolled forward via `00be903` (Port V5 architecture to production).

**Lesson.** When unifying entry points, every `if mode == X` branch in the deleted code is a hidden contract — diff each one byte-for-byte before deleting. And if the operation is destructive (volume wipe), gate it behind a path that includes the mode name so the wrong-mode case fails closed rather than silently overwriting.

### `createDataFrame(pandas)` blew through `spark.rpc.message.maxSize`

**What happened.** SF=20k augmented_incremental was failing with `Serialized task 49039:1 was 495236231 bytes, which exceeds max allowed: spark.rpc.message.maxSize (268435456 bytes)`. The CustomerMgmt schedule was a ~70M-row pandas DataFrame (~2.2 GB of int64) built on the driver. Every executor task's serialized plan was carrying the whole 2.2 GB.

**What was tried first.** Bumped post-`createDataFrame` partition count from `coalesce(8)` to `max(8, cfg.sf // 250)` (~80 partitions). Theory: smaller per-task plan size scales with rows-per-partition. Reran. **Still failed with the same RPC-size error** — *"so I'm still getting this error."*

**Actual fix.** `spark.createDataFrame(pandas)` doesn't ship the data; it embeds it as a literal in the LogicalPlan. coalesce can't help because the data is in the plan, not the input partitions. Bypassed `createDataFrame` entirely: pyarrow `Table.from_pandas` → `pq.write_table` to a UC Volume staging path → `spark.read.parquet` to bring it back as a properly-partitioned DataFrame. (`spark.rpc.message.maxSize` is also not settable on serverless — `AnalysisException: [CANNOT_MODIFY_CONFIG]` — so raising the cap was never an option.) V8's Spark-native customer scheduler later eliminated the pandas detour entirely.

**Commits.** The pyarrow-direct schedule write landed on `augmented_incremental` (~`1e5d0ab`). Made obsolete by `f9cd551` (V8-customer).

**Lesson.** `createDataFrame(pandas_df)` does not magically "ship" the data — it embeds it as a literal in the LogicalPlan, and *every executor task carries the whole plan*. Anything bigger than ~100 MB belongs on disk via pyarrow + `spark.read.parquet`, not in `createDataFrame`. And on serverless, half the RPC/network knobs you'd reach for are locked.

### Manual `F.broadcast()` hint caused 2 GiB Photon hash table OOM masquerading as "session expired"

**What happened.** SF=10k and SF=20k runs were failing in CustomerMgmt with the Spark Connect warning *"Spark Connect Session expired on the server. Please generate a new session by detaching and reattaching the compute…"* after ~39 minutes of work. First hypothesis: a hard ~60-minute serverless session TTL. A half-day refactor was scoped (heartbeat thread, reconnect-on-expiry wrapper, idempotent phase replay).

**What was tried first.** Started building heartbeat + reconnect refactor. Then re-read a notebook HTML postmortem from an earlier similar hang and found `disk_cache` swallowing a `Py4JJavaError` with a broad `except Exception`. Downstream readers re-evaluated the full lineage on every action, hanging the driver in an infinite re-compute loop. Classic stalled-driver pattern, not session TTL.

**Actual fix.** Pulled the executor logs and found the buried real error: `PHOTON_BROADCAST_OUT_OF_MEMORY` in stage 392, the `LeftAnti` build-side hash table for "drop ADDACCT against already-inactive customers." Build side = `inact_days` = 4.34M `(cid, day)` rows at SF=20k, ~200 MB on disk, but Photon's hash table for it ballooned to **2.0 GiB out of 2.1 GiB total task memory** (~10× amplification from sparse-hash buckets, payload overhead, var-len keys). The 200 MB `autoBroadcastJoinThreshold` was sized against the serialized estimate, not the runtime hash table. `F.broadcast(inact_days)` made the planner skip the threshold check entirely. Removed all 19 manual `F.broadcast()` hints across `utils.py`, `trade.py`, `customer.py`, `finwire.py`, `watch_history.py`. SF=10k ran in **26.8 min** (vs 43.4 min on the same code with hints — **38% faster**, 17.8M rec/s vs 10.98M).

**Commits.** Hint-removal landed on `augmented_incremental` (~`3766d1e`), folded into PR #23 to `main`.

**Lesson.** "Session expired" on serverless during long-running work is often not idle TTL — it's the symptom of a driver stuck in an infinite re-compute loop because some `except Exception` swallowed the real error. Find the swallowed exception before refactoring the symptom. And: `F.broadcast()` is a hint, not a guarantee that the build side fits — when the planner has stats, trust them; when you override, you own the OOM.

### CustomerMgmt dedup drift saga: hash-modulo → bijection → GrowingOffsetPermutation

**What happened.** CustomerMgmt was generating duplicate `(C_ID, action_day)` rows because the original code chose update targets via `abs(hash(seed)) % alive_pool_size`. Hash-modulo collisions across millions of updates produced 5–25% drift vs DIGen at SF=5000.

**What was tried first.** A bijection using `(b, c)` coprime parameters per update — `actual_id = (pos * b + c) % alive_pool_size`, with `b` coprime to the modulus. Solved duplicates at SF=10/100/1000 but introduced a new perf regression at SF=5000: the bijection added five new schedule DataFrames (~17M Python tuples / ~1.5 GB driver memory), Spark fell back to shuffle joins on five chained big joins against the 25M-row `all_df`, producing a 42-min gap between staging and the `_closed_accounts` view — *"my bijection refactor added three new schedule DataFrames that got much bigger at SF=5000 than I accounted for."*

**Three layered fixes (incremental).**
1. Replace driver-side `list + bisect` with numpy `searchsorted` (24× memory cut, vectorized C).
2. Arrow-convert numpy → pandas → `spark.createDataFrame` in one batch (instead of row-by-row JVM serialization). Then hit the [createDataFrame LogicalPlan bug](#createdataframepandas-blew-through-sparkrpcmessagemaxsize) above and replaced with pyarrow → parquet → `spark.read`.
3. Stage to parquet via `disk_cache` to break Catalyst plan depth.
4. **Final fix:** V8 `customer_scheduler_v2.py` moved the entire scheduler to Spark via a `GrowingOffsetPermutation` Pandas UDF — gen_customer wall went from 22 min (driver-numpy) to 4 min (executor-side) at SF=20k.

**Commits.** Bijection landed during `augmented_incremental` iteration. The numpy/Arrow refactor went in mid-branch. V8 = `f9cd551`.

**Lesson.** Hash-modulo for "uniqueness" breaks under birthday collisions at scale; only a true bijection guarantees no duplicates. But materializing the bijection's output as Python data on the driver doesn't scale — if the algorithm touches millions of rows, it has to live in Spark from day one. Driver-loop with vectorized numpy is fine up to ~10M rows; beyond that, push the whole thing into a Pandas UDF.

### WatchHistory bijection ARITHMETIC_OVERFLOW at SF≥20k

**What happened.** SF=20k WatchHistory Batch1 write failed on classic Photon with `com.databricks.photon.PhotonException: Error class: ArithmeticOverflowError. Parameters: long overflow`. The expression: `(pos_in_update * bij_b + bij_c) % greatest(1, total_cp)`. At SF=20k: `pos_in_update` up to ~13.8M, `bij_b` up to ~2.25 trillion (modulus = customers × securities cross-product), product ≈ 3×10¹⁹ — past 64-bit signed Long max (9.2×10¹⁸). User: *"Exceeding a BIGINT capacity is absurd."*

**The terrifying corollary.** The same math overflowed at SF=10k too, but SF=10k had run successfully on serverless. Either serverless wraps arithmetic silently (non-ANSI), in which case prior SF=10k WH outputs may carry quietly corrupted `pair_id`s, or Photon's serverless path skipped the overflow check.

**What was tried first.** Considered `cast(... as decimal(38,0))` to avoid overflow inside the multiplication. Works, but pushes a decimal compute onto the hot path. Discarded.

**Actual fix.** Cap `b` so `b × pos_max` stays within Long. The bijection only needs `b > pos_max` and `gcd(b, modulus) = 1` to guarantee a permutation over the actual range; `b` doesn't need to reach the modulus. Cap formula: `cap = (1 << 62) // max(1, max_pos)` — leaves headroom for `+ bij_c`. At SF=20k, `cap ≈ 3.3×10¹¹` — still trillions of valid coprime `b` values, bijection quality unaffected. Validated SF=10k WH data after the fix: distinct `(c_id, sym_id)` count matched expected — no silent corruption present in the prior runs.

**Commits.** WH bij_b cap landed alongside the F.broadcast removal sweep on `augmented_incremental` (~`3766d1e`), folded into PR #23.

**Lesson.** When math touches a "cross-product addressing space" (M × K), the modulus is huge even when the actual data is modest — bijection multipliers need an explicit Long-safety cap derived from `pos_max`, not the modulus. ANSI vs non-ANSI overflow behavior differs by *runtime* (serverless Photon vs classic Photon) — silent wrap on one, hard fail on the other. Audit any cross-product bijection sites whenever the SF crosses into a new order of magnitude.

### UC Volume FUSE EAGAIN ("Resource temporarily unavailable")

**What happened.** SF=10k was hanging in the file-concat phase. `xargs -a list_file cat > dst` aborted on first failure with exit 123. Hundreds of `cat` calls hit EAGAIN ("Resource temporarily unavailable") on the UC Volume during heavy parallel writes. Not a logic bug — flaky FUSE I/O under load. Second job retry often succeeded because FUSE read throttling reset.

**What was tried first.** 5-attempt retry around the `xargs cat` invocation with exponential backoff (1, 2, 4, 8, 16s). Held up at SF=5000 but at SF=10000 the parallel concat pressure was high enough that 5 attempts on a single failed batch wasn't sufficient — `xargs` returns non-zero on the first source's EAGAIN, abandoning every other source in the batch.

**Actual fix.** Replaced `xargs -a list cat > dst` with a Python concat loop: open `dst` once, iterate sources, `shutil.copyfileobj(src_handle, dst_handle, 4 * 1024 * 1024)` (4 MB buffer), with **per-source 10-attempt exponential-backoff retry on `OSError`**. Each EAGAIN now retries just that source, not the whole batch. Survived SF=10k and SF=20k cleanly. Same retry pattern applied to CustomerMgmt XML concat.

**Commits.** Landed during `augmented_incremental` iteration (pre-PR-23). Helper lives in `tpcdi_gen/utils.py`.

**Lesson.** `xargs cat` aborts on first failure — for FUSE-flaky workloads, **retry granularity matters more than retry count**. Per-source-with-backoff beats whole-batch-with-more-attempts. UC Volume FUSE EAGAIN is real and concurrency-driven; design copy paths to retry transparently rather than expecting clean I/O.

### CustomerMgmt XML 13.9 GB skewed-partition file

**What happened.** Early SF=5000 inventory showed WatchHistory at **13.9 GB in a single output file** — far past any reasonable target. CustomerMgmt XML produced 1.86 GB files on some partitions. Trade peaked at 742 MB. None of these are splittable for downstream readers, so a 13.9 GB single part stalls on the slowest task.

**What was tried first.** Default Spark partitioning + naive `coalesce(N)`. Partitions are skewed by upstream join distribution, not row count, so `coalesce` piles the skew into a single output file.

**Actual fix.** `maxRecordsPerFile` per dataset, set from per-dataset `bytes_per_row` × 128 MB target. After the fix at SF=5000: WatchHistory 138.7 MB largest, CashTransaction 137.1 MB largest, Trade 135.7 MB largest. The 13.9 GB file became 678 evenly-sized parts. CustomerMgmt XML required a separate `repartition(111)` because XML body assembly doesn't honor `maxRecordsPerFile`.

**Commits.** Landed during pre-`augmented_incremental` perf work on `perf/v3` and earlier branches; carried forward to PR #23.

**Lesson.** Skewed-input DataFrames produce skewed output files; `coalesce()` and `repartition()` don't fix skew, they redistribute it. **`maxRecordsPerFile` (per-dataset, calibrated to bytes-per-row) is the only knob that bounds output file size against arbitrary partition skew.**

### XML is not splittable: cap files at ~100 MB, not 128 MB

**What happened.** CustomerMgmt XML files were targeting the 128 MB cap used for CSV, on the assumption that downstream `read_files` could split them. They can't — Databricks' native XML reader reads each file as one task. A 128 MB CustomerMgmt.xml part means one Spark task reads the whole thing serially.

**Actual fix.** Special-case XML output to ~100 MB per file. At SF=20k V8: CustomerMgmt.xml = 789 files at ~100 MB each.

**Commits.** Lives in `tpcdi_gen/customer.py` XML-write path; landed during `augmented_incremental` iteration.

**Lesson.** Output file size targets must be set against the downstream reader's splittability semantics, not "what's a sensible chunk size." If the reader is one-task-per-file, your tail latency = your largest file's read time.

### Trade write_file repartition + bin-pack experiment: reverted

**What happened.** Hypothesis: forcing `write_file` to `df.repartition(target_files)` based on a per-(dataset, scale_factor) ratio would produce more uniform output file sizes, letting the small-parts-bin-pack code path go away. Predicted small win on copy phase. Tested at SF=20k.

**Measurements.** Side-by-side at SF=20k (4:23 PM EST baseline vs 7:24 PM EST run with the change):

| Task | Baseline | After repartition | Δ |
|---|---:|---:|---:|
| gen_prospect | 9 m | **38 m** | +29 m |
| gen_trade | 10 m | **32 m** | +22 m |
| copy_watch_history | 9 m | 15 m | +6 m |
| copy_prospect | 5 m | 14 m | +9 m |
| copy_trade | 33.7 m | 27.3 m | −6.4 m |
| **TOTAL** | **54 m** | **66.7 m** | **+12.7 m** |

`df.repartition(N)` is a full hash shuffle. At SF=20k the trade family alone shuffles ~488 GB through executor memory across 4 derived writes. Modest copy-phase savings (−6.4m on copy_trade) were swamped by upstream shuffle cost (+22m on gen_trade alone, +29m on gen_prospect).

**Reverted.** Commit `8b0a82b` (Revert write_file repartition + bin-pack strip).

**What survived.** `f1d4d8a` (customer scheduler: merge instead of full re-sort, drop redundant pass) — a real win unrelated to repartition, kept.

**Commits.** Tried `5b73a71` + `9f61b6b`, reverted as `8b0a82b`. Kept `f1d4d8a`.

**Lesson.** "Cleaner uniform file sizes" is not free — for upstream DataFrames whose lineage avoids shuffles by design, adding `repartition()` for downstream cosmetics can cost orders of magnitude more in shuffle than it saves in copy. **Always measure end-to-end wall, not just the phase you're optimizing.** And on serverless, run-to-run wall variance is large enough that one A/B isn't enough — the same code at 13:12 vs 13:55 EST showed a 2× wall difference *without any code change* on one occasion, leading to one premature revert that was then re-applied.

### CMP/SEC/FIN parallel write split (FINWIRE)

**What happened.** Early in perf work, FINWIRE was a single sequential write loop generating quarterly files for CMP, SEC, and FIN. At SF=10k, `fin_lines` (the largest of the three, billion-row scale) was sitting on 8 default partitions for in-memory compute and text write, leaving most executor cores idle.

**Actual fix.** Split FINWIRE into three parallel write paths. CMP and SEC stayed on default partitioning (small). FIN got an explicit `repartition(fin_target_parts)` (SF-driven) to spread compute and text-file write across the executor pool. The three writes proceed in parallel from the dependency-graph scheduler, since none depends on the others.

**Commits.** FIN repartition landed mid-`augmented_incremental`. CMP/SEC/FIN as separate phases is part of the original Phase 3 design (commit `4a36874` — 7 per-dataset gen notebooks).

**Lesson.** When a single dataset's three sub-formats have different size profiles (CMP small, SEC small, FIN huge), don't write them in one loop — each gets its own appropriate partition count, and the small ones don't block on the big one.

### Concrete numbers from the journey

A few comparison points worth surfacing for context:

- **DIGen.jar SF=10000 baseline:** 3–4 hours single-threaded on one core.
- **Spark V0 SF=10000:** 27 min on 64 cores. ~7-8× wall-clock speedup at the same scale factor.
- **Spark V0 SF=20000:** 53 min on 64 cores — DIGen had effectively never produced this scale reliably.
- **SF=10k pre-broadcast-OOM-fix:** 43.4 min (with `F.broadcast` hints causing 2 GiB hash tables).
- **SF=10k post-broadcast-OOM-fix:** 26.8 min (38% faster, no code change beyond hint removal).
- **SF=20k V0:** 75m21s total, 49m40s in copy phase.
- **SF=20k V7 (parallel-mv attempt):** Cancelled at 1h 45m elapsed with copies still running.
- **SF=20k V7.5 (serial mv with retry):** 1h 45m total (105m). Confirmed bad direction.
- **SF=20k V8 (Spark-distributed mv + Spark-native scheduler):** **19.1m total** (82% reduction vs V7.5).
- **V8-rename per-task wall reductions at SF=20k:**
  - copy_daily_market: 81.0m → 1.2m (99%)
  - copy_cashtransaction: 76.1m → 1.7m (98%)
  - copy_holdinghistory: 55.9m → 1.7m (97%)
  - copy_tradehistory: 44.7m → 1.3m (97%)
  - copy_finwire: 42.5m → 1.8m (96%)
- **V8-customer scheduler at SF=20k:** gen_customer 26.8m → 12.4m (54%); the schedule-build phase specifically went from 22m driver-numpy to 4m executor Pandas UDF.
- **FUSE rename rate (V7.5 measured, not estimated):** ~1.4–1.5s per file, not the 500ms hypothesis. At 4091 trade files = 1h 42m projected for serial copy_trade — explained why V7.5 hit ~80m on copy_trade despite the V8 implementation getting it under 2m.

---

## 8. Validation methodology {#8-validation-methodology}

Performance optimizations that change output are worth nothing if the benchmark still passes audit. Our methodology:

**Bijection invariant.** Every Phase 1 selection-without-replacement subproblem uses a mathematical bijection. The output count is exact by construction, not an empirical match. The audit row-count checks (which demand exact equality) all reduce to "did the bijection cover the right pool?" — a property we can prove rather than measure.

**Static audit snapshots.** We commit pre-computed `*_audit.csv` files at every validated scale factor. The benchmark reads from these snapshots so the audit phase doesn't re-scan ~1.7 TB on every benchmark run. After V8 we regenerated all snapshots from scratch (deleted the prior commits and ran SF=10/100/1000/5000/10000/20000 fresh, sweeping the volume's audit CSVs back into the repo).

**Cross-SF sanity check.** CustomerMgmt counts scale linearly across SF tiers. We confirmed the ×10 progression at every step: SF=10 C_NEW = 15,280 → SF=100 = 152,800 → SF=1000 = 1,528,000 → SF=10000 = 15,280,000 → SF=20000 = 30,560,000. Same for INACT, UPDCUST, ADDACCT, etc. Any drift would show up as a non-clean multiple.

**End-to-end smoke testing.** SF=10 × {Cluster Incremental, DBSQL Single Batch, SDP-CORE} all run green.

**Augmented benchmark validation.** The Augmented Incremental variant (730-day daily streaming) at SF=10 finishes in 7 min on a single serverless cluster (verified via `system.lakeflow.job_task_run_timeline`).

---

## 9. Performance gotchas to watch for {#9-perf-gotchas}

When advising customers building similar pipelines on Databricks, the things we learned the hard way:

**Storage I/O**

- **UC Volume `mv` is NOT a metadata-only operation.** Same-dir or cross-dir, it issues an ABFS rename RPC. Don't assume `mv` is free.
- **UC Volume FUSE serializes within a pod.** Within-pod parallel `mv` doesn't help — it queues at the FUSE driver. Distribute work across multiple pods (Spark `foreachPartition` is the lever).
- **ABFS rename has a per-account RPS soft cap.** ~5,000 RPS is typical; expect throttling above that. With many concurrent jobs against the same storage account, plan for retry-with-backoff.
- **Files API per-call overhead is ~400 ms.** Operations like `dbutils.fs.cp` are bottlenecked by this, not by the underlying storage.
- **EAGAIN handling matters.** Use a generous backoff schedule (~2 min total budget). Naive 5-second retries amplify under heavy concurrency.

**Compute**

- **Cache-and-broadcast is unavailable on serverless.** `spark.persist()`, `spark.cache()`, `StorageLevel`, and `spark.sparkContext.broadcast()` are all blocked. Use Delta-based `disk_cache()` to a temp staging table for cross-action reuse.
- **Closure capture works.** When you would have used a broadcast, just reference the variable from the enclosing scope and let cloudpickle serialize it into the UDF. Up to ~25 KB / closure is fine.
- **Pandas UDF type annotations need to be resolvable at runtime.** Don't combine `from __future__ import annotations` with Pandas UDF definitions — annotations stay as strings, and the runtime introspection fails.
- **Range partitioning matters.** `spark.range(N, numPartitions=K)` with K sized for ~1M rows/partition prevents huge per-task spill at large SFs. `default` partitioning at large workloads will spill 100s of GB.
- **Manual `F.broadcast()` hints are dangerous.** They override the optimizer's runtime-stat-based decision, sometimes forcing 2 GiB Photon hash tables that OOM. Trust the autoBroadcast threshold (raised to 250 MB).

**Workflows**

- **Per-task self-skip is the cheapest "repair-run" mechanism.** Each task checks if its output is intact and exits early if so. No global state needed.
- **Decouple gen and copy tasks.** gen_* writes to a staging dir (cheap); copy_* runs the rename in parallel with downstream gens (fan-out).
- **`run_if=ALL_SUCCESS` on cleanup.** Failed runs leave intermediates so a repair-run consumes them. Cleanup runs only when the DAG is fully green.

**Algorithmic**

- **Bijections beat hash-modulo for selection-without-replacement.** Hash-modulo drifts by 5–25% on dedup-heavy tables; bijections produce exact counts by construction.
- **Closed-form mathematics over iterative state.** When porting from iterative numerical code (e.g., the JAR's GrowingOffsetPermutation that we initially ported as a "maintain a sorted deletions array" loop), look for the closed-form recursion. It's almost always parallel-safe.
- **Stage minimum cross-task data.** When decomposing a stage, stage the minimum columns shared across consumers and re-derive single-consumer columns at the leaf. Saved 300 GB of staged Delta in the trade family alone.

---

## 10. What's next {#10-whats-next}

**Already in flight or planned:**

- **Snowflake port (Phase 3).** Use `snowflake.snowpark_connect` to validate the "PySpark code runs unmodified on Snowflake" claim. Documentation as a competitive blog/video. The bijection-based generation core should port cleanly; the I/O and orchestration layer needs full rewrite.

**Outstanding optimizations we haven't pursued yet:**

- **CustomerMgmt XML concat in Spark.** Currently a Python ThreadPoolExecutor of 8 workers running `shutil.copyfileobj` on the driver — about 4 minutes for 789 files at SF=20,000. Could become V9 (foreachPartition over the file list) but isn't the long pole anymore.
- **gen_customer B2/B3 incremental write.** Each batch is ~50K customers + 100K accounts (small) but takes 2–4 minutes due to executor startup overhead. Could be merged into the main schedule build.
- **Full benchmark validation of V8 outputs against silver/gold.** The audit CSV row counts were validated; running the full Cluster + SDP benchmark end-to-end at SF=20k and confirming 0 audit failures is a separate to-do.

---

## Appendix A — Commit-by-commit timeline

The full V6/V7/V8 work was committed across May 2–4, 2026 on the `data-gen-decomposition` branch. Earlier work landed across April 2026 on `augmented_incremental` and the `perf/v1` through `perf/v6` exploration branches.

| Date       | Commit  | Description                                                          |
| ---------- | ------- | -------------------------------------------------------------------- |
| 2026-05-02 | `62e1e52` | data_gen_tasks foundation (\_shared, init_intermediates, cleanup)    |
| 2026-05-02 | `4a36874` | 7 per-dataset gen notebooks                                          |
| 2026-05-02 | `e62671a` | Unified data_gen entry; deleted legacy paths                         |
| 2026-05-03 | `ecb9ed6` | **V6:** Trade family decomposed into base + 4 leaves                 |
| 2026-05-04 | `23cf017` | V7: 32-thread parallel mv with retry — *worse than serial*           |
| 2026-05-04 | `eaa7a1d` | V7.5: Reverted to serial mv, kept flat layout                        |
| 2026-05-04 | `f9cd551` | **V8 customer:** Spark-native scheduler (Pandas UDF)                 |
| 2026-05-04 | `e178349` | **V8 rename:** foreachPartition mv across executor pods              |
| 2026-05-04 | `c49cd6a` | Deleted stale static_audits before regeneration                      |
| 2026-05-04 | `7f0ff92` | Static audits regenerated at SF=10/100/1000/5000/10000/20000         |
| 2026-05-04 | `7014940` | Cleanup: deleted trade.py + dead helpers (-1,191 LoC)                |
| 2026-05-04 | `880f439` | Refreshed README + CLAUDE.md                                         |

Pull request #24 on `shannon-barrow/databricks-tpc-di` carries the full set.

---

## Appendix B — Useful queries for inspecting future runs

**Identify which serverless cluster a job's tasks ran on** (from `system.lakeflow`):

```sql
SELECT task_key, compute_ids, compute
FROM system.lakeflow.job_task_run_timeline
WHERE job_id = '<your_job_id>' AND job_run_id = '<your_run_id>'
ORDER BY task_key;
```

If `compute_ids` is the same across all rows, all tasks landed on one cluster.

**Per-task wall-time breakdown:**

```sql
SELECT task_key,
       round(execution_duration_seconds / 60, 1) AS minutes
FROM system.lakeflow.job_task_run_timeline
WHERE job_run_id = '<your_run_id>'
ORDER BY execution_duration_seconds DESC;
```
