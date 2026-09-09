# Augmented TPC-DI → Microsoft Fabric — porting notes

Living design + porting record for the Fabric port of the Augmented Incremental TPC-DI
benchmark. Started 2026-08-27; **`fabric_ss` (structured-streaming) variant validated
end-to-end at SF=10 on 2026-09-05.** Extend this doc as the Native-Execution-Engine
(`fabric_nee`) and warehouse/dbt (`synapse_ss` / dbt) variants get built.

Companion: the private working memory `project-fabric-port` has the blow-by-blow; this
file is the durable, repo-resident version other engineers (and future variants) rely on.

---

## 1. Variant roadmap

| Variant | Engine | Status | Notes |
|---|---|---|---|
| **`fabric_ss`** | Fabric Spark 4.1, JVM, Structured Streaming | ✅ **green E2E @ SF=10** | The one built here. NEE can't stream, so this is JVM. |
| `fabric_nee` | Fabric Spark 4.1 + Native Execution Engine, **batch** | planned | NEE ⊥ streaming → batch re-implementation reusing the transform SQL. See §7. |
| `synapse_ss` / dbt | Fabric Warehouse (T-SQL) via dbt | planned | Warehouse compute; mirrors the cross-CDW dbt ports (SF/RS/BQ). |

Job-name token is parameterized in `create_jobs.py` (`--variant FabricSS|FabricNEE|SynapseSS`).

---

## 2. Architecture decisions that stuck

### Identity model (two identities — settled after much thrash)
- **Databricks compute + UC reads = the user (`shannon`)** on a **SINGLE_USER** UC cluster.
  Never a SP. `single_user_name = shannon.barrow@databricks.com`.
- **OneLake writes + Fabric REST = the SP `tpcdi-fabric-sp`** (client-creds in Databricks
  secret scope `tpcdi_fabric`). Reason: an unattended cluster job has no interactive Entra
  token to reach OneLake/Fabric, so the SP is the durable cross-cloud credential.
- The SP needs **no special tenant grant** — it's a Contributor on `pmt_fabric_ws`, which is
  enough to run notebooks + access OneLake. (Early `UserAccessTokenException` was transient
  new-SP token warm-up, absorbed by the job retry policy — NOT a missing permission. Do not
  go chasing the "Service principals can use Fabric APIs" tenant setting.)

### Data bridge = DEEP CLONE → OneLake (shortcuts were a dead end)
The original plan (OneLake shortcuts to ADLS) **failed**: the Databricks staging tables +
raw volume are **UC-managed** in an opaque storage account Fabric's workspace identity has no
RBAC to, so Fabric can't read them in place; mirrored-catalog/UC-federation needs
metastore-admin + external-data-access grants we chose not to chase. Converged path:
- **Materialize (once per SF):** distributed Delta **DEEP CLONE** `main.tpcdi_incremental_staging_{sf}`
  → OneLake `Tables/staging_sf{sf}/` (Databricks side, `setup_fabric`). No pandas — a native
  `CREATE OR REPLACE TABLE delta.\`abfss://…onelake…/Tables/staging_sf{sf}/{t}\` DEEP CLONE …`.
- **Per-run reset:** **SHALLOW CLONE** within OneLake (`notebooks/setup.py`, Fabric side):
  `CREATE OR REPLACE TABLE {wh_db}_{sf}.{t} SHALLOW CLONE staging_sf{sf}.{t}` (zero-copy).
- **Raw files:** `simulate_filedrops_fabric` copies each batch's file into OneLake `Files/…`
  via `dbutils.fs.cp` (OneLake OAuth on the cluster). Fabric reads them natively.

Everything lands in the **`tpcdi_fabric` lakehouse** OneLake — the only store readable by both
sides with obtainable creds (Fabric native read; Databricks writes as the SP).

### Orchestration = Databricks Workflows drive Fabric via REST (same parent/child shape as other ports)
```
Parent (…-FabricSS-Parent, Databricks job 61571649434078):
  setup_fabric  (materialize DEEP CLONE → OneLake [idempotent]; trigger Fabric-side setup; emit batch dates)
    └─ for_each(batch_date_ls)  [SEQUENTIAL — streaming state builds batch to batch]
         └─ Child (…-FabricSS-Child, job 769329688244453):
              simulate_filedrops_fabric  (drop the day's files into OneLake Files)
              run_fabric                 (REST-trigger the Fabric batch_runner notebook; poll to done)
  delete_when_finished_TRUE_FALSE → cleanup (teardown_fabric)  [gated, default FALSE during bring-up]
```
`run_fabric` triggers ONE Fabric `batch_runner` notebook per batch (Job Scheduler REST,
`?jobType=RunNotebook`, SP token) and blocks. `batch_runner` runs the 16-step bronze→silver/gold
DAG **in one Fabric session**.

### Runtime = isolated Runtime-2.0 Environment item
Fabric workspace default is Runtime 1.3; we need **2.0 (Spark 4.1 / Delta 4.2)**. Created an
**isolated Environment item `tpcdi_fabric_rt2`** (does NOT change the shared workspace default)
and bind every notebook to it via `notebook-content.py` METADATA `dependencies.environment`.

### Deploy toolchain (all headless via `az` token + Fabric REST)
`deploy_notebooks.py` converts each repo `.py` → Fabric `notebook-content.py` (see §4 gotcha
on the PARAMETERS CELL marker) and create-or-updates it as a Notebook item. `create_environment.py`
stands up the 2.0 env. `create_jobs.py` builds the Databricks parent/child via the raw Jobs REST API.

---

## 3. Component / file map

```
augmented_incremental/fabric/
  PORT_NOTES.md                 (this file)
  _fabric_conn.py               Databricks-side: token (SP from secret scope, else az) + Job Scheduler run/poll
  setup_fabric.py               PARENT task (Databricks): idempotent DEEP-CLONE materialize → OneLake,
                                trigger Fabric setup, emit batch_date_ls
  simulate_filedrops_fabric.py  CHILD task (Databricks): copy the day's staged files → OneLake Files
  run_fabric.py                 CHILD task (Databricks): REST-trigger batch_runner, block
  create_environment.py         one-time: create + publish the Runtime-2.0 env item
  deploy_notebooks.py           push notebooks/* as Fabric items (.py → notebook-content.py)
  create_jobs.py                create the Databricks parent/child jobs (raw Jobs REST)
  notebooks/                    CODE THAT RUNS IN FABRIC:
    setup.py                    schema reset + SHALLOW CLONE 20 tables + 6 bronze creates + checkpoint reset
    batch_runner.py             per-batch DRIVER: runs the 16-step DAG via SEQUENTIAL notebook.run
    ingest_bronze.py            bronze ingest (native file stream, param table=…)
    account_updates_from_customer.py   bronze-layer transform (customer→account rows)
    incremental/*.py            8 silver/gold streaming transforms (readStream.table + foreachBatch)
```
Notebooks that run **in Databricks** (setup_fabric, run_fabric, simulate_filedrops_fabric) carry a
`# Databricks notebook source` header → deploy as workspace NOTEBOOKS. `_fabric_conn.py` +
everything under `notebooks/` are workspace FILES / Fabric items respectively.

---

## 4. Fabric-2.0 porting gotchas catalog (the reusable payoff)

Each is symptom → cause → fix. Most recur in `fabric_nee`; the streaming-specific ones (★S) do not.

**Access / deploy / runtime**
1. **Fabric can't read UC-managed ADLS** → shortcut/federation dead end → **DEEP CLONE to OneLake** (§2).
2. **OneLake ABFS path uses BARE GUIDs, no `.lakehouse` suffix** — the friendly-name form 400s
   `FriendlyNameSupportDisabled`. Path: `abfss://{ws_guid}@onelake.dfs.fabric.microsoft.com/{lh_guid}/Tables|Files/…`.
3. **UC reads (`main.…`) + `fs.azure.*` confs + `spark._jsc` require a SINGLE_USER UC cluster.**
   Shared/USER_ISOLATION clusters block JVM access and credential confs → materialize/filedrops fail there.
4. **Parameter injection requires the first cell header be `# PARAMETERS CELL ********************`** (not
   `# CELL`). Verified: a plain first cell does NOT receive `runMultiple`/Job-Scheduler `args` (child runs
   on defaults → `wh_db="" → ValueError`). `deploy_notebooks.to_fabric_content` emits cell 0 as PARAMETERS CELL.
5. **`updateDefinition` must NOT pass `updateMetadata=true`** unless you also send a `.platform` part (else
   400 "UpdateMetadata is true but .platform not provided"). Kernel/lakehouse/env deps live in
   notebook-content.py METADATA anyway, so plain `updateDefinition` suffices.
6. **Runtime 2.0 = an isolated Environment item**, bound per-notebook via METADATA `dependencies.environment`
   (env `tpcdi_fabric_rt2`). Don't flip the shared workspace default.
7. **Job Scheduler instance response has NO exitValue** (even `?beta=true`): keys are id/itemId/jobType/
   invokeType/status/failureReason/rootActivityId/start|endTimeUtc. Read a child's exit in-session via
   `runMultiple`'s return dict, or write results to OneLake and read via the DFS API.
8. **A failing streaming statement HARD-CANCELS the whole Fabric session** (`System_Cancelled_Session_Statements_Failed`),
   killing the driver notebook before any error-handling/diag-write runs. So the driver can't self-report.
   **Diagnosis pattern that works:** run the suspect logic in a standalone notebook wrapped in try/except,
   write progress + full traceback to OneLake `Files/_diag/*.txt`, read back via the DFS API (storage token).

23. **Fabric Spark pools: Memory Optimized is the ONLY node family** (unlike Databricks, you don't pick a VM
    type — only node *size* and *count* are configurable; there is no compute/storage-optimized option). Sizing
    math for the anchor: **F64 = 64 CU = 128 base Spark vCores** (up to 384 with 3× burst); a **Medium node =
    8 vCore / 64 GB = 4 CU**, so a full F64's base allotment = **16 Medium nodes** — which is exactly the fixed-size
    custom pool `tpcdi_f64_full`. Separately, on the **Databricks** orchestration side, the **x86 `D8ds_v6`
    interactive cluster hit repeated Azure VM-launch timeouts**; switching to the **ARM `D8pds_v6`** SKU launched
    in ~4 min. `create_jobs.py` also sets **`max_retries=0`** on parent + child jobs (a failed batch is a real
    failure, not transient — retrying just re-ran the doomed ~30-min batch, cf. #20).

**SQL dialect (Spark 4.1 vs Databricks) — recurs in fabric_nee**
9. **`ANALYZE TABLE … COMPUTE STATISTICS FOR ALL COLUMNS` IS supported — the earlier "unsupported" claim was
   WRONG** (my mistake: the AnalysisException was a `TABLE_OR_VIEW_NOT_FOUND` from passing a **2-part
   `schema.table` name**, which ANALYZE won't resolve, not from the syntax). **Fix: `USE {schema}` first, then
   ANALYZE the UNqualified table name.** (SELECT/CLONE/SHOW all tolerate the 2-part name; ANALYZE is the
   exception.) Two more requirements to make the stats actually *usable* by the CBO: set
   **`spark.microsoft.delta.stats.injection.catalog.enabled=true`** (else Fabric ignores catalog stats), and
   **compute them once per run, on the SHALLOW CLONEs in `setup`** (a shallow clone carries NO stats from its
   source). This **restores exact Databricks parity** — the Databricks `../setup.py` `clone_table` does the same
   `CREATE…CLONE` + `ANALYZE … FOR ALL COLUMNS` (via a ThreadPoolExecutor), and I wrongly removed it here on the
   bad "unsupported" read. See gotcha **#21** for the failure this fixes and the large perf win.
10. **Liquid clustering needs stats on the cluster column** → `bronzecustomer` (CLUSTER BY `update_dt`, its
    34th col) fails `DELTA_CLUSTERING_COLUMN_MISSING_STATS` past the default 32-col stats window. →
    `TBLPROPERTIES ('delta.dataSkippingNumIndexedCols'='34')` on the bronze creates (same as Databricks).
11. **`QUALIFY` is unsupported** (`PARSE_SYNTAX_ERROR at or near 'QUALIFY'`). → rewrite to
    `SELECT * except(…, _rn) FROM (SELECT *, row_number() over(…) _rn FROM …) WHERE _rn=1`.
    (`SELECT * except(col)` IS supported on 4.1.)
12. **`INSERT INTO … REPLACE USING (keys)` is Databricks-only.** → build the batch rows into a temp view,
    then `DELETE FROM tgt WHERE <key> IN (<literals>)` + `INSERT INTO tgt SELECT * FROM view`.
13. **DELETE can't take a subquery** (`DELTA_UNSUPPORTED_SUBQUERY`). → collect the distinct key values in
    Python and DELETE with a **literal IN-list** (tiny per batch). (Follows from #12.)
14. `INSERT OVERWRITE {tgt}` (full-table, currentaccountbalances) — **standard Spark, works fine on Fabric.**
18. **A forced `/*+ BROADCAST(t) */` hint on a large table blows the driver at scale** (SF=20000).
    FactMarketHistory carried `/*+ BROADCAST(f) */` on `companyyeareps` (**~950M rows** at SF=20k). Databricks/
    Photon AQE silently *demotes* an over-threshold broadcast hint; **Fabric Spark 4.1 honors it literally** →
    the BroadcastExchange collects the 950M-row build side to the driver → `Spark_User_Driver_MaxResultSizeExceeded`
    ("Tasks result size has exceeded"), killing FMH at ~626s (all 15 other activities green). → **remove the
    forced hint**, let AQE pick the join against `autoBroadcastJoinThreshold`. Matches CLAUDE.md's "removed ALL
    manual broadcast hints — they caused Photon OOMs at SF=20000." (Same audit applies to any ported BROADCAST hint.)
19. **FMH's 52-week window aggregation loses a node to shuffle spill at SF=20000** (the bottleneck AFTER #18).
    `sym_min_max` (group by dm_s_symb, min_by/max_by) shuffles **~110 GiB / 3.1B rows every batch** (a full
    trailing year of bronzedailymarket). On the Starter Pool (max 9 × 8c/56 GB) with the **default 200 shuffle
    partitions** each task is ~550 MB — too big to fit a 56 GB executor alongside the struct-agg state → local-disk
    spill (~300 GiB mem+disk) overflows, the node is lost, its shuffle blocks vanish → **`FetchFailedException`**.
    82 of the 200 tasks failed; the stage retried, more failed on retry, and stage 939 hit its 4-attempt limit →
    job abort (diag: `Spark_System_Executor_ExitCodeMinus100LostNode`; Fabric also flagged data skew). **NOT a node-count problem** — 9×56 GB is more RAM than the DBSQL Small (4 Photon workers) that
    runs this fine; the gap is Photon (lean, columnar) vs `fabric_ss`'s plain JVM Spark (streaming ⊥ NEE). Fix
    applied: **`spark.conf.set("spark.sql.shuffle.partitions","1000")`** in `batch_runner` (session-wide →
    ~113 MB/partition, well under the executor). Further levers if it recurs: Fabric's **Remote Shuffle Manager /
    Efficient Scaledown** (routes large shuffles to Azure Blob, decoupling shuffle from executor lifetime —
    targets exactly this lost-node cascade), pin `advisoryPartitionSizeInBytes` if AQE re-coalesces, + AQE skew
    handling. The `fabric_nee` batch variant (vectorized Velox/Gluten) should behave much more like Photon here.

20. **`batch_runner`'s per-child `notebook.run` timeout was too short for FMH at SF=20000** (the bottleneck
    AFTER #19). `_run_one` called `notebookutils.notebook.run(nb, 1800, args)` — a 30-min per-notebook cap. Once
    #18+#19 let FMH's aggregation succeed, FMH ran the full **~1887s (>1800s)** and tripped the cap. The stderr
    is unambiguous it was NOT a resource failure: `Final app status: SUCCEEDED, exitCode: 0`, `numExecutorsFailed=0`,
    all executors alive, no OOM/FetchFailed anywhere; the tail shows `WARN notebookUtils: Shutdown interpGroup
    timed out` → `SparkContext is stopping with exitCode 0 from stop at SparkEntries.java:141` → the running
    optimize stage "cancelled because SparkContext was shut down." I.e. the timeout killed FMH's interpreter
    mid-write, which cleanly tore down the shared session — Fabric's API then mislabeled it
    `System_Cancelled_Session_Statements_Failed`. → **`child_timeout` param, default 7200s** in `batch_runner`
    (NEE runner: `timeoutPerCellInSeconds` 1800→7200). Also **retries disabled** (`max_retries=0`) on the parent
    + child jobs and in `create_jobs.py` — a failed batch is not transient; retrying just re-ran the doomed ~30-min
    batch_runner 3× more. Lesson: a "session cancelled / SparkContext shut down" with exitCode 0 + zero executor
    failures is an orchestration timeout, not a Spark/capacity failure — read the app's final status first.

21. **Stat-less SHALLOW CLONEs make the CBO broadcast an 8.2 GiB side at SF=20000** (the DimCustomer/batch-2
    crash, root cause). A SHALLOW CLONE inherits none of its source's optimizer statistics, so when
    `DimCustomer Incremental`'s SCD2 MERGE joined the current `dimcustomer` snapshot (actual **2.18 GB / 27.5M
    current rows** at SF=20k) the CBO estimated it at **~6.8 MiB / ~103 KiB** — far under any threshold — and the
    plan built a **BroadcastExchange of ~8.2 GiB** that hit Spark's hard **8 GiB broadcast cap** →
    `Cannot broadcast the table that is larger than 8.0 GiB`. Note this is **threshold-INDEPENDENT**: the estimate
    was near-zero, so lowering/removing `autoBroadcastJoinThreshold` does nothing (we DID have a stale manual
    250 MB threshold in `currentaccountbalances`; removing it was correct hygiene but did NOT fix this — the
    default 10 MB alone still broadcasts a table the CBO thinks is 6.8 MiB). **The real fix is stats** — gotcha #9:
    ANALYZE the clones in `setup` + the injection flag, restoring Databricks parity. Ruled out along the way (all
    wrong theories): source dup (bronze was clean), streaming checkpoint carryover, multi-match MERGE, and
    session-level broadcast-cache carryover (each batch is a **fresh** Fabric session — nothing carries over).
    **Bonus — big perf win:** correct stats also let the CBO pick optimal joins everywhere, cutting steady-state
    per-batch compute at SF=20k from ~24–41 min down to **~11 min** (batch 2 = 650 s: FMH 598 s, DimTrade 70 s,
    FactWatches 400 s). This is the single highest-leverage fix in the whole port.

22. **Batch 1 is a one-time streaming warm-up artifact — take batch 2+ as steady state.** `fabric_ss` seeds each
    source table via CLONE, then `readStream + trigger(availableNow=True)`'s FIRST microbatch reads the ENTIRE
    pre-seeded source (the whole clone), so batch 1 does far more work than a real one-day increment (SF=20k:
    batch 1 = **1401 s** vs batch 2 = **650 s**). This is **streaming-specific** — the dbt/SDP competitors read
    with a batch-date filter and never see it, so batch 1 is NOT comparable across engines. **For perf/TCO, report
    batch 2+ as steady state** (both Databricks-Cluster-streaming and Fabric SS have this; fix them the same way).
    **Queued fix (do NOT apply while a run is live): stop streaming the source — read it with a `batch_date`
    filter** (the `fabric_nee` batch variant already does exactly this: `dm_date == to_date(batch_date)`), which
    eliminates the seed-reprocessing. Pending the user's variant decision — (a) retire Fabric SS in favor of NEE,
    or (b) keep a fixed streaming variant via `startingVersion` — AND only after the current SS run finishes.

**Streaming (★S — fabric_ss only; go away in fabric_nee batch)**
15. ★S **Fabric native file streaming source does NOT recurse into subdirectories** (Autoloader did).
    `simulate` drops per-date subdirs → `ingest_bronze` read 0 files. → `.option("recursiveFileLookup","true")`.
16. ★S **`toTable()`/`start()` return immediately and DON'T hold a triggered Fabric notebook open** (Databricks
    job tasks auto-wait). → **`.awaitTermination()`** on every streaming notebook, or `runMultiple` marks the
    activity done before the write lands.
17. ★S **`runMultiple` is incompatible with Structured Streaming — at ANY concurrency (incl. 1).** Concurrent
    (or even serial) streaming queries in the shared session get session-cancelled. `notebook.run`-sequential
    works. → `batch_runner` drives the DAG via **sequential `notebookutils.notebook.run`** in topological order.
    (fabric_nee, being batch, can go back to `runMultiple` for real parallelism.)

---

## 5. `fabric_ss` status (SF=10 validated 2026-09-05/06; SF=20000 anchor run LIVE 2026-09-06/07)

- **SF=10 smoke green E2E** (parent run `448279538373936` = SUCCESS): setup → simulate → batch_runner,
  all 16 steps ok, batch-1 in ~363 s.
- **Batch-1 counts sane** (vs `staging_sf10` baseline): 11 pure-reference tables Δ=0; incrementals added
  dimcustomer +5, dimaccount +13, dimtrade +675, factcashbalances +656, factholdings +324,
  factmarkethistory +7418, factwatches +588; internal consistency holds.

**SF=20000 anchor — LIVE on the dedicated F64 (`jfpmtechfabric`), as of 2026-09-07:**
- **HEALTHY and green** — parent run **`42548688122975`** (job `769529885403896`), child `177905919820649`,
  ~17–18 / 50 batches SUCCESS, **0 failed**, on Fabric workspace **`e0a370b6…`** + lakehouse **`54963c1f…`**
  (schema-enabled), custom pool **`tpcdi_f64_full`** (`a9513928…`, **16 Medium nodes fixed = a full F64**, since
  Memory Optimized is Fabric's only node family — gotcha #23). **Retries OFF** (a failed batch isn't transient).
  Databricks orchestration cluster **`0905-212234-wq3cjc8j`** is now **D8pds_v6 (ARM)** — the x86 D8ds_v6 hit
  Azure VM-launch timeouts; the ARM SKU launched in ~4 min (gotcha #23).
- **Steady state ≈ 11 min/batch** (batch 2 = 650 s), **≈ $2.9/batch** at F64 PAYG West US (down from the pre-ANALYZE
  ~$5.2). This is the number that made the anchor viable — see gotcha **#21** (the ANALYZE/stats fix). ~50 batches
  ≈ 9–10 h wall.
- **Batch 1 = 1401 s is a streaming warm-up artifact — excluded from steady-state** (gotcha #22).
- **Not yet:** true row-count parity diff vs a Databricks SF=20k reference; the batch-read refactor to kill the
  batch-1 artifact (queued, gotcha #22); teardown_fabric; `git push` (only synced to the tpc-di workspace repo —
  local git credential can't push to the GitHub repo); Fabric CU-second cost pull from the Capacity Metrics app.

---

## 6. Environment / ID reference

| Thing | Value |
|---|---|
| Dev workspace `pmt_fabric_ws` (capacity `pmtcapacity`/`e012df69` = **F64 paid**, West US; shared 689-item playground; **NEE works here**) | `4f119fd7-d1f7-48bc-be77-6c41c782b541` |
| Lakehouse `tpcdi_fabric` (on pmt_fabric_ws) | `3f5b1c43-a52c-4a2e-90d7-4de7504e6122` |
| Environment `tpcdi_fabric_rt2` (Runtime 2.0) | `e717a02d-0c78-4fc6-ba8b-47c0afb3be34` |
| Environment `tpcdi_fabric_nee` (RT2.0 + `spark.native.enabled=true`) | `3fc5348e…` (on pmt_fabric_ws) |
| **F64 anchor** workspace `jfpmtechfabric` (dedicated, capacity `15d8de25…`; **NEE-capable**) | `e0a370b6…` |
| Lakehouse (F64 anchor, schema-enabled) | `54963c1f…` |
| Custom Spark pool `tpcdi_f64_full` (16 Medium fixed = full F64) | `a9513928…` |
| SP `tpcdi-fabric-sp` | client_id `6710acdb-74b7-416d-8441-7c4617496e6f`; Contributor; creds in secret scope `tpcdi_fabric` |
| Databricks orchestrator | tpc-di workspace `5231320435511749` (profile `tpc-di`), catalog `main` |
| Interactive cluster (SINGLE_USER, now **D8pds_v6 ARM**) | `0905-212234-wq3cjc8j` |
| Jobs — `fabric_ss` SF=10 | parent `61571649434078`, child `769329688244453`, `wh_db=shannon_barrow_fabricss` |
| Jobs — `fabric_ss` SF=20000 anchor (LIVE) | parent run `42548688122975` / job `769529885403896`, child `177905919820649` |
| Jobs — `fabric_nee` SF=10 | parent `374896434758341`, `wh_db=shannon_barrow_fabricnee` |
| Tenant | `9f37a392-f0ae-4280-9796-f1864a10effc` (DataBricksInc) |

Access recipe: `az account get-access-token --resource https://api.fabric.microsoft.com` (Fabric REST)
or `…--resource https://storage.azure.com/` (OneLake DFS). Run az/curl with the Bash sandbox disabled.

---

## 7. `fabric_nee` plan (Native Execution Engine — batch)

**Constraint:** NEE cannot run Structured Streaming (silently falls back to JVM), so it's a **batch
re-implementation**, not a config toggle. Enable via the env Acceleration toggle / `spark.native.enabled=true`
on a separate isolated env (`tpcdi_fabric_nee`).

**What changes**
- Bronze: `readStream.format("csv")…toTable()` → batch `spark.read.csv(batch_dir)` + append. No checkpoint / availableNow / recursiveFileLookup / awaitTermination (gotchas #15–17 disappear).
- Silver/gold: lift the SQL out of `foreachBatch(upsertToDelta)` and run it as **plain batch statements** on the batch's bronze rows. Transform SQL is ~100% reusable (all the §4 SQL-dialect fixes #9–14 still apply).
- Orchestration: **`runMultiple` becomes usable again** (no concurrent-streaming session-cancel) → real parallel bronze + fan-out facts. Big perf contrast vs `fabric_ss`'s forced-serial.

**Design nuances**
- Batch-scoping "this batch's rows": streaming got it free via checkpoint; batch needs explicit scoping
  (bronze holds only the current batch, or transforms filter on `event_dt/update_dt = batch_date`).
- `bronzedailymarket` must retain history (FactMarketHistory's 52-week lookback) — can't be current-batch-only.

**NEE capabilities — VERIFIED via docs (2026-09-06), Runtime 2.0:**
- Streaming: **NOT supported (all runtimes)** → JVM fallback. Confirms the batch rewrite is mandatory.
- **CSV IS accelerated on 2.0** ("vectorized CSV parser now supports CSV") → bronze reads benefit. (One stale
  best-practices page still says CSV isn't accelerated; the authoritative NEE overview/limitations page says it
  is — trust the latter. Confirm at runtime via Spark Advisor / df.explain.)
- **UDFs + complex types (arrays/maps/structs) now supported on 2.0** → FactMarketHistory `struct/min_by/max_by`
  offload instead of falling back. `array_contains` still unsupported; JSON/XML not accelerated.
- **ANSI mode supported on 2.0** (was a 1.3-only limitation).
- Parity watch-items (silent diffs): `round()` (C++ std::round) and `collect_list()` ordering (shuffle) may
  differ from JVM; **date comparisons must be type-matched** (`CAST(x AS DATE) = '…'`) to actually offload —
  audit our dm_date/effectivedate/`DATE'9999-12-31'` comparisons.
- Enable: env Acceleration toggle or `spark.native.enabled=true` (or `%%configure {"conf":{...}}`). VERIFY offload
  via Spark Advisor real-time fallback alerts + `df.explain()` (look for `*Transformer`/`NativeFileScan`/
  `VeloxColumnarToRowExec`, green nodes in Spark UI). NEE = Velox (Meta C++) + Apache Gluten; ~4–6× on TPC-DS.

**Concrete coding changes (per catalog of the ss notebooks, 2026-09-06):**
Every incremental has ONE shape → ONE mechanical conversion. The `foreachBatch(fn)` body is reused verbatim
(the `fn(df, id)` does `df.createOrReplaceTempView("bronzeX")` + `spark.sql(<transform>)`); only the *driver* changes:
```
# ss (streaming):
(spark.readStream.table(src).writeStream.foreachBatch(fn).outputMode(...).trigger(availableNow=True).start()).awaitTermination()
# nee (batch): call the SAME fn on the batch's bronze rows — no SQL change
fn(<current-batch bronze DataFrame>, 0)
```
- **Bronze ingest → batch:** `spark.read.format("csv").schema(…).load(dir)` + write. No checkpoint / availableNow /
  recursiveFileLookup / awaitTermination (gotchas #15–17 gone). `.option("recursiveFileLookup","true")` still needed
  on the batch read (per-date subdir). NEE accelerates the CSV read on RT2.0.
- **Enable NEE:** `%%configure {"conf":{"spark.native.enabled":"true"}}` (or the `tpcdi_fabric_nee` env Acceleration toggle).
- **Orchestration:** `batch_runner` can go back to `runMultiple` (batch ⇒ no concurrent-streaming session-cancel) →
  real parallel bronze + fan-out facts (the perf story vs ss's forced-serial).

**★ THE key open design question — batch-scoping of the bronze inputs** (streaming got it free via the checkpoint;
targets like dimcustomer/dimsecurity persist & accumulate the same as streaming, so scoping is ONLY about the
bronze *sources*). Resolve per-table by reading each transform's FROM clauses (what it reads current-batch vs
cumulative):
RESOLVED (read the FROM clauses 2026-09-06): **every bronze source is current-batch-only (overwrite per batch)
EXCEPT `bronzedailymarket`.** All cumulative state lives in the *persistent targets*, which accumulate across
batches exactly as in streaming — so the bronze inputs don't need history:
- customer/account/trade/holdings/watches → transform reads current-batch bronze + persistent dim targets. Overwrite.
- **currentaccountbalances** → its `INSERT OVERWRITE` UNIONs current-batch `bronzecashtransaction` with the EXISTING
  `currentaccountbalances` snapshot (running-balance history lives in the target). So bronzecashtransaction = current
  batch only. ✓
- **DimTrade** → current-batch `bronzetrade` + join persistent `dimsecurity`/`dimaccount`. Overwrite. ✓
- **`bronzedailymarket` is the ONE exception** — FactMarketHistory's 52-week window reads the FULL table while the
  output rows are the current batch (ss filters the microbatch `dm_date>='2016-07-06'`). So dailymarket must
  **accumulate** (clone + append), and FMH's batch driver must isolate the current batch (filter by the batch's dm_date)
  for the output while still reading the full table for the window agg.
- `account_updates_from_customer` (reads bronzecustomer, appends bronzeaccount) must run before DimAccount — the
  DAG order already encodes this.

**Framing:** `fabric_ss` (streaming, JVM) vs `fabric_nee` (batch, NEE) is the core Fabric-internal comparison,
both against the Databricks Cluster (Photon) reference.

### 7a. `fabric_nee` — DEPLOYED + SF=10 validated; NEE offload CONFIRMED working (2026-09-07)

**★ CORRECTION (2026-09-07): NEE offload works — the earlier "NEE won't engage / trial doesn't support NEE"
conclusion was WRONG on two counts, both now disproven with evidence.**
1. **Not a trial.** `pmt_fabric_ws` is on capacity `pmtcapacity` / `e012df69`, **SKU F64 (paid)** — confirmed in
   Workspace settings → Workspace type. NEE is not SKU/trial-gated anyway (docs: works on any capacity, Runtime
   1.3 or 2.0, starter or custom pools).
2. **NEE was actually offloading the whole time; our detector read the wrong plan.** `dq_offload` checked
   `df._jdf.queryExecution().executedPlan().toString()`, which with **AQE enabled (default)** prints the
   `AdaptiveSparkPlan`'s **Initial Plan** — vanilla JVM node names (`HashAggregate`, `Scan parquet`) — **even when
   the Final Plan is fully columnar.** So it always said "JVM." Verified fix: read the **AQE Final Plan** via
   `queryExecution.explainString("formatted")` after `.collect()`, or set `spark.sql.adaptive.enabled=false`.
   With the correct check a Delta scan+agg offloads fully on BOTH pool types: `DefaultDeltaScanTransformer`,
   `FlushableHashAggregateTransformer`, `ColumnarExchange`, `RegularHashAggregateExecTransformer`,
   `VeloxColumnarToRow`. The Gluten plugin loads (`spark.plugins` has `org.apache.gluten.GlutenPlugin`,
   `spark.shuffle.manager=ColumnarShuffleManager`, all `spark.gluten.velox.*` confs injected).
   **`dq_offload.py` has been fixed to read the Final Plan** — trust it now.

**Curiosity (not a blocker):** on the **Starter Pool**, `spark.native.enabled` is `false` in the SparkContext
startup conf (the pre-warmed context can't take a startup-time property) but `true` in the runtime SQLConf; on a
**custom pool** it's `true` at both layers. Either way Gluten reads the runtime SQLConf at planning and offloads —
so pool type does NOT gate NEE. **How to verify NEE for real:** AQE Final Plan / `df.explain()` Final-Plan section
/ Spark UI **Gluten SQL / DataFrame** tab (green nodes) / Spark Advisor fallback alerts — NEVER the AQE Initial
Plan string.

**Still open for the actual pipeline:** confirm which of OUR real operators offload vs fall back — SCD2 `MERGE`,
`min_by/max_by(struct(...))`, the FMH 52-week window — using the Final-Plan/Gluten-tab method (per-operator
fallback is expected for some; the point is the engine engages). This is the NEE perf story to measure on the F64.


Full batch notebook set drafted under `fabric/notebooks_nee/` (mirrors `notebooks/`). Transform SQL is
**byte-identical** to `fabric_ss` in every incremental; only the driver changed. Files:
- `ingest_bronze.py` — batch `spark.read.csv(batch_dir)` of exactly this batch's per-date subdir (no
  checkpoint/availableNow/recursiveFileLookup). Writes via `INSERT [OVERWRITE|INTO] SELECT` to preserve the
  setup-created CLUSTER BY + `dataSkippingNumIndexedCols=34`. **dailymarket = INSERT INTO (accumulate);** all
  other bronze = INSERT OVERWRITE (current-batch-only).
- `account_updates_from_customer.py` — fn verbatim; driver `customeraccountupdates(spark.table(src), 0)`.
- `setup.py` — same DROP+CREATE schema + 20 SHALLOW clones + 6 bronze creates as ss; checkpoint-reset step
  DROPPED (no streams). bronzedailymarket clone = the prior-year seed.
- `incremental/*.py` (8) — each: fn body verbatim, driver = `fn(spark.table(src), 0)`. **FMH** is the one special
  driver: bronzedailymarket accumulates, so it passes only this batch (`dm_date == to_date(batch_date)`,
  type-matched for NEE offload) into the fn; the fn's 52-week agg still reads the full table. **FactCashBalances
  was already batch** in ss → NEE-identical.
- `batch_runner.py` — back to `notebookutils.notebook.runMultiple` (batch ⇒ no streaming session-cancel, so
  gotcha #17 doesn't apply) for real parallelism. **One DAG change vs ss:** `account_updates_from_customer`
  gains a `bronzeaccount` dependency — batch ingest OVERWRITES bronzeaccount, so the customer-derived append
  must run after it (streaming appended concurrently and didn't need the edge).

**Deployed + validated (2026-09-06/07):**
- **Deployed** to the shared **`pmt_fabric_ws`** (`4f119fd7…`) via `deploy_notebooks.py --source-dir notebooks_nee`
  (the `--source-dir` switch was added). Lakehouse `tpcdi_fabric` (`3f5b1c43…`); NEE env **`tpcdi_fabric_nee`
  (`3fc5348e…`)** created via `create_environment.py --spark-conf spark.native.enabled=true` (the `--spark-conf`
  switch was added; it sets env `sparkProperties`). NEE SF=10 job: **parent `374896434758341`**,
  `wh_db=shannon_barrow_fabricnee`.
- **SF=10 functionally GREEN** — all 3 batches succeeded via `runMultiple`, flat batch times (no batch-1
  streaming artifact, confirming gotcha #22's batch-read design). The batch DAG, stats/ANALYZE parity, and the
  `foreachBatch(fn)`→`fn(spark.table(src),0)` conversion all work.
- **NEE offload CONFIRMED WORKING on `pmt_fabric_ws` (F64) — see the ★ CORRECTION at the top of §7a.** The prior
  "does NOT run on the trial FTL64 / SKU limitation" bullet here was WRONG twice over (it's not a trial, and the
  detector was reading the AQE Initial Plan). Fixed `dq_offload.py` shows full Velox offload
  (`DefaultDeltaScanTransformer`, `HashAggregateTransformer`, `ColumnarExchange`, `VeloxColumnarToRow`) on both
  starter and custom pools, RT 2.0, `spark.native.enabled=true` (env sparkProperty published — no separate
  field; Fabric env Save=stage, Publish=apply).

**Next NEE steps:**
- The engine engages; now measure WHICH of our real operators offload vs fall back (SCD2 `MERGE`,
  `min_by/max_by(struct)`, FMH 52-week window) via the AQE Final Plan / Spark UI Gluten tab — see §7a.
- Run the **NEE SF=20k** test on the F64 (16-Medium pool + schema-enabled lakehouse + SF=20k staging) for the
  perf story, after the SS 20k run frees the F64.
- **Open parity watch-items** (from §7 capabilities): audit `round()` / `collect_list` ordering / `try_divide` /
  `min_by`/`max_by`(struct) for JVM-vs-Velox parity; the FMH `dm_date == to_date(batch_date)` filter is
  type-matched (should offload) — confirm.

---

## 8. `synapse_ss` / dbt plan (placeholder — fill when built)

Fabric Warehouse (T-SQL) via dbt, mirroring the cross-CDW dbt ports. Open questions: T-SQL dialect
translation of the SCD2 MERGE / window logic; how the OneLake data bridge feeds a Warehouse vs a Lakehouse;
whether the existing `augmented_incremental/dbt` project targets Fabric with a profile swap.

---

## 9. Open items / next up (priority order)

1. **Let the SS 20k anchor finish** (LIVE, ~17–18/50; monitor armed). Do NOT cancel/re-fire/kill it or touch the
   Fabric code while it runs (user directive). If a batch fails: diagnose, hold, and ask before any re-trigger.
2. **NEE on the F64** — once SS frees the F64: re-verify NEE offload there (`dq_offload.py`), then run NEE SF=20k
   (16-Medium pool + schema-enabled lakehouse + SF=20k staging). This is the NEE-vs-SS-vs-Databricks perf story.
3. **Batch-read refactor** (kills the batch-1 streaming artifact, gotcha #22) for BOTH Databricks-Cluster-streaming
   and Fabric SS — pending the user's variant decision: (a) retire Fabric SS for NEE, or (b) keep a fixed streaming
   variant via `startingVersion`. Only after the SS run finishes.
4. Cost attribution: Fabric bills **Capacity Units (CU-seconds)** via the Capacity Metrics app / monitoring API
   (no per-query tag). Confirm the programmatic surface; cost at the F64 PAYG West US rate (look up).
5. True row-count parity vs a Databricks SF=20k reference; `teardown_fabric`; `git push` once the credential is
   fixed; author a `fabric-translate` project skill.
