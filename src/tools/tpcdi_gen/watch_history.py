"""Generate WatchHistory — GrowingCrossProduct of customers x securities.

Mirrors the decompiled WatchHistoryBlackBox from DIGen.jar:
- Each WatchHistory record pairs a customer with a security via cross-product
- ACTV (new watch) and CNCL (remove watch) are the only actions (no changes)
- Each (w_c_id, w_s_symb) pair appears at most twice: one ACTV then one CNCL
- WHActivePct = 0.8 (80% ACTV / 20% CNCL per update batch)
- WHHistUpdatesLastID = CMUpdateLastID (1:1 mapping to Customer table updates)

Each generation (update) adds new customers and securities to the cross-product.
ACTV records are distributed across all generations — new_per_update per generation.
Within each generation, pairs are picked from the NEW cross-product entries for that
generation (existing_customers x new_securities + new_customers x all_securities),
ensuring every generation contributes customers/securities and guaranteeing uniqueness.

Historical output: w_c_id | w_s_symb | w_dts | w_action
Incremental output: cdc_flag | cdc_dsn | w_c_id | w_s_symb | w_dts | w_action

GrowingCrossProduct concept
===========================
The GrowingCrossProduct is a data structure from DIGen.jar that models a 2D grid
(customers x securities) which grows over time. It starts with an initial rectangle
of (start_left x start_right) and, at each "generation", expands by growth_left
customers on the left axis and growth_right securities on the right axis.

At generation g, the grid dimensions are:
    left_size(g)  = start_left  + growth_left  * g
    right_size(g) = start_right + growth_right * g
    total_pairs(g) = left_size(g) * right_size(g)

The NEW pairs added in generation g are:
    new_pairs(g) = total_pairs(g) - total_pairs(g-1)

These new pairs decompose into two rectangular regions:
  1. "existing customers x new securities": prev_left_size * growth_right
     (customers that already existed, paired with the newly-added securities)
  2. "new customers x all securities": growth_left * right_size(g)
     (newly-added customers paired with ALL securities including the new ones)

This decomposition is key: it guarantees that every generation introduces fresh
customer-security pairings without overlapping with any prior generation, because
each new pair occupies space that didn't exist in the previous generation's grid.

ACTV and CNCL record generation
================================
For each generation (update), the WHActivePct=0.8 split produces:
  - new_per_update = floor(wh_rows_per_update * 0.8) ACTV records (new watches)
  - del_per_update = wh_rows_per_update - new_per_update  CNCL records (cancellations)

ACTV records use hash-based pair selection within each generation's NEW cross-product
space. The hash function (hash_key) maps each record's sequential wh_id into the
range [0, new_cp_size) for that generation, then offsets by prev_total_cp to place
it in the global pair-ID space. This is analogous to DIGen's BijectivePermutation
— it spreads selections across both the "existing x new" and "new x all" regions,
ensuring diverse customer and security coverage per generation.

CNCL records are sampled from the ACTV pool (not generated from the cross-product
directly). This guarantees that every CNCL references a real ACTV pair. Temporal
ordering is enforced: each CNCL timestamp is computed as ACTV_timestamp + random_offset,
where random_offset is in [1, wh_end_timestamp - actv_timestamp), so the cancellation
always occurs strictly after the activation.

Deactivation quarters and cross-product parameters
===================================================
Securities begin appearing in the FINWIRE feed (FW_BEGIN_DATE) years before customer
updates start (CM_BEGIN_DATE). The gap between these two dates, measured in quarters,
determines sec_updates_before_cust — the number of quarterly security additions that
occur before any customer growth begins.

This affects the cross-product's initial "start_right" (how many securities exist
when customer updates begin) and "growth_right" (how many new securities are spread
across each customer-update generation). The remaining security quarters after the
gap are distributed evenly across the wh_update_last_id customer generations via
spread_sec = remaining_quarters * sec_per_quarter / wh_update_last_id.

Incremental batches
===================
Batch2 and Batch3 are simpler than the historical batch. Each produces exactly
wh_rows_per_update records with the same 80/20 ACTV/CNCL split. Instead of the
GrowingCrossProduct machinery, pairs are selected by hashing each record's row ID
into the expanded customer and security pools. The customer pool grows by new_custs
per batch. All records share a single batch date and include CDC columns (cdc_flag,
cdc_dsn) for downstream incremental processing.
"""

import math
from datetime import timedelta
from pyspark.sql import SparkSession, functions as F, Window
from .config import *
from .utils import write_file, seed_for, hash_key


# 80% of each update's rows are ACTV (new watches), 20% are CNCL (cancellations).
# This ratio is hardcoded in the original DIGen WatchHistoryBlackBox.
WH_ACTIVE_PCT = 0.8


def generate(spark: SparkSession, cfg, dicts: dict, dbutils) -> dict:
    """Generate WatchHistory for all batches. Returns counts dict."""
    counts = {}
    counts.update(_gen_historical(spark, cfg, dbutils))
    for batch_id in range(2, NUM_INCREMENTAL_BATCHES + 2):
        counts.update(_gen_incremental(spark, cfg, batch_id, dbutils))
    return counts


def _compute_cp_params(cfg):
    """Compute GrowingCrossProduct parameters matching WatchHistoryBlackBox.

    This derives the cross-product grid dimensions and growth rates from the
    scale-factor-dependent CustomerMgmt parameters. The logic mirrors DIGen's
    WatchHistoryBlackBox constructor:

    1. Derive CustomerMgmt update parameters (cust_per_update, acct_per_update)
       from internal_sf (= 5000 * scale_factor).
    2. Compute cm_update_last_id: how many "update rounds" of CustomerMgmt exist.
       This equals wh_update_last_id (1:1 mapping).
    3. hist_size = initial customer count before updates begin = the "start_left"
       dimension of the cross-product.
    4. Securities side: count how many securities are loaded before customer updates
       start (sec_updates_before_cust quarters), giving "start_right". The remaining
       security quarters are spread evenly across customer generations as "growth_right".
    5. wh_rows_per_update = total WH rows / (generations + 1), then split by
       WH_ACTIVE_PCT into new_per_update (ACTV) and del_per_update (CNCL).

    Returns a dict with all parameters needed by _gen_historical and _gen_incremental.
    """
    internal_sf = cfg.internal_sf
    # CustomerMgmt update sizing — mirrors DIGen's CMUpdateLastID derivation.
    # cust_per_update / acct_per_update are the total customer/account operations
    # per update round; fractions below split them into new/update/deactivate.
    cust_per_update = int(0.005 * internal_sf)
    acct_per_update = int(0.01 * internal_sf)
    new_custs = int(cust_per_update * 0.7)
    new_accts = int(acct_per_update * 0.7)
    # rpu (rows per update) = total CustomerMgmt rows generated per update round:
    #   new_accts + updated_accts(20%) + deactivated_accts(10%) + updated_custs(20%) + deactivated_custs(10%)
    rpu = new_accts + int(acct_per_update * 0.2) + int(acct_per_update * 0.1) + int(cust_per_update * 0.2) + int(cust_per_update * 0.1)
    cm_final = cfg.cm_final_row_count
    # cm_update_last_id = number of update generations. The initial load (hist_size)
    # plus cm_update_last_id * rpu rows = cm_final total rows.
    cm_update_last_id = (cm_final - new_accts) // rpu
    hist_size = cm_final - cm_update_last_id * rpu

    # WatchHistory has the same number of generations as CustomerMgmt updates.
    wh_update_last_id = cm_update_last_id
    # Evenly distribute total WH rows across all generations (including gen 0).
    wh_rows_per_update = cfg.wh_total // (wh_update_last_id + 1)
    # Split each generation's rows into ACTV (new watches) and CNCL (cancellations).
    new_per_update = int(wh_rows_per_update * WH_ACTIVE_PCT)
    del_per_update = wh_rows_per_update - new_per_update

    # --- Deactivation quarters: how many security quarters elapse before customers ---
    # Securities start appearing at FW_BEGIN_DATE; customers start at CM_BEGIN_DATE.
    # The gap in quarters determines how many securities are "pre-loaded" (start_right)
    # vs spread across customer generations (growth_right).
    fw_begin_ms = int(FW_BEGIN_DATE.timestamp() * 1000)
    cm_begin_ms = int(CM_BEGIN_DATE.timestamp() * 1000)
    # Subtract 2 because DIGen skips the first and last partial quarters.
    sec_updates_before_cust = int((cm_begin_ms - fw_begin_ms) / ONE_QUARTER_MS) - 2

    # sec_q1 = securities in the first quarter (total minus growth over remaining quarters).
    sec_q1 = cfg.sec_total - (cfg.fw_quarters - 1) * cfg.sec_per_quarter
    # hist_sec_ids = total securities available when customer updates begin.
    # This becomes start_right — the initial right dimension of the cross-product.
    if sec_updates_before_cust <= 0:
        hist_sec_ids = 0
    elif sec_updates_before_cust == 1:
        hist_sec_ids = sec_q1
    else:
        hist_sec_ids = sec_q1 + (sec_updates_before_cust - 1) * cfg.sec_per_quarter

    # Remaining security quarters are spread evenly across customer generations
    # to give growth_right — how many new securities are added per generation.
    remaining_sec_updates = max(0, cfg.fw_quarters - sec_updates_before_cust)
    spread_sec = int(remaining_sec_updates * cfg.sec_per_quarter / wh_update_last_id) if wh_update_last_id > 0 else 0

    return {
        "start_left": hist_size,       # Initial customer count (left axis of cross-product)
        "start_right": hist_sec_ids,    # Initial security count (right axis of cross-product)
        "growth_left": new_custs,       # New customers added per generation
        "growth_right": spread_sec,     # New securities added per generation
        "generations": wh_update_last_id,  # Total number of update generations
        "wh_rows_per_update": wh_rows_per_update,
        "new_per_update": new_per_update,  # ACTV records per generation (80%)
        "del_per_update": del_per_update,  # CNCL records per generation (20%)
        "new_custs": new_custs,
        "hist_size": hist_size,
        "cm_update_last_id": cm_update_last_id,
    }


def _gen_historical(spark, cfg, dbutils):
    """Generate historical WatchHistory.txt (Batch1).

    Distributes ACTV records across all generations (new_per_update each).
    Within each generation, picks unique pairs from the NEW cross-product entries.
    CNCL records reference pairs from earlier generations.

    The algorithm has 7 steps:
      1. Create a sequential range of all WH record IDs and assign each to a
         generation (update_id) and position within that generation.
      2. For ACTV records, hash into the NEW cross-product pairs for that generation.
         For CNCL records, hash into ALL previously-created pairs.
      3. Decompose each flat pair_id into (cust_idx, sec_idx) using the
         GrowingCrossProduct geometry (two rectangular regions per generation).
      4. Assign timestamps spread across the historical date range.
      5. Deduplicate ACTV pairs and index them for CNCL lookup.
      6. Sample from ACTV pool to create CNCL records with later timestamps.
      7. Union ACTV + CNCL and write to file.
    """
    symbols_df = spark.table("_symbols")
    num_sec = symbols_df.count()

    cp = _compute_cp_params(cfg)
    sl, sr = cp["start_left"], cp["start_right"]   # start_left (custs), start_right (secs)
    gl, gr = cp["growth_left"], cp["growth_right"]  # growth per generation
    max_gen = cp["generations"]
    wh_rpu = cp["wh_rows_per_update"]
    new_pu = cp["new_per_update"]   # ACTV count per generation
    del_pu = cp["del_per_update"]   # CNCL count per generation

    total_updates = max_gen + 1  # generations 0 through max_gen inclusive
    n_actv = new_pu * total_updates
    n_cncl = del_pu * total_updates
    wh_total_hist = wh_rpu * total_updates

    # Precompute generation sizes (Python side — only ~435 values)
    # For each generation g, compute:
    #   ls, rs         = left/right dimensions at generation g
    #   prev_ls/rs     = dimensions at generation g-1 (0 for g=0)
    #   prev_cp        = total cross-product size at g-1 (cumulative pairs before this gen)
    #   total_cp       = total cross-product size at g (cumulative pairs including this gen)
    #   new_cp         = new pairs added in this generation = total_cp - prev_cp
    # new_cp_size(g) = totalSize(g) - totalSize(g-1) = new pairs added in generation g
    gen_info = []
    for g in range(total_updates):
        ls = sl + gl * g
        rs = sr + gr * g
        total_cp = ls * rs
        if g == 0:
            prev_cp = 0
            prev_ls = 0
            prev_rs = 0
        else:
            prev_ls = sl + gl * (g - 1)
            prev_rs = sr + gr * (g - 1)
            prev_cp = prev_ls * prev_rs
        new_cp = total_cp - prev_cp
        gen_info.append((g, ls, rs, prev_ls, prev_rs, prev_cp, total_cp, new_cp))

    print(f"  WatchHistory: start=({sl} custs x {sr} secs), growth=(+{gl}, +{gr}), gen={max_gen}")
    print(f"  Per update: {new_pu} ACTV + {del_pu} CNCL = {wh_rpu}, total={wh_total_hist}")
    total_left = sl + gl * max_gen
    total_right = sr + gr * max_gen
    print(f"  Final dimensions: {total_left} custs x {total_right} secs = {total_left * total_right} total CP space")

    # Timestamp range: spread evenly across WH_BEGIN_DATE to WH_END_DATE.
    # Each generation gets a secs_per_update-wide time window, and records within
    # a generation are interpolated within that window by position.
    wh_begin_s = int(WH_BEGIN_DATE.timestamp())
    wh_end_s = int(WH_END_DATE.timestamp())
    wh_range_s = wh_end_s - wh_begin_s
    secs_per_update = wh_range_s // max(1, total_updates)

    # === Build generation lookup as broadcast DataFrame ===
    # This small DataFrame (~435 rows) is broadcast-joined to every WH record
    # so each record knows its generation's cross-product geometry.
    gen_rows = [(g, int(ls), int(rs), int(pls), int(prs), int(pcp), int(tcp), int(ncp))
                for g, ls, rs, pls, prs, pcp, tcp, ncp in gen_info]
    gen_df = spark.createDataFrame(gen_rows,
        ["gen_id", "left_size", "right_size", "prev_left_size", "prev_right_size",
         "prev_total_cp", "total_cp", "new_cp_size"])

    # === Step 1: Generate all WH records with generation assignment ===
    # Sequential range [0, wh_total_hist). Each record's generation (update_id) is
    # determined by integer division: records 0..wh_rpu-1 are gen 0, etc.
    # Within a generation, the first new_pu positions are ACTV, the rest are CNCL.
    all_df = (spark.range(0, wh_total_hist).withColumnRenamed("id", "wh_id")
        .withColumn("update_id", (F.col("wh_id") / F.lit(wh_rpu)).cast("int"))
        .withColumn("pos_in_update", (F.col("wh_id") % F.lit(wh_rpu)).cast("long"))
        .withColumn("w_action",
            F.when(F.col("pos_in_update") < F.lit(new_pu), F.lit("ACTV"))
             .otherwise(F.lit("CNCL")))
    )

    # Join generation info so each record has its generation's cross-product dimensions.
    all_df = all_df.join(F.broadcast(gen_df), all_df["update_id"] == gen_df["gen_id"], "left")

    # === Step 2: Assign unique pair within generation ===
    # ACTV records: hash wh_id into the NEW pairs added in this generation.
    #   For gen 0: hash into the full initial cross-product [0, start_left * start_right).
    #   For gen g>0: hash into [0, new_cp_size) then offset by prev_total_cp to get a
    #   globally unique pair_id. This ensures ACTV pairs never collide across generations
    #   because each generation's pair_ids occupy a non-overlapping range.
    #
    # CNCL records: hash wh_id into ALL pairs from prior generations [0, prev_total_cp).
    #   This means CNCL can only reference pairs that were already created (by earlier
    #   ACTV records), which is necessary for the "activate then cancel" invariant.
    #
    # The hash function (hash_key) acts as a pseudo-random permutation, analogous to
    # DIGen's BijectivePermutation — it spreads selections across both the
    # "existing custs x new secs" and "new custs x all secs" regions of new pairs.
    all_df = (all_df
        .withColumn("_pair_id",
            F.when(F.col("w_action") == "ACTV",
                F.when(F.col("update_id") == 0,
                    # Gen 0: hash into full initial cross-product
                    hash_key(F.col("wh_id"), seed_for("WH", "actv")) %
                        F.greatest(F.lit(1), F.col("total_cp").cast("long")))
                .otherwise(
                    # Gen g>0: hash into new pairs, offset to global pair space
                    F.col("prev_total_cp").cast("long") +
                    hash_key(F.col("wh_id"), seed_for("WH", "actv")) %
                        F.greatest(F.lit(1), F.col("new_cp_size").cast("long"))))
            .otherwise(
                # CNCL: hash into all previously-created pairs
                hash_key(F.col("wh_id"), seed_for("WH", "cncl")) %
                    F.greatest(F.lit(1), F.col("prev_total_cp").cast("long"))))
    )

    # === Step 3: Decompose pair_id into (cust_idx, sec_idx) ===
    # GrowingCrossProductLong.getPair() logic:
    # The flat pair_id must be decomposed back into 2D coordinates (customer, security).
    # The cross-product has three regions with different decomposition rules:
    #
    # Region A (pair_id < start_left * start_right):
    #   Generation 0's initial rectangle. Simple row-major: cust = pair_id / sr, sec = pair_id % sr.
    #
    # Region B (within "existing customers x new securities"):
    #   x = pair_id - prev_total_cp (offset into this generation's new pairs)
    #   If x < prev_left_size * growth_right, the pair is in the "existing x new" rectangle:
    #     cust_idx = x % prev_left_size  (cycles through existing customers)
    #     sec_idx  = x / prev_left_size + prev_right_size  (indexes into the new securities)
    #
    # Region C (within "new customers x all securities"):
    #   Otherwise the pair is in the "new x all" rectangle:
    #     cust_idx = (x - left_part_offset) / right_size + prev_left_size  (new customer)
    #     sec_idx  = pair_id % right_size  (any security in the current generation)
    hist_cp = F.lit(sl).cast("long") * F.lit(sr).cast("long")
    x_expr = F.col("_pair_id") - F.col("prev_total_cp").cast("long")
    left_part_offset = F.col("prev_left_size").cast("long") * F.lit(gr)

    all_df = (all_df
        .withColumn("_cust_idx",
            F.when(F.col("_pair_id") < hist_cp,
                # Generation 0 decomposition
                F.col("_pair_id") / F.lit(max(1, sr)))
            .when(x_expr < left_part_offset,
                # Existing customers paired with new securities
                x_expr % F.greatest(F.lit(1), F.col("prev_left_size").cast("long")))
            .otherwise(
                # New customers paired with all securities
                (x_expr - left_part_offset) / F.greatest(F.lit(1), F.col("right_size").cast("long"))
                + F.col("prev_left_size").cast("long"))
            .cast("long"))
        .withColumn("_sec_idx",
            F.when(F.col("_pair_id") < hist_cp,
                F.col("_pair_id") % F.lit(max(1, sr)))
            .when(x_expr < left_part_offset,
                x_expr / F.greatest(F.lit(1), F.col("prev_left_size").cast("long"))
                + F.col("prev_right_size").cast("long"))
            .otherwise(
                F.col("_pair_id") % F.greatest(F.lit(1), F.col("right_size").cast("long")))
            .cast("long"))
    )

    # Map _cust_idx to w_c_id (string customer ID)
    all_df = all_df.withColumn("w_c_id", F.col("_cust_idx").cast("string"))

    # Map _sec_idx to symbol info via broadcast join (temporal check applied after Step 4)
    all_df = all_df.withColumn("_sym_join_idx", (F.col("_sec_idx") % F.lit(num_sec)).cast("long"))
    all_df = all_df.join(
        F.broadcast(symbols_df.select(
            F.col("_idx").cast("long").alias("_sym_join_idx"),
            F.col("Symbol").alias("_sym_name"),
            F.col("creation_quarter").alias("_sym_cq"),
            F.col("deactivation_quarter").alias("_sym_dq"))),
        on="_sym_join_idx", how="left")

    # === Step 4: Timestamps for ACTV records ===
    # Each generation's base timestamp is wh_begin + update_id * secs_per_update.
    # Within a generation, records are interpolated by position: pos_in_update / wh_rpu
    # gives a fractional offset within the generation's time window.
    all_df = (all_df
        .withColumn("_update_base_ts",
            F.lit(wh_begin_s).cast("long") + F.col("update_id").cast("long") * F.lit(secs_per_update))
        .withColumn("_pos_frac",
            F.col("pos_in_update").cast("long") * F.lit(secs_per_update) / F.lit(max(1, wh_rpu)))
        .withColumn("_ts",
            (F.col("_update_base_ts") + F.col("_pos_frac")).cast("long"))
        .withColumn("w_dts", F.date_format(
            F.col("_ts").cast("timestamp"), "yyyy-MM-dd HH:mm:ss"))
    )

    # Temporal symbol check (now that _ts is available from Step 4).
    # Use strict > for creation_quarter to avoid within-quarter timing conflicts.
    # FW_BEGIN_DATE, ONE_QUARTER_MS already imported via "from .config import *"
    fw_begin_s_sym = int(FW_BEGIN_DATE.timestamp())
    quarter_secs = ONE_QUARTER_MS / 1000
    _sym0 = symbols_df.filter(F.col("_idx") == 0).select("Symbol").collect()[0][0]

    all_df = all_df.withColumn("_watch_quarter",
        ((F.col("_ts") - F.lit(fw_begin_s_sym)) / F.lit(quarter_secs)).cast("int"))
    all_df = all_df.withColumn("w_s_symb",
        F.when((F.col("_watch_quarter") > F.col("_sym_cq")) &
               (F.col("_watch_quarter") < F.col("_sym_dq")),
            F.col("_sym_name"))
         .otherwise(F.lit(_sym0)))

    # === Step 5: Extract ACTV records, deduplicate, index for CNCL lookup ===
    actv_df = all_df.filter(F.col("w_action") == "ACTV")
    # Deduplicate ACTV (hash collisions within generation) — dropDuplicates is
    # more efficient than row_number window (one shuffle vs TopK+Window)
    actv_df = actv_df.dropDuplicates(["w_c_id", "w_s_symb"])
    # Index ACTV pairs for CNCL to reference — monotonically_increasing_id avoids
    # the single-partition global sort that row_number(orderBy) requires
    actv_df = (actv_df
        .withColumn("_actv_idx", F.monotonically_increasing_id())
        .select("w_c_id", "w_s_symb", "w_dts", "w_action", "_ts", "_actv_idx"))

    # Get actual ACTV count (needed to compute exact CNCL count for target total).
    n_actv = actv_df.count()
    print(f"  WatchHistory: {n_actv:,} ACTV pairs (after dedup)")

    # === Step 6: Generate CNCL records to hit target total ===
    # DIGen produces exactly wh_total rows (ACTV + CNCL). We match this by computing
    # the CNCL count as target_total - actual_actv. This maintains close to the 80/20
    # ACTV/CNCL split while guaranteeing the exact total row count.
    # Sample from the ACTV pool — every CNCL references a valid ACTV pair.
    target_total = cfg.wh_total
    target_cncl = target_total - n_actv
    cncl_fraction = min(0.99, target_cncl / max(1, n_actv) * 1.05)  # slight oversample

    cncl_df = (actv_df
        .sample(fraction=cncl_fraction, seed=seed_for("WH", "cncl_sample"))
        .limit(target_cncl)  # exact count
        .withColumn("w_action", F.lit("CNCL"))
        # Compute a random time offset between the ACTV timestamp and wh_end,
        # ensuring the CNCL always comes after the ACTV (+1 second minimum).
        .withColumn("_cncl_offset",
            hash_key(F.monotonically_increasing_id(), seed_for("WH", "cncl_ts")) %
                F.greatest(F.lit(1), F.lit(wh_end_s).cast("long") - F.col("_ts")))
        .withColumn("w_dts", F.date_format(
            (F.col("_ts") + F.col("_cncl_offset") + 1).cast("timestamp"),
            "yyyy-MM-dd HH:mm:ss"))
    )

    # === Step 7: Combine ACTV + CNCL and write ===
    final_cols = ["w_c_id", "w_s_symb", "w_dts", "w_action"]
    result_df = actv_df.select(*final_cols).union(cncl_df.select(*final_cols))

    write_file(result_df, f"{cfg.batch_path(1)}/WatchHistory.txt", "|", dbutils,
               scale_factor=cfg.sf)

    print(f"  WatchHistory Batch1: {n_actv:,} ACTV + {target_cncl:,} CNCL = {target_total:,} target")
    return {("WatchHistory", 1): target_total}


def _gen_incremental(spark, cfg, batch_id, dbutils):
    """Generate incremental WatchHistory.txt for Batch2/3.

    Incremental batches are simpler than the historical batch. Each produces
    exactly wh_rows_per_update records with the same 80/20 ACTV/CNCL split.

    Instead of the full GrowingCrossProduct machinery, customer and security
    assignments are done by hashing each record's row ID into the expanded
    pools:
      - Customer pool: hist_size + cm_update_last_id * new_custs + (batch_offset+1) * new_custs
        (grows by new_custs each batch)
      - Security pool: all symbols from the _symbols table (static)

    All records in an incremental batch share a single date (batch_date) and
    include CDC columns:
      - cdc_flag = "I" (insert) for all records
      - cdc_dsn = sequential data sequence number starting from wh_total + batch_offset * inc_rows
    """
    symbols_df = spark.table("_symbols")
    num_sec = symbols_df.count()
    bs = seed_for(f"WH_B{batch_id}", "base")
    batch_offset = batch_id - 2  # 0 for Batch2, 1 for Batch3

    cp = _compute_cp_params(cfg)
    wh_rpu = cp["wh_rows_per_update"]
    new_pu = cp["new_per_update"]
    del_pu = cp["del_per_update"]
    inc_rows = wh_rpu

    # DSN continues from where the historical batch left off.
    dsn_base = cfg.wh_total + batch_offset * inc_rows
    # Customer pool expands each batch: historical customers + all update-generation
    # customers + incremental-batch customers up to this batch.
    total_customers = cp["hist_size"] + cp["cm_update_last_id"] * cp["new_custs"]
    total_customers += (batch_offset + 1) * cp["new_custs"]

    # All incremental records share a single date for this batch.
    batch_date = (FIRST_BATCH_DATE + timedelta(days=batch_id - 1)).strftime("%Y-%m-%d")

    # Build the incremental DataFrame: sequential range, hash-based customer/security
    # assignment, same 80/20 ACTV/CNCL split as historical.
    inc_df = (spark.range(0, inc_rows).withColumnRenamed("id", "rid")
        .withColumn("cdc_flag", F.lit("I"))
        .withColumn("cdc_dsn", (F.lit(dsn_base) + F.col("rid")).cast("string"))
        .withColumn("w_action",
            F.when(F.col("rid") < F.lit(new_pu), F.lit("ACTV"))
             .otherwise(F.lit("CNCL")))
        # Hash row ID to pick a customer from the expanded pool.
        .withColumn("w_c_id",
            (hash_key(F.col("rid"), bs + 1) % F.lit(total_customers)).cast("string"))
        # Hash row ID (different seed) to pick a security symbol index.
        .withColumn("_sym_idx",
            (hash_key(F.col("rid"), bs + 2) % F.lit(num_sec)).cast("long"))
        .withColumn("w_dts", F.lit(batch_date))
    )

    # Map _sym_idx to w_s_symb with temporal validity check.
    # FW_BEGIN_DATE, ONE_QUARTER_MS, FIRST_BATCH_DATE already imported via "from .config import *"
    fw_begin_s = int(FW_BEGIN_DATE.timestamp())
    quarter_secs = ONE_QUARTER_MS / 1000
    batch_dt = FIRST_BATCH_DATE + timedelta(days=batch_id - 1)
    batch_quarter = int((int(batch_dt.timestamp()) - fw_begin_s) / quarter_secs)
    _sym0 = symbols_df.filter(F.col("_idx") == 0).select("Symbol").collect()[0][0]

    inc_df = inc_df.join(
        F.broadcast(symbols_df.select(
            F.col("_idx").cast("long").alias("_sym_idx"),
            F.col("Symbol").alias("_sym_name"),
            F.col("creation_quarter").alias("_sym_cq"),
            F.col("deactivation_quarter").alias("_sym_dq"))),
        on="_sym_idx", how="left")
    inc_df = inc_df.withColumn("w_s_symb",
        F.when((F.lit(batch_quarter) > F.col("_sym_cq")) &
               (F.lit(batch_quarter) < F.col("_sym_dq")),
            F.col("_sym_name"))
         .otherwise(F.lit(_sym0)))

    inc_df = inc_df.select("cdc_flag", "cdc_dsn", "w_c_id", "w_s_symb", "w_dts", "w_action")

    write_file(inc_df, f"{cfg.batch_path(batch_id)}/WatchHistory.txt", "|", dbutils, scale_factor=cfg.sf)
    print(f"  WatchHistory Batch{batch_id}: {inc_rows} rows ({new_pu} ACTV, {del_pu} CNCL)")
    return {("WatchHistory", batch_id): inc_rows}
