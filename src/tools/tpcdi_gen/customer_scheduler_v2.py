"""V8-customer: Spark-native CustomerMgmt scheduler.

Replaces the driver-side numpy + ``_resolve_skip_vec`` loop in
``customer.generate_customermgmt`` with a port of DIGen's
``GrowingOffsetPermutation`` algorithm executed via a Pandas UDF on
Spark executors. Eliminates the ~22-26 min driver-only step at SF=20k.

DIGen's algorithm in one paragraph
----------------------------------
For each generation g (= update id, 1..G), DIGen picks INACT/CLOSE/
UPDCUST/UPDACCT/ADDACCT entities by calling
``GrowingOffsetPermutation.getPermutation(slot, g-1)`` on the Customer
or Account "deleted permutation". This is a closed-form recursion over
generations that maps a slot index → a unique entity ID drawn from
the alive pool at gen g-1, without any iterative state. The recursion
shifts permVal by per-generation deletion offsets, walking back
through earlier gens until permVal lands above the deletion range.
Same-update non-collision between action types is achieved by giving
each action type a disjoint slot range:

    INACT     uses slots [0, del_custs)
    UPDCUST   uses slots [del_custs, del_custs + change_custs)
    ADDACCT   uses slots [del_custs + change_custs,
                          del_custs + change_custs + addaccts_per_update)

(and analogously for CLOSE / UPDACCT on the Account permutation).
The bijection over [0, alive_pool) guarantees uniqueness within a slot
range, and disjoint ranges guarantee uniqueness across action types.

Output contract — matches the legacy scheduler so the existing
join in ``customer.generate_customermgmt`` (line ~494) keeps working:

    columns: _sched_update (int), _sched_action (string),
             _sched_pos (int), _sched_id (long)
    actions: "INACT", "CLOSEACCT", "UPDCUST", "UPDACCT", "ADDACCT"

The IDs from this scheduler will NOT byte-for-byte match the legacy
output (different bijection parameters, different action-type pool
semantics for ADDACCT). Per the user policy, audit static snapshots
will be regenerated against this scheduler.
"""
import math
import random as _rand

import numpy as _np

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import LongType


# ---------------------------------------------------------------------------
# GrowingOffsetPermutation — per-generation bijection params
# ---------------------------------------------------------------------------
def _coprime_b(modulus: int, rng: _rand.Random) -> int:
    """Pick `b` in [1, modulus) with gcd(b, modulus) = 1.

    Mirrors DIGen's BijectivePermutation parameter selection. For our
    use the modulus is the alive-pool size at a generation; an
    accepted `b` makes ``(x*b + c) % modulus`` a true bijection over
    [0, modulus).
    """
    if modulus < 3:
        return 1
    b = rng.randrange(1, modulus)
    while math.gcd(b, modulus) != 1:
        b = b + 1 if b + 1 < modulus else 1
    return int(b)


def _build_growing_offset_params(sizes: _np.ndarray, offsets: _np.ndarray,
                                 seed: int) -> dict:
    """Pre-compute the per-generation bijection parameters for a
    GrowingOffsetPermutation.

    Args:
        sizes: ``maxValidIDsPerGeneration`` — cumulative entity count
            through gen i. ``sizes[i]`` = total customers/accounts that
            exist at end of gen i (NOT counting deletions).
        offsets: ``deleteOffsets`` — deletions at gen i+1 by DIGen's
            convention. Length must equal ``len(sizes)``.
        seed: Deterministic seed (e.g. ``seed_for("CM", "cust_perm")``).

    Returns:
        dict with broadcast-friendly numpy arrays:
          - ``b[g]`` / ``c[g]`` — bijection parameters at gen g
          - ``perm_max[g]`` — alive pool size at gen g (= bijection's modulus)
          - ``offsets[g]`` — delete count at gen g+1 (== input ``offsets``)
          - ``offset_sums[g]`` — cumulative deletions before gen g
    """
    rng = _rand.Random(seed)
    n = len(sizes)
    b = _np.zeros(n, dtype=_np.int64)
    c = _np.zeros(n, dtype=_np.int64)
    perm_max = _np.zeros(n, dtype=_np.int64)
    offset_sums = _np.zeros(n, dtype=_np.int64)

    perm_max[0] = sizes[0]
    b[0] = _coprime_b(int(sizes[0]), rng)
    c[0] = rng.randrange(0, max(1, int(sizes[0])))
    for i in range(1, n):
        offset_sums[i] = offset_sums[i - 1] + offsets[i - 1]
        size_i = int(sizes[i] - offset_sums[i])
        if size_i < 0:
            raise ValueError(f"GrowingOffsetPerm: pool degenerated at gen {i}")
        perm_max[i] = size_i
        b[i] = _coprime_b(size_i, rng) if size_i >= 3 else 1
        c[i] = rng.randrange(0, max(1, size_i))
    return {
        "b": b, "c": c,
        "perm_max": perm_max,
        "offsets": _np.asarray(offsets, dtype=_np.int64),
        "offset_sums": offset_sums,
    }


def _get_permutation_vec(slots: _np.ndarray, gens: _np.ndarray,
                         params: dict) -> _np.ndarray:
    """Vectorized DIGen GrowingOffsetPermutation.getPermutation.

    For each row i, computes the recursion:
        permVal = bij[gens[i]](slots[i])
        while gens[i] > 0:
            gens[i] -= 1
            permVal += offsets[gens[i]]
            if permVal >= perm_max[gens[i]]:
                return permVal + offset_sums[gens[i]]
            permVal = bij[gens[i]](permVal)
        return permVal

    Implementation runs all rows in lockstep with a "still active"
    mask. Each iteration applies one bijection step + one shift +
    one exit-check. Bounded by max(gens) iterations — typically
    ~half that on average.
    """
    b = params["b"]
    c = params["c"]
    perm_max = params["perm_max"]
    offsets = params["offsets"]
    offset_sums = params["offset_sums"]

    n = len(slots)
    cur_gen = gens.astype(_np.int64).copy()
    cur_val = slots.astype(_np.int64).copy()
    out = _np.full(n, -1, dtype=_np.int64)
    active = _np.ones(n, dtype=bool)

    # Bound: G+1 iterations is a hard upper bound (recursion depth ≤ G).
    max_iter = int(perm_max.shape[0]) + 1
    for _ in range(max_iter):
        if not active.any():
            break
        # Apply bij[gen] to permVal for all active rows.
        g = cur_gen[active]
        cv = cur_val[active]
        cur_val[active] = (cv * b[g] + c[g]) % perm_max[g]

        # Rows that just hit gen 0: return the bijection result directly.
        zero_mask_local = (cur_gen[active] == 0)
        idx_active = _np.flatnonzero(active)
        if zero_mask_local.any():
            done_global = idx_active[zero_mask_local]
            out[done_global] = cur_val[done_global]
            active[done_global] = False

        # Remaining active rows: decrement gen, shift permVal by offsets[gen].
        nonzero_mask_local = ~zero_mask_local
        if not nonzero_mask_local.any():
            continue
        nonzero_global = idx_active[nonzero_mask_local]
        new_gen = cur_gen[nonzero_global] - 1
        cur_gen[nonzero_global] = new_gen
        cur_val[nonzero_global] = cur_val[nonzero_global] + offsets[new_gen]

        # Rows whose permVal now >= perm_max[gen] exit with an offset_sum shift.
        max_at_gen = perm_max[new_gen]
        exited_local = cur_val[nonzero_global] >= max_at_gen
        if exited_local.any():
            exited_global = nonzero_global[exited_local]
            out[exited_global] = cur_val[exited_global] + offset_sums[cur_gen[exited_global]]
            active[exited_global] = False
        # The rest loop again — next iteration applies bij[gen-1] to permVal.

    if active.any():
        # Defensive: should never happen given max_iter bound. Surface for
        # debugging if it does.
        raise RuntimeError(
            f"GrowingOffsetPerm: {active.sum()} rows didn't converge after "
            f"{max_iter} iterations")
    return out


# ---------------------------------------------------------------------------
# Schedule builder — the public API customer.generate_customermgmt calls
# ---------------------------------------------------------------------------
def build_schedule_df(spark: SparkSession, *,
                      hist_size: int, update_last_id: int,
                      new_custs: int, change_custs: int, del_custs: int,
                      new_accts: int, change_accts: int, del_accts: int,
                      addaccts_per_update: int,
                      cust_perm_seed: int, acct_perm_seed: int):
    """Build the unified ``schedule_df`` directly in Spark.

    Output schema matches the legacy scheduler at
    ``customer.generate_customermgmt`` line ~452:

        _sched_update LONG    update id (1..G)
        _sched_action STRING  one of: INACT, CLOSEACCT, UPDCUST, UPDACCT, ADDACCT
        _sched_pos    LONG    intra-(update, action) row index — used by
                              the join in customer.py to align with the
                              shuffled-pos column on all_df
        _sched_id     LONG    the C_ID or CA_ID picked for this slot

    Args:
        hist_size: customers/accounts created in update 0 (historical).
        update_last_id: G — the last update id (so total updates = G).
        new_custs / change_custs / del_custs: target row counts per
            update for NEW / UPDCUST / INACT.
        new_accts / change_accts / del_accts: same for accounts.
        addaccts_per_update: ``new_accts - new_custs`` extra accounts.
        cust_perm_seed / acct_perm_seed: deterministic seeds for the
            GrowingOffsetPermutation parameter generation. Different
            from the legacy seeds — output IDs won't byte-for-byte
            match the legacy scheduler.

    Returns:
        A Spark DataFrame ready to be joined against ``all_df`` in
        ``customer.generate_customermgmt``.
    """
    G = update_last_id

    # Build the GrowingOffsetPermutation params for both customer and account
    # tables. sizes/offsets follow DIGen's UpdateBlackBox.initialize layout:
    #   sizes[i] = cumulative entities through gen i
    #   offsets[i] = deletions at gen i+1
    # Note: array length is G+1 so we can index gen=0..G.
    cust_sizes = _np.array(
        [hist_size + i * new_custs for i in range(G + 1)], dtype=_np.int64)
    acct_sizes = _np.array(
        [hist_size + i * new_accts for i in range(G + 1)], dtype=_np.int64)
    cust_offsets = _np.array(
        [del_custs if i + 1 <= G else 0 for i in range(G + 1)], dtype=_np.int64)
    acct_offsets = _np.array(
        [del_accts if i + 1 <= G else 0 for i in range(G + 1)], dtype=_np.int64)

    cust_params = _build_growing_offset_params(cust_sizes, cust_offsets, cust_perm_seed)
    acct_params = _build_growing_offset_params(acct_sizes, acct_offsets, acct_perm_seed)

    # Action slot layouts on the customer perm:
    #   slots [0, del_custs)                                       → INACT
    #   slots [del_custs, del_custs + change_custs)                → UPDCUST
    #   slots [del_custs + change_custs,
    #          del_custs + change_custs + addaccts_per_update)     → ADDACCT
    # Account perm:
    #   slots [0, del_accts)                                       → CLOSEACCT
    #   slots [del_accts, del_accts + change_accts)                → UPDACCT
    cust_slot_offsets = {
        "INACT":   0,
        "UPDCUST": del_custs,
        "ADDACCT": del_custs + change_custs,
    }
    acct_slot_offsets = {
        "CLOSEACCT": 0,
        "UPDACCT":   del_accts,
    }

    # Pass the bijection params to executors via closure capture (the
    # ``spark.sparkContext.broadcast()`` API is blocked on serverless,
    # but cloudpickle-serializing a small dict of numpy arrays into the
    # UDF closure is cheap — ~25 KB at SF=20k for both perms combined).
    from pyspark.sql.functions import pandas_udf
    import pandas as pd

    _cust_params_local = cust_params
    _acct_params_local = acct_params

    @pandas_udf(LongType())
    def _cust_perm_udf(slots: pd.Series, gens: pd.Series) -> pd.Series:
        ids = _get_permutation_vec(
            slots.to_numpy(dtype=_np.int64),
            gens.to_numpy(dtype=_np.int64),
            _cust_params_local)
        return pd.Series(ids)

    @pandas_udf(LongType())
    def _acct_perm_udf(slots: pd.Series, gens: pd.Series) -> pd.Series:
        ids = _get_permutation_vec(
            slots.to_numpy(dtype=_np.int64),
            gens.to_numpy(dtype=_np.int64),
            _acct_params_local)
        return pd.Series(ids)

    # Per-action picks. Each action emits exactly its target rows per update,
    # so the output is N_action * G rows for each action.
    def _picks_for(action: str, target_n: int, slot_offset: int, perm_udf):
        if target_n <= 0:
            return None
        df = (spark.range(0, G * target_n)
              .withColumnRenamed("id", "_seq")
              .withColumn("_sched_update", (F.col("_seq") / F.lit(target_n)).cast("long") + F.lit(1))
              .withColumn("_sched_pos", (F.col("_seq") % F.lit(target_n)).cast("long"))
              .withColumn("_sched_action", F.lit(action)))
        # gen passed to GrowingOffsetPerm = update_id - 1 (DIGen convention).
        df = df.withColumn(
            "_sched_id",
            perm_udf(
                (F.col("_sched_pos") + F.lit(slot_offset)).cast("long"),
                (F.col("_sched_update") - F.lit(1)).cast("long"),
            ),
        )
        return df.select("_sched_update", "_sched_action",
                         "_sched_pos", "_sched_id")

    inact_df   = _picks_for("INACT",     del_custs,           cust_slot_offsets["INACT"],   _cust_perm_udf)
    updcust_df = _picks_for("UPDCUST",   change_custs,        cust_slot_offsets["UPDCUST"], _cust_perm_udf)
    addacct_df = _picks_for("ADDACCT",   addaccts_per_update, cust_slot_offsets["ADDACCT"], _cust_perm_udf)
    close_df   = _picks_for("CLOSEACCT", del_accts,           acct_slot_offsets["CLOSEACCT"], _acct_perm_udf)
    updacct_df = _picks_for("UPDACCT",   change_accts,        acct_slot_offsets["UPDACCT"],   _acct_perm_udf)

    parts = [df for df in (inact_df, updcust_df, addacct_df, close_df, updacct_df)
             if df is not None]
    if not parts:
        # Pathological: nothing to schedule. Return empty with correct schema.
        from pyspark.sql.types import StructType, StructField, StringType
        empty_schema = StructType([
            StructField("_sched_update", LongType()),
            StructField("_sched_action", StringType()),
            StructField("_sched_pos", LongType()),
            StructField("_sched_id", LongType()),
        ])
        return spark.createDataFrame([], schema=empty_schema)

    out = parts[0]
    for p in parts[1:]:
        out = out.unionByName(p)
    return out
