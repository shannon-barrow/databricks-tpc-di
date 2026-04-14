"""Generate CustomerMgmt XML — exact DIGen logic using update-by-update state machine.

Mirrors the decompiled CustomerMgmtScheduler + CustomerAccountBlackBox from DIGen.jar:
- Historical batch (update 0): all NEW actions
- Updates 1..CMUpdateLastID: mixed actions per update with exact counts
- BijectivePermutation-like shuffle within each update for randomized ordering
- Sequential C_ID and CA_ID allocation across updates
- Referential integrity: non-NEW actions only reference previously-created IDs

CustomerMgmt XML State Machine
==============================
The TPC-DI CustomerMgmt.xml file represents the Change Data Capture (CDC) stream from
a hypothetical customer management OLTP system. Each XML ``<TPCDI:Action>`` element
carries an ``ActionType`` that describes a state transition in one of two entity types
(Customer or Account):

  **Customer-level actions:**
    - ``NEW``       — Create a new customer *and* their first brokerage account.
                      Emits full Customer element (Name, Address, ContactInfo, TaxInfo)
                      plus one embedded Account element. This is the only action that
                      creates *both* a C_ID and a CA_ID simultaneously.
    - ``UPDCUST``   — Update an existing customer's profile (address, phone, email, tier).
                      Uses **sparse fields**: only the changed attributes are populated;
                      unchanged fields appear as empty XML elements (e.g. ``<C_CITY/>``).
                      No Account element is present.
    - ``INACT``     — Deactivate (inactivate) an existing customer. Emits a minimal
                      Customer element with only C_ID — no Name, Address, or Account.

  **Account-level actions:**
    - ``ADDACCT``   — Add a new brokerage account to an existing customer. The C_ID
                      references a previously-created customer; the CA_ID is newly allocated.
                      Emits a Customer wrapper (C_ID only) with an embedded Account element.
    - ``UPDACCT``   — Update an existing account's attributes (broker, name, tax status).
                      C_ID and CA_ID both reference previously-created entities.
    - ``CLOSEACCT`` — Close an existing account. Emits a minimal Account element
                      (CA_ID only, no broker or name) inside a Customer wrapper.

C_ID and CA_ID Sequential Allocation
=====================================
IDs are allocated **sequentially across updates** to ensure uniqueness and enable
referential integrity:

  - **Update 0 (historical batch):** ``hist_size`` rows, all NEW actions.
      - C_IDs: 0 .. hist_size-1
      - CA_IDs: 0 .. hist_size-1 (one account per new customer)
  - **Update k (k >= 1):** ``rows_per_update`` rows with mixed action types.
      - NEW creates ``new_custs`` customers and ``new_custs`` accounts:
          C_IDs: hist_size + (k-1)*new_custs .. hist_size + k*new_custs - 1
          CA_IDs (for NEW): sequential within the update's account allocation
      - ADDACCT creates ``addaccts_per_update`` additional accounts (no new customers):
          CA_IDs: continue sequentially after NEW's CA_IDs within the same update
      - Total new CA_IDs per update: new_custs + addaccts_per_update = new_accts
  - **Non-NEW/ADDACCT actions** (UPDACCT, CLOSEACCT, UPDCUST, INACT) always reference
    C_IDs and CA_IDs from *prior* updates only (update_id - 1 and earlier), enforcing
    the invariant that an entity must exist before it can be modified or closed.

Temporal Ordering (NEWs first)
==============================
Within each update, actions are ordered by a deterministic sort key that enforces:
  1. **NEW** actions come first (sub-sorted by C_ID) — ensures the customer and its
     first account exist before any subsequent actions reference them.
  2. **ADDACCT** actions come next (sub-sorted by CA_ID) — the new account's C_ID
     was created in a prior update, so it already exists.
  3. **UPDACCT**, **CLOSEACCT**, **UPDCUST**, **INACT** follow (sorted by position).
This ordering is critical for Rule 1 (customer exists before account) and Rule 5
(no action references a future entity). The sort key is:
  ``update_id * 1_000_000 + action_order * 100_000 + entity_id_or_position``

Per-Day Deduplication
=====================
The TPC-DI spec requires at most one update per entity per calendar day:
  - **Customer-level dedup:** For each (C_ID, day) pair, at most one non-ADDACCT action
    is kept (the earliest by sort key). ADDACCT is exempt because it creates a *new*
    account — it does not conflict with a same-day UPDCUST or INACT on the same C_ID.
    Example conflict: if NEW and UPDCUST for the same C_ID fall on the same day, only
    the NEW is kept.
  - **Account-level dedup:** For each (CA_ID, day) pair among UPDACCT/CLOSEACCT actions,
    only the first is kept. NEW and ADDACCT are exempt (they create the CA_ID and thus
    cannot conflict with a prior action on the same CA_ID on the same day).
Both dedup passes use ``row_number()`` partitioned by the dedup key, ordered by
``_sort_key``, keeping only rank == 1.

Sparse UPDCUST Fields
=====================
When ActionType is UPDCUST, the XML emits only the fields that "changed." This is
implemented by giving each field an independent ~50% probability of being populated
(via ``hash_key(global_seq, field_seed) % 100 < 50``). Fields that are not "changed"
appear as self-closing empty elements (e.g. ``<C_ADLINE1/>``). This matches DIGen's
``CMFieldUpdatePct = 50`` parameter. The Address and ContactInfo blocks are always
present (as containers), but individual fields within them are independently sparse.
Customer-level attributes like C_TIER only appear ~20% of the time on UPDCUST actions.

Incremental Customer.txt and Account.txt CDC Generation
=======================================================
For Batch 2 and Batch 3, the generator produces pipe-delimited CDC extract files
(Customer.txt and Account.txt) rather than XML. Key differences from CustomerMgmt.xml:
  - **All fields present:** Every row has every column (no sparse fields). This is a
    traditional CDC extract where the source system sends the full current row state.
  - **CDC_FLAG column:** ``'I'`` for inserts (new entities), ``'U'`` for updates.
  - **CDC_DSN column:** Sequential data sequence number, continuing from the historical
    batch's sequence to maintain a monotonically increasing stream position.
  - **ID continuation:** New C_IDs and CA_IDs continue from where the historical batch
    left off (n_hist_customers, n_hist_accounts), incremented by batch offset.
  - **Phone fields:** Individual pipe-delimited columns (c_ctry_1, c_area_1, c_local_1,
    c_ext_1, etc.) rather than nested XML phone blocks.

_closed_accounts Temp View
==========================
After generating CustomerMgmt.xml, all CA_IDs with ActionType=CLOSEACCT are collected
into a ``_closed_accounts`` temp view (column: ``closed_ca_id``). This view is consumed
by ``trade.py`` to exclude closed accounts from trade generation — no trades should
reference an account that has been closed.

_customer_dates Temp View
=========================
A ``_customer_dates`` temp view is created with columns:
  - ``cust_id``: The C_ID
  - ``cust_create_ts``: The ActionTS of the NEW action that created this customer
  - ``cust_create_update``: The update_id in which the customer was created
  - ``cust_inact_ts``: The earliest ActionTS of any INACT action for this customer
    (NULL if never inactivated)
This view is consumed by ``watch_history.py`` to ensure watch items only fall within
the temporal window when the customer was active (created and not yet inactivated).
"""

from pyspark.sql import SparkSession, functions as F, Window
from .config import *
from .utils import write_file, write_text, seed_for, dict_join, dict_join_batch, hash_key, register_copy, log


def generate_customermgmt(spark: SparkSession, cfg, dicts: dict, dbutils, views_ready_event=None) -> dict:
    """Generate CustomerMgmt.xml with exact DIGen action distribution and ID allocation.

    This function implements the full state machine that produces the TPC-DI
    CustomerMgmt.xml file. It works in several phases:

    1. **Compute scaling parameters** — Derive per-update action counts from the
       scale factor using the same formulas as DIGen.jar's CustomerMgmtScheduler.
    2. **Build a DataFrame of all actions** — One row per action, spanning the
       historical batch (update 0) and all regular updates (1..update_last_id).
    3. **Shuffle within updates** — Apply a deterministic pseudo-random permutation
       within each update to randomize the order of actions (except update 0, which
       is all NEWs and needs no shuffle).
    4. **Assign action types** — Based on shuffled position within each update,
       assign NEW, ADDACCT, UPDACCT, CLOSEACCT, UPDCUST, or INACT.
    5. **Allocate C_ID and CA_ID** — Sequential for NEW/ADDACCT, hash-based reference
       to prior IDs for update/close/inactivate actions.
    6. **Assign timestamps** — Deterministic timestamps that respect temporal ordering
       (NEWs before other actions within each update).
    7. **Generate customer/account attributes** — Names, addresses, phones, etc. via
       dictionary lookups and hash-based attribute generation.
    8. **Build XML** — Assemble XML fragments per action type, including sparse fields
       for UPDCUST.
    9. **Deduplicate** — Enforce at most one action per entity per calendar day.
    10. **Write output** — Write XML part files and create temp views for downstream
        generators (trade.py, watch_history.py).

    Args:
        spark: Active SparkSession.
        cfg: Configuration object with scale factor and path information.
        dicts: Dictionary of reference data (names, addresses, etc.).
        dbutils: Databricks utilities for file operations.

    Returns:
        dict: Mapping of (file_name, batch_id) -> row count.
    """
    brokers_df = spark.table("_brokers")
    broker_count = brokers_df.count()

    # === Exact DIGen formulas (verified against audit at SF=10) ===
    # These scaling constants (CScaling=0.005, AScaling=0.01) come from DIGen.jar's
    # CustomerMgmtScheduler class, controlling how many customer and account actions
    # are generated per update cycle at each scale factor.
    internal_sf = cfg.internal_sf
    cust_per_update = int(0.005 * internal_sf)  # CScaling * SF
    acct_per_update = int(0.01 * internal_sf)   # AScaling * SF

    # Customer table: 70% new, 20% change, 10% delete
    new_custs = int(cust_per_update * 0.7)
    change_custs = int(cust_per_update * 0.2)
    del_custs = int(cust_per_update * 0.1)

    # Account table: 70% new, 20% change, 10% delete
    new_accts = int(acct_per_update * 0.7)
    change_accts = int(acct_per_update * 0.2)
    del_accts = int(acct_per_update * 0.1)

    # Per update: NEW = new_custs, ADDACCT = new_accts - new_custs
    # Each NEW creates one customer AND one account, so the remaining new_accts are ADDACCTs
    # (additional accounts for already-existing customers).
    addaccts_per_update = new_accts - new_custs
    rows_per_update = new_accts + change_accts + del_accts + change_custs + del_custs

    # Historical (update 0) + regular updates
    # CMHistoricalSize and CMUpdateLastID from the scaling formulas.
    # We derive update_last_id such that hist_size + update_last_id * rows_per_update = cm_final.
    cm_final = cfg.cm_final_row_count
    update_last_id = (cm_final - new_accts) // rows_per_update  # derived to match total
    hist_size = cm_final - update_last_id * rows_per_update
    total = hist_size + update_last_id * rows_per_update

    log(f"[CustomerMgmt] params: hist={hist_size}, updates=1..{update_last_id}, rows/update={rows_per_update}", "DEBUG")
    log(f"[CustomerMgmt] Per update: NEW={new_custs}, ADDACCT={addaccts_per_update}, UPDACCT={change_accts}, "
        f"CLOSEACCT={del_accts}, UPDCUST={change_custs}, INACT={del_custs}", "DEBUG")

    # Timestamp range: the entire CustomerMgmt timeline is divided into equal-width
    # windows, one per update. Within each window, actions are spaced evenly.
    cm_begin_s = int(CM_BEGIN_DATE.timestamp())
    cm_range_s = int((CM_END_DATE - CM_BEGIN_DATE).total_seconds())
    secs_per_update = cm_range_s // (update_last_id + 1)

    # === Build DataFrame: one row per action ===
    # Each row knows its update_id and position within that update.
    # Update 0: rows 0..hist_size-1 (all NEW)
    # Update k (k>=1): rows hist_size + (k-1)*rows_per_update .. hist_size + k*rows_per_update - 1

    all_df = spark.range(0, total).withColumnRenamed("id", "global_seq")

    # Determine update_id and position within update.
    # Update 0 contains the historical batch (all NEWs), updates 1+ are regular CDC updates.
    all_df = (all_df
        .withColumn("update_id",
            F.when(F.col("global_seq") < F.lit(hist_size), F.lit(0))
             .otherwise(((F.col("global_seq") - F.lit(hist_size)) / F.lit(rows_per_update) + 1).cast("int")))
        .withColumn("pos_in_update",
            F.when(F.col("update_id") == 0, F.col("global_seq"))
             .otherwise((F.col("global_seq") - F.lit(hist_size)) % F.lit(rows_per_update)))
    )

    # Permute position within each update using row_number with random sort (true bijection).
    # This guarantees each position 0..rows_per_update-1 is assigned exactly once per update.
    # The shuffle randomizes which action types land on which rows, while preserving
    # the exact count of each action type per update. Update 0 is not shuffled because
    # all its rows are NEW actions (shuffling NEWs among themselves has no effect).
    all_df = all_df.withColumn("_rand_sort", hash_key(F.col("global_seq"), seed_for("CM", "perm")))
    all_df = all_df.withColumn("shuffled_pos",
        F.when(F.col("update_id") == 0, F.col("pos_in_update"))  # historical: no shuffle, all NEW
         .otherwise(
            F.row_number().over(
                Window.partitionBy("update_id").orderBy("_rand_sort")) - 1))

    # === Assign action type based on shuffled position within each update ===
    # The position ranges within each update map to action types in this fixed order,
    # matching DIGen's CustomerAccountBlackBox:
    #   [0 .. new_custs)                                        -> NEW
    #   [new_custs .. new_accts)                                -> ADDACCT
    #   [new_accts .. new_accts+change_accts)                   -> UPDACCT
    #   [new_accts+change_accts .. +del_accts)                  -> CLOSEACCT
    #   [... +del_accts .. +change_custs)                       -> UPDCUST
    #   [... +change_custs .. +del_custs)                       -> INACT
    all_df = all_df.withColumn("ActionType",
        F.when(F.col("update_id") == 0, F.lit("NEW"))  # historical: all NEW
         .when(F.col("shuffled_pos") < F.lit(new_custs), F.lit("NEW"))
         .when(F.col("shuffled_pos") < F.lit(new_accts), F.lit("ADDACCT"))
         .when(F.col("shuffled_pos") < F.lit(new_accts + change_accts), F.lit("UPDACCT"))
         .when(F.col("shuffled_pos") < F.lit(new_accts + change_accts + del_accts), F.lit("CLOSEACCT"))
         .when(F.col("shuffled_pos") < F.lit(new_accts + change_accts + del_accts + change_custs), F.lit("UPDCUST"))
         .otherwise(F.lit("INACT")))

    # === Assign C_ID ===
    # NEW actions get sequentially allocated C_IDs. The allocation is cumulative:
    #   - Update 0 (historical): C_IDs 0 .. hist_size-1
    #   - Update k (k>=1): C_IDs hist_size + (k-1)*new_custs .. hist_size + k*new_custs - 1
    # Non-NEW actions reference a C_ID from a PRIOR update (update_id - 1 or earlier)
    # using a deterministic hash modulo the count of C_IDs created so far.
    new_window = Window.partitionBy("update_id").orderBy("global_seq")

    # Count NEW actions before this row within same update to get the NEW index
    all_df = all_df.withColumn("_new_idx_in_update",
        F.when(F.col("ActionType") == "NEW",
            F.row_number().over(
                Window.partitionBy("update_id", F.when(F.col("ActionType") == "NEW", F.lit(True)))
                .orderBy("global_seq")) - 1)
         .otherwise(F.lit(-1)))

    # C_ID for NEW: sequential based on cumulative count across updates
    all_df = all_df.withColumn("C_ID",
        F.when((F.col("ActionType") == "NEW") & (F.col("update_id") == 0),
            F.col("_new_idx_in_update"))  # historical: 0..hist_size-1
         .when(F.col("ActionType") == "NEW",
            F.lit(hist_size) + (F.col("update_id") - 1) * F.lit(new_custs) + F.col("_new_idx_in_update"))
         # Non-NEW: reference a previously-created C_ID.
         # The modulus ensures we only pick from C_IDs that existed BEFORE this update,
         # enforcing the "entity must exist before it can be referenced" invariant.
         .otherwise(
            hash_key(F.col("global_seq"), seed_for("CM", "ref_cid")) %
                F.greatest(F.lit(1), F.lit(hist_size) + (F.col("update_id") - 1) * F.lit(new_custs))))  # only ref C_IDs from PRIOR updates

    # === Assign CA_ID ===
    # NEW and ADDACCT get sequentially allocated CA_IDs. The allocation is cumulative:
    #   - Update 0: CA_IDs 0 .. hist_size-1 (one per NEW)
    #   - Update k: CA_IDs hist_size + (k-1)*new_accts .. hist_size + k*new_accts - 1
    #     (first new_custs are for NEW actions, next addaccts_per_update are for ADDACCT)
    # UPDACCT and CLOSEACCT reference existing CA_IDs from prior updates via hash.
    # UPDCUST and INACT have CA_ID = -1 (no account involvement).
    all_df = all_df.withColumn("_acct_idx_in_update",
        F.when(F.col("ActionType").isin("NEW", "ADDACCT"),
            F.row_number().over(
                Window.partitionBy("update_id", F.when(F.col("ActionType").isin("NEW", "ADDACCT"), F.lit(True)))
                .orderBy("global_seq")) - 1)
         .otherwise(F.lit(-1)))

    all_df = all_df.withColumn("CA_ID",
        F.when((F.col("ActionType").isin("NEW", "ADDACCT")) & (F.col("update_id") == 0),
            F.col("_acct_idx_in_update"))  # historical
         .when(F.col("ActionType").isin("NEW", "ADDACCT"),
            F.lit(hist_size) + (F.col("update_id") - 1) * F.lit(new_accts) + F.col("_acct_idx_in_update"))
         .otherwise(F.lit(-1)))  # Placeholder for UPDACCT/CLOSEACCT (set below), -1 for UPDCUST/INACT

    # For ADDACCT: C_ID should be an existing customer (not the new one being created).
    # Override the default C_ID with a hash-based reference to a prior customer.
    all_df = all_df.withColumn("C_ID",
        F.when(F.col("ActionType") == "ADDACCT",
            hash_key(F.col("global_seq"), seed_for("CM", "addacct_cref")) %
                F.greatest(F.lit(1), F.lit(hist_size) + (F.col("update_id") - 1) * F.lit(new_custs)))  # ref existing customers from PRIOR updates
         .otherwise(F.col("C_ID")))

    # --- UPDACCT/CLOSEACCT: derive CA_ID from the customer's first account ---
    # Instead of picking a random CA_ID from the global pool (which creates broken
    # customer-account pairings), derive the CA_ID from the C_ID that's already
    # assigned. Each customer's first account was created by the NEW action:
    #   - Historical customer (C_ID < hist_size): CA_ID = C_ID
    #   - Customer from update k: CA_ID = hist_size + (k-1)*new_accts + offset_in_update
    # This guarantees referential integrity: the account belongs to the customer.
    _cid_from_hist = F.col("C_ID") < F.lit(hist_size)
    _update_of_cid = ((F.col("C_ID") - F.lit(hist_size)) / F.lit(new_custs)).cast("int")
    _offset_in_update = (F.col("C_ID") - F.lit(hist_size)) % F.lit(new_custs)
    _first_acct = F.when(_cid_from_hist, F.col("C_ID")).otherwise(
        F.lit(hist_size) + _update_of_cid * F.lit(new_accts) + _offset_in_update)

    all_df = all_df.withColumn("CA_ID",
        F.when(F.col("ActionType").isin("UPDACCT", "CLOSEACCT"), _first_acct)
         .otherwise(F.col("CA_ID")))

    # --- INACT filtering ---
    # The downstream DimAccount pipeline does a date-range join with DimCustomer.
    # When a customer is INACT'd, their DimCustomer record ends. Any subsequent
    # account action for that customer creates an SCD record with no overlapping
    # DimCustomer record, producing null sk_customerid. DIGen's state machine never
    # generates account actions for INACT'd customers. We filter them out.
    inact_map = (all_df
        .filter(F.col("ActionType") == "INACT")
        .groupBy("C_ID")
        .agg(F.min("update_id").alias("_inact_at"))
        .select(F.col("C_ID").alias("_inact_cid"), "_inact_at"))

    all_df = (all_df
        .join(F.broadcast(inact_map),
              F.col("ActionType").isin("UPDACCT", "CLOSEACCT", "ADDACCT") &
              (F.col("C_ID") == F.col("_inact_cid")),
              "left")
        # ADDACCT: filter if customer INACT'd in same or prior update (<=).
        # No point creating a new account for a customer being deactivated.
        # UPDACCT/CLOSEACCT: filter only if INACT'd in prior update (<).
        # Same-update UPDACCT is valid (happens before INACT in sort order).
        .filter(~(
            (F.col("ActionType") == "ADDACCT") &
            F.col("_inact_at").isNotNull() &
            (F.col("_inact_at") <= F.col("update_id"))))
        .filter(~(
            F.col("ActionType").isin("UPDACCT", "CLOSEACCT") &
            F.col("_inact_at").isNotNull() &
            (F.col("_inact_at") < F.col("update_id"))))
        .drop("_inact_cid", "_inact_at"))

    # === Assign timestamp ===
    # Each update gets a time window of secs_per_update seconds. Within each update:
    #   1. NEWs come first (ordered by C_ID) — ensures Rule 1 and Rule 5
    #   2. ADDACCTs next (ordered by CA_ID) — ensures CA_ID created after customer
    #   3. Then UPDACCT, CLOSEACCT, UPDCUST, INACT (any order)
    # This ordering is enforced via a composite sort key that the timestamp is derived from.
    # The sort key encodes: update_id (millions), action_order (hundred-thousands),
    # and a sub-sort within each action type (entity ID or positional index).
    action_order = (
        F.when(F.col("ActionType") == "NEW", F.lit(0))
         .when(F.col("ActionType") == "ADDACCT", F.lit(1))
         .when(F.col("ActionType") == "UPDACCT", F.lit(2))
         .when(F.col("ActionType") == "CLOSEACCT", F.lit(3))
         .when(F.col("ActionType") == "UPDCUST", F.lit(4))
         .otherwise(F.lit(5)))  # INACT

    all_df = all_df.withColumn("_sort_key",
        F.col("update_id").cast("long") * 1000000 + action_order * 100000 +
        F.when(F.col("ActionType") == "NEW", F.col("C_ID") % 100000)
         .when(F.col("ActionType") == "ADDACCT", F.col("CA_ID") % 100000)
         .otherwise(F.col("pos_in_update")))

    # Compute sequential position within update based on the sort key.
    # This ordered position is then used to derive evenly-spaced timestamps
    # within the update's time window.
    all_df = all_df.withColumn("_ordered_pos",
        F.row_number().over(Window.partitionBy("update_id").orderBy("_sort_key")) - 1)

    all_df = (all_df
        .withColumn("_update_base_ts", F.lit(cm_begin_s).cast("long") + F.col("update_id").cast("long") * F.lit(secs_per_update))
        .withColumn("_pos_offset",
            F.when(F.col("update_id") == 0,
                F.col("_ordered_pos").cast("long") * F.lit(secs_per_update) / F.lit(max(1, hist_size)))
             .otherwise(
                F.col("_ordered_pos").cast("long") * F.lit(secs_per_update) / F.lit(rows_per_update)))
        .withColumn("ActionTS", F.date_format(
            (F.col("_update_base_ts") + F.col("_pos_offset")).cast("timestamp"),
            "yyyy-MM-dd'T'HH:mm:ss"))
        .withColumn("C_ID_str", F.col("C_ID").cast("string"))
        .withColumn("CA_ID_str", F.col("CA_ID").cast("string"))
    )

    # === Add customer/account attributes via dictionary joins ===
    # Each attribute is derived deterministically from C_ID or CA_ID using hash_key
    # with domain-specific seeds. This ensures the same C_ID always produces the same
    # name, address, etc., regardless of which action references it.
    all_df = all_df.withColumn("C_TAX_ID", F.concat(
        F.lpad((hash_key(F.col("C_ID"), seed_for("CM", "t1")) % 999).cast("string"), 3, "0"), F.lit("-"),
        F.lpad((hash_key(F.col("C_ID"), seed_for("CM", "t2")) % 99).cast("string"), 2, "0"), F.lit("-"),
        F.lpad((hash_key(F.col("C_ID"), seed_for("CM", "t3")) % 9999).cast("string"), 4, "0")))
    all_df = all_df.withColumn("C_GNDR",
        F.when(hash_key(F.col("C_ID"), seed_for("CM", "gn")) % 100 < 5, F.lit(None).cast("string"))
        .otherwise(F.array([F.lit(g) for g in ["M","F","m","f"]])[(hash_key(F.col("C_ID"), seed_for("CM", "gndr")) % 4).cast("int")]))
    all_df = all_df.withColumn("C_TIER",
        F.when(hash_key(F.col("C_ID"), seed_for("CM", "tier")) % 100 < 50, F.lit("3"))
        .when(hash_key(F.col("C_ID"), seed_for("CM", "tier")) % 100 < 80, F.lit("2")).otherwise(F.lit("1")))
    all_df = all_df.withColumn("C_DOB", F.date_format(F.date_add(F.lit("1920-01-01"),
        (hash_key(F.col("C_ID"), seed_for("CM", "dob")) % 25000).cast("int")), "yyyy-MM-dd"))
    all_df = all_df.withColumn("C_M_NAME",
        F.when(hash_key(F.col("C_ID"), seed_for("CM", "mn")) % 100 < 25, F.lit(None).cast("string"))
        .otherwise(F.substring(F.lit("ABCDEFGHIJKLMNOPQRSTUVWXYZ"),
            (hash_key(F.col("C_ID"), seed_for("CM", "mi_v")) % 26 + 1).cast("int"), 1)))
    all_df = all_df.withColumn("C_ADLINE2",
        F.when(hash_key(F.col("C_ID"), seed_for("CM", "a2n")) % 100 < 90, F.lit(None).cast("string"))
        .otherwise(F.concat(F.lit("Apt. "), (hash_key(F.col("C_ID"), seed_for("CM", "apt")) % 999 + 1).cast("string"))))
    all_df = all_df.withColumn("C_CTRY",
        F.when(hash_key(F.col("C_ID"), seed_for("CM", "ctry")) % 100 < 80, F.lit("United States of America")).otherwise(F.lit("Canada")))
    # Phone 1: country code, area code, local, extension
    # Use null (not empty string) for empty fields so XML writer omits them
    all_df = all_df.withColumn("C_CTRY_1", F.lit("1"))
    all_df = all_df.withColumn("C_AREA_1", (hash_key(F.col("C_ID"), seed_for("CM", "ar1")) % 900 + 100).cast("string"))
    all_df = all_df.withColumn("C_LOCAL_1", F.concat(
        (hash_key(F.col("C_ID"), seed_for("CM", "l1a")) % 900 + 100).cast("string"), F.lit("-"),
        (hash_key(F.col("C_ID"), seed_for("CM", "l1b")) % 9000 + 1000).cast("string")))
    all_df = all_df.withColumn("C_EXT_1", F.lit(None).cast("string"))
    # Phone 2/3: null for initial NEW actions (populated via UPDCUST)
    all_df = all_df.withColumn("C_CTRY_2", F.lit(None).cast("string"))
    all_df = all_df.withColumn("C_AREA_2", F.lit(None).cast("string"))
    all_df = all_df.withColumn("C_LOCAL_2", F.lit(None).cast("string"))
    all_df = all_df.withColumn("C_EXT_2", F.lit(None).cast("string"))
    all_df = all_df.withColumn("C_CTRY_3", F.lit(None).cast("string"))
    all_df = all_df.withColumn("C_AREA_3", F.lit(None).cast("string"))
    all_df = all_df.withColumn("C_LOCAL_3", F.lit(None).cast("string"))
    all_df = all_df.withColumn("C_EXT_3", F.lit(None).cast("string"))
    all_df = all_df.withColumn("CA_TAX_ST",
        F.when(F.col("CA_ID") < 0, F.lit(None).cast("string"))
        .otherwise(F.when(hash_key(F.col("CA_ID"), seed_for("CM", "ct")) % 100 < 70, F.lit("1"))
        .when(hash_key(F.col("CA_ID"), seed_for("CM", "ct")) % 100 < 90, F.lit("2")).otherwise(F.lit("0"))))
    all_df = all_df.withColumn("CA_NAME",
        F.when(F.col("CA_ID") < 0, F.lit(None).cast("string"))
        .otherwise(F.when(hash_key(F.col("CA_ID"), seed_for("CM", "can")) % 100 < 5, F.lit(None).cast("string"))
        .otherwise(F.substring(F.md5(F.concat(F.col("CA_ID").cast("string"), F.lit("ca"))), 1, 30))))

    # Dictionary-based attribute lookups: batch all lookups into fewer joins.
    # Using dict_join_batch computes all join keys in one pass, then performs one
    # join per unique dictionary instead of 10 sequential broadcast joins.
    all_df = dict_join_batch(all_df, [
        ("hr_family_names", hash_key(F.col("C_ID"), seed_for("CM", "ln")), "C_L_NAME"),
        ("hr_given_names",  hash_key(F.col("C_ID"), seed_for("CM", "fn")), "C_F_NAME"),
        ("address_lines",   hash_key(F.col("C_ID"), seed_for("CM", "a1")), "C_ADLINE1"),
        ("zip_codes",       hash_key(F.col("C_ID"), seed_for("CM", "zip")), "C_ZIPCODE"),
        ("cities",          hash_key(F.col("C_ID"), seed_for("CM", "city")), "C_CITY"),
        ("provinces",       hash_key(F.col("C_ID"), seed_for("CM", "st")), "C_STATE_PROV"),
        ("mail_providers",  hash_key(F.col("C_ID"), seed_for("CM", "em1")), "_mp1"),
        ("mail_providers",  hash_key(F.col("C_ID"), seed_for("CM", "em2")), "_mp2"),
        ("taxrate_ids",     hash_key(F.col("C_ID"), seed_for("CM", "lt")), "C_LCL_TX_ID"),
        ("taxrate_ids",     hash_key(F.col("C_ID"), seed_for("CM", "nt")), "C_NAT_TX_ID"),
    ])

    # Broker assignment: each account is assigned a broker deterministically from the CA_ID.
    all_df = all_df.withColumn("_broker_idx", hash_key(F.col("CA_ID"), seed_for("CM", "br")) % broker_count)
    all_df = all_df.join(
        F.broadcast(brokers_df.select(F.col("_idx").alias("_broker_idx"), F.col("broker_id").alias("CA_B_ID"))),
        on="_broker_idx", how="left")

    # Email addresses: primary is always populated; alternate has ~50% chance of being empty.
    all_df = (all_df
        .withColumn("C_PRIM_EMAIL", F.concat(F.col("C_F_NAME"), F.lit("."),
            F.when(F.length(F.col("C_M_NAME")) > 0, F.concat(F.col("C_M_NAME"), F.lit("."))).otherwise(F.lit("")),
            F.col("C_L_NAME"), F.lit("@"), F.col("_mp1")))
        .withColumn("C_ALT_EMAIL",
            F.when(hash_key(F.col("C_ID"), seed_for("CM", "em2n")) % 100 < 50, F.lit(""))
            .otherwise(F.concat(F.col("C_F_NAME"), F.lit("."), F.col("C_L_NAME"), F.lit("@"), F.col("_mp2")))))

    # === UPDCUST-specific values ===
    # UPDCUST sparse fields must represent actual CHANGES, not repeat the original values.
    # Generate different values using global_seq (unique per action) instead of C_ID
    # (same for all actions on the same customer). This ensures that when a field is
    # populated in UPDCUST, the value differs from the customer's current attributes.
    all_df = dict_join_batch(all_df, [
        ("address_lines",  hash_key(F.col("global_seq"), seed_for("CM", "upd_a1v")), "_upd_ADLINE1"),
        ("zip_codes",      hash_key(F.col("global_seq"), seed_for("CM", "upd_zipv")), "_upd_ZIPCODE"),
        ("cities",         hash_key(F.col("global_seq"), seed_for("CM", "upd_cityv")), "_upd_CITY"),
        ("provinces",      hash_key(F.col("global_seq"), seed_for("CM", "upd_stv")), "_upd_STATE_PROV"),
        ("mail_providers", hash_key(F.col("global_seq"), seed_for("CM", "upd_em1v")), "_upd_mp1"),
        ("mail_providers", hash_key(F.col("global_seq"), seed_for("CM", "upd_em2v")), "_upd_mp2"),
    ])
    all_df = all_df.withColumn("_upd_ADLINE2",
        F.when(hash_key(F.col("global_seq"), seed_for("CM", "upd_a2v")) % 100 < 90, F.lit(None).cast("string"))
        .otherwise(F.concat(F.lit("Apt. "), (hash_key(F.col("global_seq"), seed_for("CM", "upd_aptv")) % 999 + 1).cast("string"))))
    # Country: toggle from original to guarantee a change
    all_df = all_df.withColumn("_upd_CTRY",
        F.when(F.col("C_CTRY") == "United States of America", F.lit("Canada"))
         .otherwise(F.lit("United States of America")))
    all_df = (all_df
        .withColumn("_upd_PRIM_EMAIL", F.concat(F.col("C_F_NAME"), F.lit("."),
            F.when(F.length(F.col("C_M_NAME")) > 0, F.concat(F.col("C_M_NAME"), F.lit("."))).otherwise(F.lit("")),
            F.col("C_L_NAME"), F.lit("@"), F.col("_upd_mp1")))
        .withColumn("_upd_ALT_EMAIL",
            F.when(hash_key(F.col("global_seq"), seed_for("CM", "upd_em2nv")) % 100 < 50, F.lit(""))
            .otherwise(F.concat(F.col("C_F_NAME"), F.lit("."), F.col("C_L_NAME"), F.lit("@"), F.col("_upd_mp2")))))

    # === Build XML using pure Spark SQL ===
    # Helper to emit an XML element: populated if value is non-empty, self-closing otherwise.
    def _e(tag, col):
        """Emit ``<tag>value</tag>`` if non-empty, else ``<tag/>``."""
        return F.when(F.coalesce(F.col(col), F.lit("")) == "", F.lit(f"\t\t\t\t<{tag}/>")) \
                .otherwise(F.concat(F.lit(f"\t\t\t\t<{tag}>"), F.col(col), F.lit(f"</{tag}>")))

    # Phones 2 and 3 are always empty in the initial NEW action (only phone 1 is populated).
    # They may get values later via UPDCUST sparse updates.
    ph23 = ("\t\t\t\t<C_PHONE_2>\n\t\t\t\t\t<C_CTRY_CODE/>\n\t\t\t\t\t<C_AREA_CODE/>\n"
            "\t\t\t\t\t<C_LOCAL/>\n\t\t\t\t\t<C_EXT/>\n\t\t\t\t</C_PHONE_2>\n"
            "\t\t\t\t<C_PHONE_3>\n\t\t\t\t\t<C_CTRY_CODE/>\n\t\t\t\t\t<C_AREA_CODE/>\n"
            "\t\t\t\t\t<C_LOCAL/>\n\t\t\t\t\t<C_EXT/>\n\t\t\t\t</C_PHONE_3>\n")

    # XML fragments for each section of a full Customer element (used by NEW action):
    ah = F.concat(F.lit('\t<TPCDI:Action ActionType="'), F.col("ActionType"), F.lit('" ActionTS="'), F.col("ActionTS"), F.lit('">\n'))
    # coalesce nullable fields to empty string — a single null in F.concat kills the entire row
    cf = F.concat(F.lit('\t\t<Customer C_ID="'), F.col("C_ID_str"), F.lit('" C_TAX_ID="'), F.col("C_TAX_ID"),
        F.lit('" C_GNDR="'), F.coalesce(F.col("C_GNDR"), F.lit("")), F.lit('" C_TIER="'), F.col("C_TIER"), F.lit('" C_DOB="'), F.col("C_DOB"), F.lit('">\n'))
    nm = F.concat(F.lit("\t\t\t<Name>\n"), _e("C_L_NAME","C_L_NAME"), F.lit("\n"), _e("C_F_NAME","C_F_NAME"), F.lit("\n"), _e("C_M_NAME","C_M_NAME"), F.lit("\n\t\t\t</Name>\n"))
    ad = F.concat(F.lit("\t\t\t<Address>\n"), _e("C_ADLINE1","C_ADLINE1"), F.lit("\n"), _e("C_ADLINE2","C_ADLINE2"), F.lit("\n"), _e("C_ZIPCODE","C_ZIPCODE"), F.lit("\n"), _e("C_CITY","C_CITY"), F.lit("\n"), _e("C_STATE_PROV","C_STATE_PROV"), F.lit("\n"), _e("C_CTRY","C_CTRY"), F.lit("\n\t\t\t</Address>\n"))
    ci = F.concat(F.lit("\t\t\t<ContactInfo>\n"), _e("C_PRIM_EMAIL","C_PRIM_EMAIL"), F.lit("\n"), _e("C_ALT_EMAIL","C_ALT_EMAIL"), F.lit("\n"),
        F.lit("\t\t\t\t<C_PHONE_1>\n\t\t\t\t\t<C_CTRY_CODE>1</C_CTRY_CODE>\n\t\t\t\t\t<C_AREA_CODE>"), F.col("C_AREA_1"),
        F.lit("</C_AREA_CODE>\n\t\t\t\t\t<C_LOCAL>"), F.col("C_LOCAL_1"), F.lit("</C_LOCAL>\n\t\t\t\t\t<C_EXT/>\n\t\t\t\t</C_PHONE_1>\n"),
        F.lit(ph23), F.lit("\t\t\t</ContactInfo>\n"))
    tx = F.concat(F.lit("\t\t\t<TaxInfo>\n"), _e("C_LCL_TX_ID","C_LCL_TX_ID"), F.lit("\n"), _e("C_NAT_TX_ID","C_NAT_TX_ID"), F.lit("\n\t\t\t</TaxInfo>\n"))
    ac = F.concat(F.lit('\t\t\t<Account CA_ID="'), F.col("CA_ID_str"), F.lit('" CA_TAX_ST="'), F.coalesce(F.col("CA_TAX_ST"), F.lit("")), F.lit('">\n\t\t\t\t<CA_B_ID>'), F.coalesce(F.col("CA_B_ID"), F.lit("")), F.lit('</CA_B_ID>\n'), _e("CA_NAME","CA_NAME"), F.lit("\n\t\t\t</Account>\n"))
    ft = F.lit("\t</TPCDI:Action>")

    # === UPDCUST: sparse update matching DIGen pattern ===
    # DIGen always includes Address + ContactInfo container blocks in UPDCUST,
    # but each individual field within those blocks independently has a ~50% chance
    # of being populated (CMFieldUpdatePct = 50 from DIGen configuration).
    # Unpopulated fields appear as empty self-closing XML elements (e.g. ``<C_CITY/>``),
    # signaling to the downstream ETL that those fields were NOT changed and should
    # retain their previous values. This is the "sparse update" pattern.
    # Customer-level attributes (C_TIER, C_GNDR, C_DOB) rarely appear in updates
    # (~20% for C_TIER, omitted for C_GNDR and C_DOB in UPDCUST).

    # Address block with sparse fields (~50% chance each field appears)
    def _sparse(tag, col, seq, field_seed):
        """Include field with ~50% probability, otherwise emit empty element.

        This implements the sparse update pattern for UPDCUST actions. Each field
        is independently toggled: if ``hash_key(seq, field_seed) % 100 < 50``,
        the field is "changed" and its value is emitted; otherwise the field is
        "unchanged" and an empty element ``<tag/>`` is emitted.
        """
        return F.when(hash_key(seq, field_seed) % 100 < 50, _e(tag, col)) \
                .otherwise(F.lit(f"\t\t\t\t<{tag}/>"))

    gs = F.col("global_seq")
    # UPDCUST address uses _upd_* columns (different values from NEW) so updates
    # represent actual changes to the customer's address, not repeats of originals.
    ad_upd = F.concat(F.lit("\t\t\t<Address>\n"),
        _sparse("C_ADLINE1", "_upd_ADLINE1", gs, seed_for("CM", "upd_a1")), F.lit("\n"),
        _sparse("C_ADLINE2", "_upd_ADLINE2", gs, seed_for("CM", "upd_a2")), F.lit("\n"),
        _sparse("C_ZIPCODE", "_upd_ZIPCODE", gs, seed_for("CM", "upd_zip")), F.lit("\n"),
        _sparse("C_CITY", "_upd_CITY", gs, seed_for("CM", "upd_city")), F.lit("\n"),
        _sparse("C_STATE_PROV", "_upd_STATE_PROV", gs, seed_for("CM", "upd_st")), F.lit("\n"),
        _sparse("C_CTRY", "_upd_CTRY", gs, seed_for("CM", "upd_ctry")), F.lit("\n"),
        F.lit("\t\t\t</Address>\n"))

    # ContactInfo block with sparse fields and phone blocks matching DIGen patterns:
    # PHONE_1: ~97% has data, PHONE_2: ~74%, PHONE_3: ~30%
    # Values: C_CTRY_CODE="1" (~50% when phone has data), C_AREA_CODE=3-digit (~50%),
    #         C_LOCAL="NNN-NNNN" (always when phone has data), C_EXT=5-digit (~10%)
    # Each phone uses DIFFERENT values (not all sharing C_AREA_1/C_LOCAL_1)
    def _phone_upd(n, gs, data_pct):
        """Generate a sparse phone block for UPDCUST with per-phone independent values.

        Args:
            n: Phone number (1, 2, or 3).
            gs: Column expression for global_seq (used as hash input for determinism).
            data_pct: Probability (0-100) that this phone has any data at all.
                Phone 1: 97%, Phone 2: 74%, Phone 3: 30% (matching DIGen distributions).

        Within a populated phone block, each sub-field is independently toggled:
          - C_CTRY_CODE: 50% chance of "1", else empty
          - C_AREA_CODE: 50% chance of 3-digit code, else empty
          - C_LOCAL: always populated when phone has data (NNN-NNNN format)
          - C_EXT: 10% chance of 5-digit extension, else empty
        """
        has_data = hash_key(gs, seed_for("CM", f"upd_ph{n}")) % 100 < data_pct
        # Generate per-phone values using different seeds
        area = (hash_key(gs, seed_for("CM", f"upd_ph{n}_area")) % 900 + 100).cast("string")
        local = F.concat(
            (hash_key(gs, seed_for("CM", f"upd_ph{n}_loc1")) % 900 + 100).cast("string"),
            F.lit("-"),
            (hash_key(gs, seed_for("CM", f"upd_ph{n}_loc2")) % 9000 + 1000).cast("string"))
        ext = F.lpad((hash_key(gs, seed_for("CM", f"upd_ph{n}_ext")) % 99999 + 1).cast("string"), 5, "0")
        # Area code must be present when country code is present, otherwise the
        # pipeline produces "+1 NNN-NNNN" which fails phone format validation.
        _has_area = has_data & (hash_key(gs, seed_for("CM", f"upd_ph{n}a")) % 100 < 50)
        _has_ctry = _has_area & (hash_key(gs, seed_for("CM", f"upd_ph{n}c")) % 100 < 50)
        return F.concat(
            F.lit(f"\t\t\t\t<C_PHONE_{n}>\n"),
            F.when(_has_ctry,
                F.lit("\t\t\t\t\t<C_CTRY_CODE>1</C_CTRY_CODE>\n")).otherwise(F.lit("\t\t\t\t\t<C_CTRY_CODE/>\n")),
            F.when(_has_area,
                F.concat(F.lit("\t\t\t\t\t<C_AREA_CODE>"), area, F.lit("</C_AREA_CODE>\n"))).otherwise(F.lit("\t\t\t\t\t<C_AREA_CODE/>\n")),
            F.when(has_data,
                F.concat(F.lit("\t\t\t\t\t<C_LOCAL>"), local, F.lit("</C_LOCAL>\n"))).otherwise(F.lit("\t\t\t\t\t<C_LOCAL/>\n")),
            F.when(has_data & (hash_key(gs, seed_for("CM", f"upd_ph{n}e")) % 100 < 10),
                F.concat(F.lit("\t\t\t\t\t<C_EXT>"), ext, F.lit("</C_EXT>\n"))).otherwise(F.lit("\t\t\t\t\t<C_EXT/>\n")),
            F.lit(f"\t\t\t\t</C_PHONE_{n}>\n"))

    ci_upd = F.concat(F.lit("\t\t\t<ContactInfo>\n"),
        _sparse("C_PRIM_EMAIL", "_upd_PRIM_EMAIL", gs, seed_for("CM", "upd_em1")), F.lit("\n"),
        _sparse("C_ALT_EMAIL", "_upd_ALT_EMAIL", gs, seed_for("CM", "upd_em2")), F.lit("\n"),
        _phone_upd(1, gs, 97),
        _phone_upd(2, gs, 74),
        _phone_upd(3, gs, 30),
        F.lit("\t\t\t</ContactInfo>\n"))

    # Optional Customer-level attributes (C_TIER appears ~20% of time on updates per DIGen).
    # Unlike NEW actions which always include C_TAX_ID, C_GNDR, C_TIER, C_DOB as XML
    # attributes, UPDCUST only conditionally includes C_TIER (~20%) and omits the others.
    cust_attrs_upd = F.concat(F.lit('\t\t<Customer C_ID="'), F.col("C_ID_str"),
        F.when(hash_key(gs, seed_for("CM", "upd_tier")) % 100 < 20,
            F.concat(F.lit('" C_TIER="'), F.col("C_TIER"))).otherwise(F.lit("")),
        F.lit('">\n'))

    updcust_body = F.concat(ah, cust_attrs_upd, ad_upd, ci_upd, F.lit("\t\t</Customer>\n"), ft)

    # === Assemble final XML body per action type ===
    # Each action type has a different XML structure:
    #   NEW:       Full Customer element with Name, Address, ContactInfo, TaxInfo, and Account
    #   ADDACCT:   Minimal Customer wrapper (C_ID only) with a full Account element
    #   UPDACCT:   Customer wrapper (C_ID) with Account element (updated broker/name/tax)
    #   CLOSEACCT: Customer wrapper (C_ID) with minimal Account element (CA_ID only)
    #   UPDCUST:   Customer wrapper (C_ID, optional C_TIER) with sparse Address + ContactInfo
    #   INACT:     Customer wrapper (C_ID only), no child elements
    xml_body = (
        F.when(F.col("ActionType") == "NEW",
            F.concat(ah, cf, nm, ad, ci, tx, ac, F.lit("\t\t</Customer>\n"), ft))
        .when(F.col("ActionType") == "ADDACCT",
            F.concat(ah, F.lit('\t\t<Customer C_ID="'), F.col("C_ID_str"), F.lit('" >\n'), ac, F.lit("\t\t</Customer>\n"), ft))
        .when(F.col("ActionType") == "CLOSEACCT",
            F.concat(ah, F.lit('\t\t<Customer C_ID="'), F.col("C_ID_str"), F.lit('">\n\t\t\t<Account CA_ID="'), F.col("CA_ID_str"), F.lit('" />\n\t\t</Customer>\n'), ft))
        .when(F.col("ActionType") == "INACT",
            F.concat(ah, F.lit('\t\t<Customer C_ID="'), F.col("C_ID_str"), F.lit('">\n\t\t</Customer>\n'), ft))
        .when(F.col("ActionType") == "UPDCUST", updcust_body)
        .when(F.col("ActionType") == "UPDACCT",
            F.concat(ah, F.lit('\t\t<Customer C_ID="'), F.col("C_ID_str"), F.lit('">\n'), ac, F.lit("\t\t</Customer>\n"), ft))
    )

    # === Deduplicate: max 1 event per entity per calendar day ===
    # The TPC-DI spec requires at most one CDC event per entity per day. The source
    # system extracts only the end-of-day state, so two events on the same day for
    # the same customer or account should never occur. This prevents duplicate SKs
    # in DimCustomer/DimAccount (SK = concat(yyyyMMdd, entityid)).
    all_df = all_df.withColumn("_day", F.substring(F.col("ActionTS"), 1, 10))

    # --- Customer-level dedup: (C_ID, day) ---
    # Customer-modifying actions: NEW, UPDCUST, INACT. At most one per (C_ID, day).
    # ADDACCT/UPDACCT/CLOSEACCT don't modify the customer entity, so they're excluded.
    all_df = all_df.withColumn("_cust_dedup_key",
        F.when(F.col("ActionType").isin("NEW", "UPDCUST", "INACT"),
            F.concat(F.col("C_ID").cast("string"), F.lit("_"), F.col("_day")))
         .otherwise(F.lit(None)))
    all_df = all_df.withColumn("_cust_dedup_rank",
        F.when(F.col("_cust_dedup_key").isNotNull(),
            F.row_number().over(Window.partitionBy("_cust_dedup_key").orderBy("_sort_key")))
         .otherwise(F.lit(1)))

    # --- Account-level dedup: (CA_ID, day) ---
    # Account-modifying actions: NEW, ADDACCT, UPDACCT, CLOSEACCT. At most one per (CA_ID, day).
    # UPDCUST/INACT don't have a CA_ID (CA_ID=-1), so they're excluded.
    all_df = all_df.withColumn("_acct_dedup_key",
        F.when(F.col("CA_ID") >= 0,
            F.concat(F.col("CA_ID").cast("string"), F.lit("_"), F.col("_day")))
         .otherwise(F.lit(None)))
    all_df = all_df.withColumn("_acct_dedup_rank",
        F.when(F.col("_acct_dedup_key").isNotNull(),
            F.row_number().over(Window.partitionBy("_acct_dedup_key").orderBy("_sort_key")))
         .otherwise(F.lit(1)))

    # Keep only first occurrence per dedup key (both customer-level and account-level
    # dedup must pass for a row to be retained).
    all_df = all_df.filter((F.col("_cust_dedup_rank") == 1) & (F.col("_acct_dedup_rank") == 1))

    # === Filter out ADDACCTs on the same day as their customer's INACT ===
    # An ADDACCT and INACT for the same customer on the same day would create
    # a same-day conflict when we generate the synthetic CLOSEACCT. Remove the
    # ADDACCT — no point creating an account for a customer being deactivated.
    inact_days = (all_df
        .filter(F.col("ActionType") == "INACT")
        .select(F.col("C_ID").alias("_inact_cid"),
                F.substring(F.col("ActionTS"), 1, 10).alias("_inact_day")))
    all_df = all_df.withColumn("_day_tmp", F.substring(F.col("ActionTS"), 1, 10))
    all_df = (all_df
        .join(F.broadcast(inact_days),
              (F.col("ActionType") == "ADDACCT") &
              (F.col("C_ID") == F.col("_inact_cid")) &
              (F.col("_day_tmp") == F.col("_inact_day")),
              "left_anti")
        .drop("_day_tmp", "_inact_cid", "_inact_day"))

    # === Generate CLOSEACCT for ALL accounts of INACT'd customers ===
    # When a customer is deactivated (INACT), ALL their accounts must also become
    # inactive. If the CLOSEACCT would land on the same day as an existing account
    # action (e.g., a NEW from a prior update), shift it +1 day. The INACT's
    # DimCustomer record extends to 9999-12-31, so the +1 day is always within
    # the inactive window.
    inact_rows = all_df.filter(F.col("ActionType") == "INACT")
    acct_owners = (all_df
        .filter(F.col("ActionType").isin("NEW", "ADDACCT"))
        .select(F.col("C_ID").alias("_owner_cid"), F.col("CA_ID").alias("_acct_caid")))

    close_for_inact = (inact_rows
        .join(F.broadcast(acct_owners), F.col("C_ID") == F.col("_owner_cid"), "inner")
        .withColumn("ActionType", F.lit("CLOSEACCT"))
        .withColumn("CA_ID", F.col("_acct_caid"))
        .withColumn("CA_ID_str", F.col("CA_ID").cast("string"))
        .withColumn("status", F.lit("Inactive"))
        .withColumn("_close_day", F.substring(F.col("ActionTS"), 1, 10))
        .drop("_owner_cid", "_acct_caid"))

    # Check for same-day conflicts with existing account actions (e.g., NEW)
    existing_acct_days = (all_df
        .filter(F.col("CA_ID") >= 0)
        .select(F.col("CA_ID").alias("_ea_caid"),
                F.substring(F.col("ActionTS"), 1, 10).alias("_ea_day")))
    close_for_inact = (close_for_inact
        .join(F.broadcast(existing_acct_days),
              (F.col("CA_ID") == F.col("_ea_caid")) &
              (F.col("_close_day") == F.col("_ea_day")),
              "left")
        # Shift +1 day if there's a conflict; INACT DimCustomer enddate=9999-12-31
        .withColumn("ActionTS",
            F.when(F.col("_ea_caid").isNotNull(),
                F.date_format(F.date_add(F.to_timestamp(F.col("ActionTS")), 1), "yyyy-MM-dd'T'HH:mm:ss"))
             .otherwise(F.col("ActionTS")))
        .drop("_close_day", "_ea_caid", "_ea_day"))

    all_df = all_df.unionByName(close_for_inact, allowMissingColumns=True)

    # Repartition before caching so the cached data is evenly distributed.
    # All downstream operations (4 views + XML write) benefit from balanced partitions.
    # Target ~128MB per partition: ~15GB XML at SF=5000 → ~120 partitions.
    n_parts = max(1, int(cfg.cm_final_row_count * 600 / (128 * 1024 * 1024)))
    all_df = all_df.repartition(n_parts)

    # Cache all_df — it's used to derive 4 views + the XML write. Without caching,
    # each view's materialization re-evaluates the full plan (Windows, dedup, INACT
    # filtering, dictionary joins) from scratch. Caching once lets all downstream
    # operations read from memory.
    all_df = all_df.cache()
    all_df.count()  # materialize
    log(f"[CustomerMgmt] all_df cached ({n_parts} partitions)")

    # === Create _closed_accounts temp view ===
    # Cache and materialize views that Trade depends on so Trade reads instantly.
    # when it reads these views — essentially re-running CustomerMgmt.
    closed_accts = (all_df
        .filter(F.col("ActionType") == "CLOSEACCT")
        .select(F.col("CA_ID").alias("closed_ca_id"))
        .distinct()).cache()
    closed_accts.createOrReplaceTempView("_closed_accounts")
    n_closed = closed_accts.count()  # materializes cache
    log(f"[CustomerMgmt] {n_closed} closed accounts -> _closed_accounts view")

    _acct_creating = all_df.filter(F.col("ActionType").isin("NEW", "ADDACCT"))

    created_accts = _acct_creating.select(F.col("CA_ID").alias("created_ca_id")).cache()
    created_accts.createOrReplaceTempView("_created_accounts")
    n_created = created_accts.count()  # materializes cache
    log(f"[CustomerMgmt] {n_created} created accounts -> _created_accounts view")

    acct_owners = _acct_creating.select(
        F.col("CA_ID").cast("string").alias("ca_id"),
        F.col("C_ID").cast("string").alias("owner_cid")).cache()
    acct_owners.createOrReplaceTempView("_account_owners")
    acct_owners.count()  # materializes cache
    log(f"[CustomerMgmt] {n_created} account owners -> _account_owners view", "DEBUG")

    # === Create _customer_dates temp view ===
    # Track the lifecycle of each customer: when they were created (NEW) and when they
    # were inactivated (INACT, if ever). This is used by watch_history.py to ensure
    # watch items only reference customers during their active window.
    # For customers with multiple INACT actions (shouldn't happen after dedup, but
    # defensively), we take the earliest inactivation timestamp.
    cust_new = (all_df
        .filter(F.col("ActionType") == "NEW")
        .select(F.col("C_ID").alias("cust_id"),
                F.col("ActionTS").alias("cust_create_ts"),
                F.col("update_id").alias("cust_create_update")))
    cust_inact = (all_df
        .filter(F.col("ActionType") == "INACT")
        .groupBy(F.col("C_ID").alias("cust_id"))
        .agg(F.min("ActionTS").alias("cust_inact_ts")))
    cust_dates = cust_new.join(cust_inact, on="cust_id", how="left")
    cust_dates.createOrReplaceTempView("_customer_dates")
    n_cust = cust_dates.count()
    log(f"[CustomerMgmt] {n_cust} customers -> _customer_dates view", "DEBUG")

    # Signal that views are ready — Trade can now start while we continue
    # writing XML and incremental files.
    if views_ready_event is not None:
        views_ready_event.set()

    # === Write XML as compact text with per-partition header/footer ===
    # Uses the pre-built xml_body strings (compact, no indentation) and wraps
    # each partition with XML root tags via mapInPandas. This produces valid XML
    # files that are ~3x smaller than the native XML writer's indented output,
    # while keeping writes fully distributed (no coalesce).
    # Inherits balanced partitions from the repartitioned+cached all_df.
    xml_df = all_df.withColumn("xml_line", xml_body).select("xml_line")

    xml_header = '<?xml version="1.0" encoding="UTF-8"?>\n<TPCDI:Actions xmlns:TPCDI="http://www.tpc.org/tpc-di">'
    xml_footer = '</TPCDI:Actions>'

    import pandas as pd
    from pyspark.sql.types import StructType, StructField, StringType

    def wrap_xml_partition(iterator):
        """Wrap each partition's XML lines with root element tags."""
        yield pd.DataFrame({"xml_line": [xml_header]})
        for pdf in iterator:
            yield pdf
        yield pd.DataFrame({"xml_line": [xml_footer]})

    out_schema = StructType([StructField("xml_line", StringType())])
    wrapped_df = xml_df.mapInPandas(wrap_xml_partition, schema=out_schema)

    tmp_path = f"{cfg.batch_path(1)}/CustomerMgmt.xml__tmp"
    try:
        dbutils.fs.rm(tmp_path, recurse=True)
    except:
        pass

    wrapped_df.write.mode("overwrite").text(tmp_path)

    part_files = sorted(
        [f for f in dbutils.fs.ls(tmp_path) if f.name.startswith("part-")],
        key=lambda f: f.name)
    for i, pf in enumerate(part_files):
        register_copy(pf.path, f"{cfg.batch_path(1)}/CustomerMgmt_{i+1}.xml")

    total_new = hist_size + update_last_id * new_custs
    total_caids = hist_size + update_last_id * new_accts
    n_parts = len(part_files)
    log(f"[CustomerMgmt] CustomerMgmt.xml: {total} actions, {total_new} unique C_IDs, {total_caids} unique CA_IDs -> {n_parts} files")

    # Release all_df cache — views are cached independently and XML is written
    all_df.unpersist()

    return {("CustomerMgmt", 1): total}


def generate_incremental(spark, cfg, dicts, dbutils):
    """Generate incremental Customer.txt and Account.txt for Batch 2 and Batch 3.

    This function produces pipe-delimited CDC extract files that simulate incremental
    data feeds from the customer management system. Unlike the historical batch which
    uses CustomerMgmt.xml (sparse XML with a state machine), incremental batches use
    a simpler flat-file CDC format where every field is always present.

    Key differences from CustomerMgmt.xml:
      - **CDC extract format:** ALL fields present on every row (not sparse). The source
        system sends the complete current state of each changed row.
      - **Phone fields:** Individual pipe-delimited columns (c_ctry_1, c_area_1,
        c_local_1, c_ext_1, etc.) rather than nested XML phone elements.
      - **CDC_FLAG:** ``'I'`` for insert (new entity), ``'U'`` for update (changed entity).
      - **CDC_DSN:** Sequential data sequence number that continues monotonically from
        the historical batch, ensuring the ETL can determine ordering across batches.
      - **C_ID continuity:** Insert C_IDs continue from ``n_hist_customers`` (the total
        customers created by the historical batch). Update C_IDs reference any previously
        existing customer via hash modulo.
      - **CA_ID continuity:** Insert CA_IDs continue from ``n_hist_accounts``. Update
        CA_IDs reference any previously existing account via hash modulo.
      - **Status fields:** Inserts are always ``ACTV``; updates use a 67%/33% split
        between ``ACTV`` and ``INAC``, yielding ~90% active overall (70% inserts all
        active + 30% updates with 67% active = 90.1%).

    Args:
        spark: Active SparkSession.
        cfg: Configuration object with scale factor and path information.
        dicts: Dictionary of reference data (names, addresses, etc.).
        dbutils: Databricks utilities for file operations.

    Returns:
        dict: Mapping of (file_name, batch_id) -> row count for each generated file.
    """
    brokers_df = spark.table("_brokers")
    broker_count = brokers_df.count()

    internal_sf = cfg.internal_sf
    cust_per_update = int(0.005 * internal_sf)
    acct_per_update = int(0.01 * internal_sf)
    new_custs = int(cust_per_update * 0.7)
    new_accts = int(acct_per_update * 0.7)

    # Historical sizing (matching generate_customermgmt) — we need these to compute
    # the ID and DSN continuation points for incremental batches.
    rows_per_update = new_accts + int(acct_per_update * 0.2) + int(acct_per_update * 0.1) + int(cust_per_update * 0.2) + int(cust_per_update * 0.1)
    cm_final = cfg.cm_final_row_count
    update_last_id = (cm_final - new_accts) // rows_per_update
    hist_size = cm_final - update_last_id * rows_per_update

    # Historical totals for ID continuation: new incremental IDs start after these.
    n_hist_customers = hist_size + update_last_id * new_custs
    n_hist_accounts = hist_size + update_last_id * new_accts

    # CDC_DSN bases (continues from historical sequence).
    # The DSN is a monotonically increasing sequence number that spans across all
    # batches, allowing the ETL to determine the global ordering of CDC events.
    cust_dsn_base = hist_size + update_last_id * cust_per_update
    acct_dsn_base = hist_size + update_last_id * acct_per_update

    counts = {}
    for batch_id in range(2, NUM_INCREMENTAL_BATCHES + 2):
        bp = cfg.batch_path(batch_id)
        bs = seed_for(f"B{batch_id}", "inc")
        batch_offset = batch_id - 2  # 0 for Batch2, 1 for Batch3

        # === Customer.txt ===
        # 70% inserts (new C_IDs continuing from historical max), 30% updates
        # (referencing existing C_IDs via hash). Each row has ALL fields populated
        # (no sparse updates — this is a full-row CDC extract).
        n_cust_insert = new_custs  # same as per-update new customer count
        n_cust_update = cust_per_update - n_cust_insert

        cust_df = (spark.range(0, cust_per_update).withColumnRenamed("id", "rid")
            .withColumn("_is_insert", F.col("rid") < F.lit(n_cust_insert))
            .withColumn("cdc_flag", F.when(F.col("_is_insert"), F.lit("I")).otherwise(F.lit("U")))
            # CDC_DSN: sequential, continuing from where the historical batch left off.
            # Batch2 starts at cust_dsn_base, Batch3 continues from Batch2's end.
            .withColumn("cdc_dsn", (F.lit(cust_dsn_base) + F.lit(batch_offset * cust_per_update) + F.col("rid")).cast("string"))
            # C_ID: inserts get new sequential IDs, updates reference existing ones.
            .withColumn("c_id",
                F.when(F.col("_is_insert"),
                    (F.lit(n_hist_customers) + F.lit(batch_offset * n_cust_insert) + F.col("rid")).cast("string"))
                .otherwise(
                    (hash_key(F.col("rid"), bs + 50) % F.lit(n_hist_customers + batch_offset * n_cust_insert)).cast("string")))
            .withColumn("c_tax_id", F.concat(
                F.lpad((hash_key(F.col("rid"), bs+1) % 999).cast("string"), 3, "0"), F.lit("-"),
                F.lpad((hash_key(F.col("rid"), bs+2) % 99).cast("string"), 2, "0"), F.lit("-"),
                F.lpad((hash_key(F.col("rid"), bs+3) % 9999).cast("string"), 4, "0")))
            # c_st_id: inserts=ACTV, updates use the UpdateActionType pattern
            # In DIGen: NEW/CHANGE->ACTV, DELETE->INAC. For incremental updates,
            # ~67% remain ACTV and ~33% become INAC (matching DIGen's observed 90%/10% overall
            # since 70% inserts are all ACTV + 30% updates with 67%/33% split).
            # _candidate_inac marks updates that WOULD be INAC — actual INAC assignment
            # is deferred until after c_id is computed, so we can verify the customer has accounts.
            .withColumn("_candidate_inac",
                (~F.col("_is_insert")) & (hash_key(F.col("rid"), bs+4) % 100 >= 67))
            .withColumn("c_st_id", F.when(F.col("_is_insert"), F.lit("ACTV"))
                .when(F.col("_candidate_inac"), F.lit("INAC"))
                .otherwise(F.lit("ACTV")))
            .withColumn("c_m_name", F.when(hash_key(F.col("rid"), bs+7) % 100 < 25, F.lit("")).otherwise(
                F.substring(F.lit("ABCDEFGHIJKLMNOPQRSTUVWXYZ"), (hash_key(F.col("rid"), bs+70) % 26 + 1).cast("int"), 1)))
            # c_gndr: DIGen overall ~52.6% empty across inserts+updates.
            # NullGenerator(PERCENT_NULL=5%) wraps ProbabilityGenerator(98% mMfF, 2% error).
            # But the 52% empty comes from the CustomerMgmt XML sparse update pattern where
            # gender is only populated ~48% of the time. The CDC extract carries forward these values.
            # For simplicity: 52% empty, then 98% of rest = mMfF, 2% = error char
            .withColumn("c_gndr",
                F.when(hash_key(F.col("rid"), bs+8) % 1000 < 526, F.lit(""))
                 .when(hash_key(F.col("rid"), bs+8) % 1000 < 990,
                    F.array(*[F.lit(g) for g in ["M","F","m","f"]])[(hash_key(F.col("rid"), bs+71) % 4).cast("int")])
                 .otherwise(F.substring(F.lit("ABCDEGHIJKLNOPQRSTUVWXYZabcdeghijklnopqrstuvwxyz"),
                    (hash_key(F.col("rid"), bs+72) % 48 + 1).cast("int"), 1)))
            # c_tier: DIGen uses NullGenerator(5%) then ProbabilityGenerator
            # tier 1=~10%, tier 2=~30%, tier 3=~60% (after NULL removal)
            # Combined: 5% NULL, ~9.5% tier1, ~28.5% tier2, ~57% tier3
            .withColumn("c_tier",
                F.when(hash_key(F.col("rid"), bs+9) % 1000 < 50, F.lit(None))
                 .when(hash_key(F.col("rid"), bs+9) % 1000 < 145, F.lit("1"))
                 .when(hash_key(F.col("rid"), bs+9) % 1000 < 430, F.lit("2"))
                 .otherwise(F.lit("3")))
            .withColumn("c_dob", F.date_format(F.date_add(F.lit("1920-01-01"), (hash_key(F.col("rid"), bs+10) % 25000).cast("int")), "yyyy-MM-dd"))
            .withColumn("c_adline2", F.when(hash_key(F.col("rid"), bs+12) % 100 < 90, F.lit("")).otherwise(
                F.concat(F.lit("Suite "), (hash_key(F.col("rid"), bs+13) % 999).cast("string"))))
            # c_ctry: DIGen uses 80/20 USA/Canada for NEW, NullGenerator(5%) + 80/20 for updates
            .withColumn("c_ctry",
                F.when(F.col("_is_insert"),
                    F.when(hash_key(F.col("rid"), bs+17) % 100 < 80, F.lit("United States of America"))
                     .otherwise(F.lit("Canada")))
                .otherwise(
                    F.when(hash_key(F.col("rid"), bs+17) % 1000 < 50, F.lit(None))
                     .when(hash_key(F.col("rid"), bs+17) % 1000 < 810, F.lit("United States of America"))
                     .otherwise(F.lit("Canada"))))
            # Phone 1: country code requires area code (otherwise pipeline produces
            # "+1 NNN-NNNN" which fails phone format validation)
            .withColumn("c_area_1", F.when(hash_key(F.col("rid"), bs+31) % 100 < 50,
                (hash_key(F.col("rid"), bs+18) % 900 + 100).cast("string")).otherwise(F.lit("")))
            .withColumn("c_ctry_1", F.when(
                (F.col("c_area_1") != "") & (hash_key(F.col("rid"), bs+30) % 100 < 50),
                F.lit("1")).otherwise(F.lit("")))
            # Phone 1 local: ~97% populated (DIGen shows 97-98%)
            .withColumn("c_local_1",
                F.when(hash_key(F.col("rid"), bs+73) % 100 < 97,
                    F.concat((hash_key(F.col("rid"), bs+19) % 900 + 100).cast("string"), F.lit("-"),
                        (hash_key(F.col("rid"), bs+20) % 9000 + 1000).cast("string")))
                .otherwise(F.lit("")))
            .withColumn("c_ext_1", F.when(hash_key(F.col("rid"), bs+32) % 100 < 10,
                F.lpad((hash_key(F.col("rid"), bs+33) % 99999 + 1).cast("string"), 5, "0")).otherwise(F.lit("")))
            # Phone 2: sparser than phone 1 (~74% local populated)
            .withColumn("c_ctry_2", F.lit(""))
            .withColumn("c_area_2", F.when(hash_key(F.col("rid"), bs+34) % 100 < 40,
                (hash_key(F.col("rid"), bs+35) % 900 + 100).cast("string")).otherwise(F.lit("")))
            .withColumn("c_local_2", F.when(hash_key(F.col("rid"), bs+36) % 100 < 74,
                F.concat((hash_key(F.col("rid"), bs+37) % 900 + 100).cast("string"), F.lit("-"),
                    (hash_key(F.col("rid"), bs+38) % 9000 + 1000).cast("string"))).otherwise(F.lit("")))
            .withColumn("c_ext_2", F.lit(""))
            # Phone 3: sparsest (~30% local populated)
            .withColumn("c_ctry_3", F.lit(""))
            .withColumn("c_area_3", F.lit(""))
            .withColumn("c_local_3", F.when(hash_key(F.col("rid"), bs+39) % 100 < 30,
                F.concat((hash_key(F.col("rid"), bs+40) % 900 + 100).cast("string"), F.lit("-"),
                    (hash_key(F.col("rid"), bs+41) % 9000 + 1000).cast("string"))).otherwise(F.lit("")))
            .withColumn("c_ext_3", F.lit(""))
            # c_email_2: ~50% populated with a second email address
            .withColumn("c_email_2", F.lit(None).cast("string"))  # placeholder, filled after dict joins
        )
        # Dictionary-based attribute lookups for customer names, addresses, and tax rates.
        cust_df = dict_join(cust_df, "hr_family_names", hash_key(F.col("rid"), bs+5), "c_l_name")
        cust_df = dict_join(cust_df, "hr_given_names", hash_key(F.col("rid"), bs+6), "c_f_name")
        cust_df = dict_join(cust_df, "address_lines", hash_key(F.col("rid"), bs+11), "c_adline1")
        cust_df = dict_join(cust_df, "zip_codes", hash_key(F.col("rid"), bs+14), "c_zipcode")
        cust_df = dict_join(cust_df, "cities", hash_key(F.col("rid"), bs+15), "c_city")
        cust_df = dict_join(cust_df, "provinces", hash_key(F.col("rid"), bs+16), "c_state_prov")
        cust_df = dict_join(cust_df, "mail_providers", hash_key(F.col("rid"), bs+21), "_mp")
        cust_df = dict_join(cust_df, "taxrate_ids", hash_key(F.col("rid"), bs+22), "c_lcl_tx_id")
        cust_df = dict_join(cust_df, "taxrate_ids", hash_key(F.col("rid"), bs+23), "c_nat_tx_id")
        cust_df = cust_df.withColumn("c_email_1", F.concat(F.col("c_f_name"), F.lit("."), F.col("c_l_name"), F.lit("@"), F.col("_mp")))
        # c_email_2: ~50% get a second email (last.first@provider format)
        cust_df = dict_join(cust_df, "mail_providers", hash_key(F.col("rid"), bs+43), "_mp2")
        cust_df = cust_df.withColumn("c_email_2",
            F.when(hash_key(F.col("rid"), bs+42) % 100 < 50,
                F.concat(F.col("c_l_name"), F.lit("."), F.col("c_f_name"), F.lit("@"), F.col("_mp2")))
             .otherwise(F.lit("")))

        # Force INAC candidates back to ACTV if they have no accounts in _account_owners.
        # This prevents the audit failure where an inactive customer has zero accounts.
        # (Incremental insert customers don't automatically get accounts like historical NEWs.)
        _owners = spark.table("_account_owners")
        cust_df = (cust_df
            .join(F.broadcast(_owners.select(F.col("owner_cid").alias("_has_acct_cid")).distinct()),
                  F.col("c_id") == F.col("_has_acct_cid"), "left")
            .withColumn("c_st_id",
                F.when((F.col("c_st_id") == "INAC") & F.col("_has_acct_cid").isNull(), F.lit("ACTV"))
                 .otherwise(F.col("c_st_id")))
            .drop("_has_acct_cid"))

        # Select columns in the exact order expected by the TPC-DI Customer.txt schema.
        cust_df = cust_df.select("cdc_flag","cdc_dsn","c_id","c_tax_id","c_st_id",
            "c_l_name","c_f_name","c_m_name","c_gndr","c_tier","c_dob",
            "c_adline1","c_adline2","c_zipcode","c_city","c_state_prov","c_ctry",
            "c_ctry_1","c_area_1","c_local_1","c_ext_1",
            "c_ctry_2","c_area_2","c_local_2","c_ext_2",
            "c_ctry_3","c_area_3","c_local_3","c_ext_3",
            "c_email_1","c_email_2","c_lcl_tx_id","c_nat_tx_id")
        write_file(cust_df, f"{bp}/Customer.txt", "|", dbutils, scale_factor=cfg.sf)

        # === Account.txt ===
        # 70% inserts (new CA_IDs continuing from historical max), 30% updates.
        # Similar structure to Customer.txt but for brokerage accounts.
        n_acct_insert = new_accts
        n_acct_update = acct_per_update - n_acct_insert
        total_existing_accounts = n_hist_accounts + batch_offset * n_acct_insert
        total_existing_customers = n_hist_customers + batch_offset * n_cust_insert

        acct_df = (spark.range(0, acct_per_update).withColumnRenamed("id", "rid")
            .withColumn("_is_insert", F.col("rid") < F.lit(n_acct_insert))
            .withColumn("cdc_flag", F.when(F.col("_is_insert"), F.lit("I")).otherwise(F.lit("U")))
            .withColumn("cdc_dsn", (F.lit(acct_dsn_base) + F.lit(batch_offset * acct_per_update) + F.col("rid")).cast("string"))
            # CA_ID: inserts get new sequential IDs, updates reference existing ones.
            .withColumn("ca_id",
                F.when(F.col("_is_insert"),
                    (F.lit(n_hist_accounts) + F.lit(batch_offset * n_acct_insert) + F.col("rid")).cast("string"))
                .otherwise(
                    (hash_key(F.col("rid"), bs+105) % F.lit(total_existing_accounts)).cast("string")))
            # ca_c_id: the owning customer — always references an existing customer.
            .withColumn("ca_c_id",
                (hash_key(F.col("rid"), bs+104) % F.lit(total_existing_customers)).cast("string"))
            # ca_name: NullGenerator(5%) + RandomAString(30-50 chars)
            .withColumn("ca_name",
                F.when(hash_key(F.col("rid"), bs+106) % 1000 < 50, F.lit(""))
                .otherwise(F.substring(F.md5(F.concat(F.col("rid").cast("string"), F.lit(f"a{batch_id}"))), 1, 30)))
            # ca_tax_st: 10% value 0, 70% value 1, 20% value 2 (matching DIGen/spec)
            .withColumn("ca_tax_st",
                F.when(hash_key(F.col("rid"), bs+102) % 100 < 10, F.lit("0"))
                .when(hash_key(F.col("rid"), bs+102) % 100 < 80, F.lit("1"))
                .otherwise(F.lit("2")))
            # ca_st_id: inserts=ACTV. Updates: 67% ACTV / 33% INAC
            # (gives 70%x100% + 30%x67% = 90% ACTV overall, matching DIGen exactly)
            .withColumn("ca_st_id", F.when(F.col("_is_insert"), F.lit("ACTV")).otherwise(
                F.when(hash_key(F.col("rid"), bs+103) % 100 < 67, F.lit("ACTV")).otherwise(F.lit("INAC"))))
            .withColumn("_broker_idx", hash_key(F.col("rid"), bs+101) % broker_count)
        )
        acct_df = acct_df.join(
            F.broadcast(brokers_df.select(F.col("_idx").alias("_broker_idx"), F.col("broker_id").alias("ca_b_id"))),
            on="_broker_idx", how="left")
        # --- Add INAC account rows for customers deactivated in this batch ---
        # When a customer's c_st_id becomes INAC, their accounts must also be closed.
        # Use _account_owners (historical + prior incremental inserts) to find all
        # accounts belonging to INAC'd customers.
        inac_custs = (cust_df
            .filter(F.col("c_st_id") == "INAC")
            .select(F.col("c_id").alias("_inac_cid")))
        acct_owners = spark.table("_account_owners")
        inac_acct_rows = (inac_custs
            .join(F.broadcast(acct_owners), F.col("_inac_cid") == F.col("owner_cid"), "inner")
            .withColumn("cdc_flag", F.lit("U"))
            .withColumn("cdc_dsn", F.lit("0"))
            .withColumn("ca_b_id", F.lit(""))
            .withColumn("ca_c_id", F.col("_inac_cid"))
            .withColumn("ca_name", F.lit(""))
            .withColumn("ca_tax_st", F.lit(""))
            .withColumn("ca_st_id", F.lit("INAC"))
            .select("cdc_flag","cdc_dsn","ca_id","ca_b_id","ca_c_id","ca_name","ca_tax_st","ca_st_id"))

        acct_final = acct_df.select("cdc_flag","cdc_dsn","ca_id","ca_b_id","ca_c_id","ca_name","ca_tax_st","ca_st_id") \
            .union(inac_acct_rows)
        write_file(acct_final, f"{bp}/Account.txt", "|", dbutils, scale_factor=cfg.sf)

        # Update _account_owners with this batch's insert accounts so the next batch
        # can close them if the customer becomes INAC later.
        new_owners = (acct_df
            .filter(F.col("_is_insert"))
            .select(F.col("ca_id"), F.col("ca_c_id").alias("owner_cid")))
        acct_owners.union(new_owners).createOrReplaceTempView("_account_owners")

        log(f"[CustomerMgmt] Batch{batch_id}: {cust_per_update} customers ({n_cust_insert}I/{n_cust_update}U), "
            f"{acct_per_update} accounts ({n_acct_insert}I/{n_acct_update}U)")
        log(f"[CustomerMgmt]   Customer DSN: {cust_dsn_base + batch_offset * cust_per_update}-{cust_dsn_base + batch_offset * cust_per_update + cust_per_update - 1}", "DEBUG")
        log(f"[CustomerMgmt]   Account DSN: {acct_dsn_base + batch_offset * acct_per_update}-{acct_dsn_base + batch_offset * acct_per_update + acct_per_update - 1}", "DEBUG")
        counts[("Customer", batch_id)] = cust_per_update
        counts[("Account", batch_id)] = acct_per_update

    return counts


def generate(spark: SparkSession, cfg, dicts: dict, dbutils, views_ready_event=None) -> dict:
    """Top-level entry point — generates all customer/account data.

    If views_ready_event is provided (a threading.Event), signals it as soon as
    temp views (_created_accounts, _closed_accounts, etc.) are created. This lets
    Trade start ~5 min earlier while XML writing + incremental batches continue.
    """
    log("[CustomerMgmt] Starting generation")
    counts = {}
    counts.update(generate_customermgmt(spark, cfg, dicts, dbutils, views_ready_event))
    counts.update(generate_incremental(spark, cfg, dicts, dbutils))
    log("[CustomerMgmt] Generation complete")
    return {"counts": counts}
