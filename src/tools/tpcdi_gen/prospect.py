"""Generate Prospect table — exact DIGen matching logic.

Matching Logic
--------------
Uses a BijectivePermutation-like approach: exactly P_MATCH_PCT (~30.6%) of
prospect rows copy their (LastName, FirstName, AddressLine1, AddressLine2,
PostalCode) from a Customer record. The match covers as many distinct customers
as possible by using sequential mapping: prospect p_id maps to customer
C_ID = p_id % n_hist_customers.

For matching rows, the SAME hash seeds used in CustomerMgmt are applied to
generate identical dictionary lookup keys, producing identical name/address
values. This ensures the TPC-DI pipeline can join Prospect to DimCustomer
on (LastName, FirstName, AddressLine1, AddressLine2, PostalCode).

For non-matching rows, different seeds are used so the generated values
will not collide with any customer record.

Churn Model
-----------
Batch 1 is the full historical extract. Batches 2+ are also full extracts
(not incremental), but with ~0.12% of rows replaced ("churned"):
  - churn_per_batch = max(1, CScaling * internal_sf * 1.2)
  - The last churn_per_batch rows of the base set are replaced with newly
    generated prospects that do NOT match any customer.
  - The total row count stays the same across all batches.
This simulates a prospect data vendor periodically refreshing a small
portion of their contact list.

Gender Error Injection
-----------------------
The gender field follows this distribution:
  - ~52% empty (no gender data)
  - ~46% valid M/F values (mixed case: "M", "F", "m", "f")
  - ~2% error injection: a single random character from A-Za-z plus space
This 2% error rate tests the pipeline's ability to handle dirty data in
the gender field (the pipeline should map these to 'U' for unknown).

Other Field Notes
-----------------
  - Prospect rows = PHistScaling * internal_sf * 0.9988 (excludes deleted IDs)
  - AgencyID: first 3 chars of lastname + row index (unique identifier)
  - Middle initial: ~55% empty, rest single uppercase A-Z
  - All demographic fields (income, cars, children, etc.) have 5% NULL rate
"""

from pyspark.sql import SparkSession, functions as F, Window
from .config import *
from .utils import write_file, seed_for, dict_join, hash_key, dict_count, log


def generate_prospect(spark: SparkSession, cfg, dicts: dict, dbutils) -> dict:
    """Generate Prospect.csv for all batches (historical + incremental).

    Args:
        spark: Active SparkSession.
        cfg: ScaleConfig with internal_sf, cm_final_row_count, batch_path().
        dicts: Dictionary data (unused directly; dict_join uses registered views).
        dbutils: Databricks dbutils for file I/O.

    Returns:
        dict mapping (table_name, batch_id) to row counts for audit reporting.
    """

    log("[Prospect] Starting generation")

    # Row count: PHistScaling * SF * 0.9988 (matches DIGen's excludeDeletedIDs behavior)
    # Verified: SF=10 -> 49940, SF=100 -> 499400, SF=1000 -> 4994000
    prospect_total = int(5 * cfg.internal_sf * 0.9988)

    # --- Match count calculation ---
    # P_MATCH_PCT of rows match a customer. The actual match rate in DIGen is ~30.6%,
    # which is PMatchPct applied to the FULL prospect set including incremental overlap.
    # For simplicity, we target matching ~30.6% of rows to match DIGen's audit output.
    # --- Derive historical customer count ---
    # We need to know how many customers exist so matching prospect rows can
    # reference valid C_IDs. This replicates the CustomerMgmt formula to
    # compute the total number of historical customers (those created via NEW actions).
    cust_per_update = int(0.005 * cfg.internal_sf)
    acct_per_update = int(0.01 * cfg.internal_sf)
    new_custs = int(cust_per_update * 0.7)
    new_accts = int(acct_per_update * 0.7)
    rows_per_update = new_accts + int(acct_per_update * 0.2) + int(acct_per_update * 0.1) + int(cust_per_update * 0.2) + int(cust_per_update * 0.1)
    update_last_id = (cfg.cm_final_row_count - new_accts) // rows_per_update
    hist_size = cfg.cm_final_row_count - update_last_id * rows_per_update
    n_hist_customers = hist_size + update_last_id * new_custs

    # Cap matches at n_hist_customers so each customer maps to at most one prospect.
    # Without this cap, n_match > n_hist_customers causes multiple prospects to share
    # the same (lastname, firstname, address, postalcode), which duplicates DimCustomer
    # SCD2 records when the pipeline joins DimCustomer to Prospect.
    n_match = min(int(prospect_total * 0.306), n_hist_customers)

    log(f"[Prospect] {prospect_total} rows, {n_match} matches ({n_match*100//prospect_total}%), {n_hist_customers} historical customers")

    # =====================================================================
    # Build Prospect DataFrame
    # =====================================================================
    # First n_match rows (p_id < n_match) will match a customer record.
    # Remaining rows are independent (no customer match).
    prospect_df = (spark.range(0, prospect_total).withColumnRenamed("id", "p_id")
        .withColumn("_is_match", F.col("p_id") < F.lit(n_match))
        # For matching rows: map to a customer C_ID using sequential modular mapping.
        # Each matching prospect maps to a unique customer (wraps around if n_match > n_hist_customers).
        .withColumn("_cust_ref",
            F.when(F.col("_is_match"), F.col("p_id") % F.lit(n_hist_customers))
            .otherwise(F.lit(-1)))
    )

    # --- Name+address matching via shared hash seeds ---
    # For matching rows, use the SAME seeds as CustomerMgmt so dictionary lookups
    # produce identical values. This is the core of the matching logic: if Prospect
    # and Customer share the same (hash_key(C_ID, seed) % dict_size), they get the
    # same dictionary entry, producing identical name/address strings.
    cm_ln_seed = seed_for("CM", "ln")
    cm_fn_seed = seed_for("CM", "fn")
    cm_a1_seed = seed_for("CM", "a1")
    cm_a2n_seed = seed_for("CM", "a2n")
    cm_apt_seed = seed_for("CM", "apt")
    cm_zip_seed = seed_for("CM", "zip")

    # Dictionary sizes needed for modular arithmetic
    n_ln = dict_count("hr_family_names")
    n_fn = dict_count("hr_given_names")
    n_addr = dict_count("address_lines")
    n_zip = dict_count("zip_codes")

    # --- RandomAString helper for non-matching rows ---
    # DIGen uses RandomAString (random A-Za-z of variable length) for non-matching
    # prospect rows, NOT dictionary lookups. We approximate this by generating long
    # strings from concatenated MD5 hashes, translating hex to mixed-case alpha,
    # then taking a variable-length substring.
    def _rand_astring(id_col, seed_name, min_len, max_len):
        """Generate a random A-Za-z string of length uniformly in [min_len, max_len]."""
        _len = (hash_key(id_col, seed_for("P", seed_name + "_len")) % (max_len - min_len + 1) + min_len).cast("int")
        # Concatenate 3 MD5 hashes (96 hex chars) to cover up to 80 char output
        _raw = F.concat(
            F.md5(F.concat(id_col.cast("string"), F.lit(seed_name + "a"))),
            F.md5(F.concat(id_col.cast("string"), F.lit(seed_name + "b"))),
            F.md5(F.concat(id_col.cast("string"), F.lit(seed_name + "c"))))
        # Translate hex digits to mixed-case alpha
        _alpha = F.translate(_raw, "0123456789abcdef", "ABCDEFGHIJabcdef")
        return F.substring(_alpha, 1, _len)

    def _case_transform(id_col, seed_name, val):
        """Apply DIGen case variation: 50% first_uppercase, 25% all uppercase, 25% all lowercase."""
        _r = hash_key(id_col, seed_for("P", seed_name)) % 4
        return (F.when(_r < 2, F.concat(F.upper(F.substring(val, 1, 1)), F.lower(F.substring(val, 2, 999))))
                 .when(_r == 2, F.upper(val))
                 .otherwise(F.lower(val)))

    # For matching rows: compute dict index using same hash as CustomerMgmt (shared seeds).
    # For non-matching rows: generate RandomAString (random gibberish) to match DIGen.
    prospect_df = (prospect_df
        .withColumn("_ln_key",
            F.when(F.col("_is_match"), hash_key(F.col("_cust_ref"), cm_ln_seed) % n_ln)
            .otherwise(F.lit(-1)))
        .withColumn("_fn_key",
            F.when(F.col("_is_match"), hash_key(F.col("_cust_ref"), cm_fn_seed) % n_fn)
            .otherwise(F.lit(-1)))
        .withColumn("_a1_key",
            F.when(F.col("_is_match"), hash_key(F.col("_cust_ref"), cm_a1_seed) % n_addr)
            .otherwise(F.lit(-1)))
        .withColumn("_zip_key",
            F.when(F.col("_is_match"), hash_key(F.col("_cust_ref"), cm_zip_seed) % n_zip)
            .otherwise(F.lit(-1)))
        # AddressLine2: matching rows use 90% empty + "Apt. NNN";
        # non-matching use 5% null (empty), 95% RandomAString(5,80).
        .withColumn("_addr2_match",
            F.when(F.col("_is_match"),
                F.when(hash_key(F.col("_cust_ref"), cm_a2n_seed) % 100 < 90, F.lit(""))
                .otherwise(F.concat(F.lit("Apt. "), (hash_key(F.col("_cust_ref"), cm_apt_seed) % 999 + 1).cast("string"))))
            .otherwise(
                F.when(hash_key(F.col("p_id"), seed_for("P", "a2n")) % 100 < 5, F.lit(""))
                .otherwise(_case_transform(F.col("p_id"), "case_a2",
                    _rand_astring(F.col("p_id"), "a2", 5, 80)))))
    )

    # Join dictionary values for matching rows (non-matching get -1 key, no match)
    prospect_df = dict_join(prospect_df, "hr_family_names", F.col("_ln_key"), "_ln_raw")
    prospect_df = dict_join(prospect_df, "hr_given_names", F.col("_fn_key"), "_fn_raw")
    prospect_df = dict_join(prospect_df, "address_lines", F.col("_a1_key"), "_a1_raw")
    prospect_df = dict_join(prospect_df, "zip_codes", F.col("_zip_key"), "_zip_raw")

    # --- Assign final field values ---
    # Matching rows: use dictionary values (same as CustomerMgmt for matching).
    # Non-matching rows: RandomAString with case transformation (DIGen pattern).
    prospect_df = (prospect_df
        .withColumn("lastname",
            F.when(F.col("_is_match"), F.col("_ln_raw"))
            .otherwise(_case_transform(F.col("p_id"), "case_ln",
                _rand_astring(F.col("p_id"), "ln", 5, 30))))
        .withColumn("firstname",
            F.when(F.col("_is_match"), F.col("_fn_raw"))
            .otherwise(_case_transform(F.col("p_id"), "case_fn",
                _rand_astring(F.col("p_id"), "fn", 5, 30))))
        .withColumn("addressline1",
            F.when(F.col("_is_match"), F.col("_a1_raw"))
            .otherwise(
                F.when(hash_key(F.col("p_id"), seed_for("P", "a1n")) % 100 < 5, F.lit(""))
                .otherwise(_case_transform(F.col("p_id"), "case_a1",
                    _rand_astring(F.col("p_id"), "a1", 5, 80)))))
        .withColumn("postalcode",
            F.when(F.col("_is_match"), F.col("_zip_raw"))
            .otherwise(
                F.when(hash_key(F.col("p_id"), seed_for("P", "zipn")) % 100 < 5, F.lit(""))
                .otherwise(_rand_astring(F.col("p_id"), "zip", 5, 12))))
        .withColumn("addressline2", F.col("_addr2_match"))
    )

    # AgencyID: first 3 chars of lastname + row number (unique per prospect)
    prospect_df = prospect_df.withColumn("agencyid",
        F.concat(F.substring(F.col("lastname"), 1, 3), F.col("p_id").cast("string")))

    # Middle initial: DIGen uses NullGenerator(0.05) wrapping RandomAString max=1.
    # So 5% null, 95% have a single random A-Za-z char.
    prospect_df = prospect_df.withColumn("middleinitial",
        F.when(hash_key(F.col("p_id"), seed_for("P", "mi")) % 100 < 5, F.lit(""))
        .otherwise(F.substring(F.lit("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"),
            (hash_key(F.col("p_id"), seed_for("P", "mi_v")) % 52 + 1).cast("int"), 1)))

    # --- Gender ---
    # DIGen: NullGenerator(0.05) wrapping ProbabilityGenerator for M/F/m/f.
    # So 5% null, 95% have a gender value. Of the 95%: equal mix of M/F/m/f.
    # ~2% error injection: single random char from A-Za-z plus space
    all_chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz "
    prospect_df = prospect_df.withColumn("gender",
        F.when(hash_key(F.col("p_id"), seed_for("P", "gn")) % 100 < 5, F.lit(""))
        .when(hash_key(F.col("p_id"), seed_for("P", "gn")) % 100 < 98,
            F.array([F.lit(g) for g in ["M", "F", "m", "f"]])[
                (hash_key(F.col("p_id"), seed_for("P", "gndr")) % 4).cast("int")])
        .otherwise(  # 2% error: random char from the full alphabet + space
            F.substring(F.lit(all_chars),
                (hash_key(F.col("p_id"), seed_for("P", "gndr_err")) % len(all_chars) + 1).cast("int"), 1)))

    # Remaining demographic fields via dictionary broadcast joins
    prospect_df = dict_join(prospect_df, "cities", hash_key(F.col("p_id"), seed_for("P", "city")), "city")
    prospect_df = dict_join(prospect_df, "provinces", hash_key(F.col("p_id"), seed_for("P", "st")), "state")
    prospect_df = dict_join(prospect_df, "employers", hash_key(F.col("p_id"), seed_for("P", "emp")), "_emp")

    # Generate remaining demographic columns, each with 5% NULL rate (DIGen PERCENT_NULL)
    prospect_df = (prospect_df
        .withColumn("country",
            F.when(hash_key(F.col("p_id"), seed_for("P", "ctry")) % 100 < 80,
                F.lit("United States of America")).otherwise(F.lit("Canada")))
        # Phone: CHAR(30) -- format like "1-NNN-NNN-NNNN"
        .withColumn("phone",
            F.when(hash_key(F.col("p_id"), seed_for("P", "phn")) % 100 < 5, F.lit(""))
            .otherwise(F.concat(F.lit("1-"),
                (hash_key(F.col("p_id"), seed_for("P", "ar")) % 900 + 100).cast("string"), F.lit("-"),
                (hash_key(F.col("p_id"), seed_for("P", "l1")) % 900 + 100).cast("string"), F.lit("-"),
                (hash_key(F.col("p_id"), seed_for("P", "l2")) % 9000 + 1000).cast("string"))))
        .withColumn("income", F.when(hash_key(F.col("p_id"), seed_for("P", "in")) % 100 < 5, F.lit(""))
            .otherwise((hash_key(F.col("p_id"), seed_for("P", "inc")) % 500000).cast("string")))
        .withColumn("numbercars", F.when(hash_key(F.col("p_id"), seed_for("P", "cn")) % 100 < 5, F.lit(""))
            .otherwise((hash_key(F.col("p_id"), seed_for("P", "cars")) % 6).cast("string")))
        .withColumn("numberchildren", F.when(hash_key(F.col("p_id"), seed_for("P", "chn")) % 100 < 5, F.lit(""))
            .otherwise((hash_key(F.col("p_id"), seed_for("P", "ch")) % 6).cast("string")))
        .withColumn("maritalstatus", F.when(hash_key(F.col("p_id"), seed_for("P", "msn")) % 100 < 5, F.lit(""))
            .otherwise(F.array([F.lit(s) for s in ["S", "M", "D", "W", "U"]])[
                (hash_key(F.col("p_id"), seed_for("P", "ms")) % 5).cast("int")]))
        .withColumn("age", F.when(hash_key(F.col("p_id"), seed_for("P", "agn")) % 100 < 5, F.lit(""))
            .otherwise((hash_key(F.col("p_id"), seed_for("P", "age")) % 100).cast("string")))
        .withColumn("creditrating", F.when(hash_key(F.col("p_id"), seed_for("P", "crn")) % 100 < 5, F.lit(""))
            .otherwise((hash_key(F.col("p_id"), seed_for("P", "cr")) % 550 + 300).cast("string")))
        .withColumn("ownorrentflag", F.when(hash_key(F.col("p_id"), seed_for("P", "orn")) % 100 < 5, F.lit(""))
            .otherwise(F.array([F.lit(f) for f in ["O", "R", "U"]])[
                (hash_key(F.col("p_id"), seed_for("P", "or")) % 3).cast("int")]))
        .withColumn("employer", F.when(hash_key(F.col("p_id"), seed_for("P", "emn")) % 100 < 5, F.lit(""))
            .otherwise(F.col("_emp")))
        .withColumn("numbercreditcards", F.when(hash_key(F.col("p_id"), seed_for("P", "ccn")) % 100 < 5, F.lit(""))
            .otherwise((hash_key(F.col("p_id"), seed_for("P", "cc")) % 11).cast("string")))
        .withColumn("networth", F.when(hash_key(F.col("p_id"), seed_for("P", "nwn")) % 100 < 5, F.lit(""))
            .otherwise((hash_key(F.col("p_id"), seed_for("P", "nw")) % 3990000 + 10000).cast("string")))
    )

    final = prospect_df.select(
        "agencyid", "lastname", "firstname", "middleinitial", "gender",
        "addressline1", "addressline2", "postalcode", "city", "state", "country",
        "phone", "income", "numbercars", "numberchildren", "maritalstatus", "age",
        "creditrating", "ownorrentflag", "employer", "numbercreditcards", "networth")

    # =====================================================================
    # Write all batches
    # =====================================================================
    # Batch 1: full historical load (all prospect_total rows)
    counts = {}
    write_file(final, f"{cfg.batch_path(1)}/Prospect.csv", ",", dbutils,
               scale_factor=cfg.sf, estimated_rows=prospect_total, avg_row_bytes=200)
    counts[("Prospect", 1)] = prospect_total

    # --- Churn model for incremental batches ---
    # Batch 2+: full extract with ~0.12% churn (same total rows, some replaced).
    # Churn formula: ~CScaling * internal_sf * 1.2 prospects replaced per batch.
    # The last churn_per_batch rows of the base set are swapped out for new
    # prospects (with different seeds) that will NOT match any customer.
    churn_per_batch = max(1, int(0.005 * cfg.internal_sf * 1.2))

    for batch_id in range(2, NUM_INCREMENTAL_BATCHES + 2):
        batch_offset = batch_id - 1
        # New prospect IDs start after the historical range to avoid collisions
        new_start = prospect_total + (batch_offset - 1) * churn_per_batch

        # Keep rows 0..prospect_total-churn-1 from the base, replace the rest with new ones
        keep_count = prospect_total - churn_per_batch
        batch_base = final.limit(keep_count)

        # Generate churn_per_batch new prospects (non-matching, unique seeds per batch)
        new_df = (spark.range(0, churn_per_batch).withColumnRenamed("id", "new_id")
            .withColumn("p_id", F.col("new_id") + F.lit(new_start)))

        # Build new prospect fields using batch-specific seed prefix (e.g. "PB2", "PB3")
        # to ensure each batch's churned rows are unique
        new_df = dict_join(new_df, "hr_family_names", hash_key(F.col("p_id"), seed_for(f"PB{batch_id}", "ln")), "_ln")
        new_df = dict_join(new_df, "hr_given_names", hash_key(F.col("p_id"), seed_for(f"PB{batch_id}", "fn")), "_fn")
        new_df = dict_join(new_df, "address_lines", hash_key(F.col("p_id"), seed_for(f"PB{batch_id}", "a1")), "_a1")
        new_df = dict_join(new_df, "zip_codes", hash_key(F.col("p_id"), seed_for(f"PB{batch_id}", "zip")), "_zip")
        new_df = dict_join(new_df, "cities", hash_key(F.col("p_id"), seed_for(f"PB{batch_id}", "city")), "_city")
        new_df = dict_join(new_df, "provinces", hash_key(F.col("p_id"), seed_for(f"PB{batch_id}", "st")), "_state")
        new_df = dict_join(new_df, "employers", hash_key(F.col("p_id"), seed_for(f"PB{batch_id}", "emp")), "_emp2")

        new_prospects = (new_df
            .withColumn("agencyid", F.concat(F.substring(F.col("_ln"), 1, 3), F.col("p_id").cast("string")))
            .withColumn("lastname", F.col("_ln"))
            .withColumn("firstname", F.col("_fn"))
            .withColumn("middleinitial", F.when(hash_key(F.col("p_id"), seed_for(f"PB{batch_id}", "mi")) % 100 < 55, F.lit(""))
                .otherwise(F.substring(F.lit("ABCDEFGHIJKLMNOPQRSTUVWXYZ"), (hash_key(F.col("p_id"), seed_for(f"PB{batch_id}", "mi_v")) % 26 + 1).cast("int"), 1)))
            .withColumn("gender", F.when(hash_key(F.col("p_id"), seed_for(f"PB{batch_id}", "gn")) % 100 < 52, F.lit(""))
                .otherwise(F.array([F.lit(g) for g in ["M","F","m","f"]])[(hash_key(F.col("p_id"), seed_for(f"PB{batch_id}", "gndr")) % 4).cast("int")]))
            .withColumn("addressline1", F.col("_a1"))
            .withColumn("addressline2", F.when(hash_key(F.col("p_id"), seed_for(f"PB{batch_id}", "a2")) % 100 < 70, F.lit(""))
                .otherwise(F.concat(F.lit("apt. "), (hash_key(F.col("p_id"), seed_for(f"PB{batch_id}", "apt")) % 999).cast("string"))))
            .withColumn("postalcode", F.col("_zip"))
            .withColumn("city", F.col("_city"))
            .withColumn("state", F.col("_state"))
            .withColumn("country", F.when(hash_key(F.col("p_id"), seed_for(f"PB{batch_id}", "ctry")) % 100 < 80,
                F.lit("United States of America")).otherwise(F.lit("Canada")))
            .withColumn("phone", F.concat(F.lit("1-"),
                (hash_key(F.col("p_id"), seed_for(f"PB{batch_id}", "ar")) % 900 + 100).cast("string"), F.lit("-"),
                (hash_key(F.col("p_id"), seed_for(f"PB{batch_id}", "l1")) % 900 + 100).cast("string"), F.lit("-"),
                (hash_key(F.col("p_id"), seed_for(f"PB{batch_id}", "l2")) % 9000 + 1000).cast("string")))
            .withColumn("income", (hash_key(F.col("p_id"), seed_for(f"PB{batch_id}", "inc")) % 500000).cast("string"))
            .withColumn("numbercars", (hash_key(F.col("p_id"), seed_for(f"PB{batch_id}", "cars")) % 6).cast("string"))
            .withColumn("numberchildren", (hash_key(F.col("p_id"), seed_for(f"PB{batch_id}", "ch")) % 6).cast("string"))
            .withColumn("maritalstatus", F.array([F.lit(s) for s in ["S","M","D","W","U"]])[(hash_key(F.col("p_id"), seed_for(f"PB{batch_id}", "ms")) % 5).cast("int")])
            .withColumn("age", (hash_key(F.col("p_id"), seed_for(f"PB{batch_id}", "age")) % 100).cast("string"))
            .withColumn("creditrating", (hash_key(F.col("p_id"), seed_for(f"PB{batch_id}", "cr")) % 550 + 300).cast("string"))
            .withColumn("ownorrentflag", F.array([F.lit(f) for f in ["O","R","U"]])[(hash_key(F.col("p_id"), seed_for(f"PB{batch_id}", "or")) % 3).cast("int")])
            .withColumn("employer", F.col("_emp2"))
            .withColumn("numbercreditcards", (hash_key(F.col("p_id"), seed_for(f"PB{batch_id}", "cc")) % 11).cast("string"))
            .withColumn("networth", (hash_key(F.col("p_id"), seed_for(f"PB{batch_id}", "nw")) % 3990000 + 10000).cast("string"))
            .select("agencyid", "lastname", "firstname", "middleinitial", "gender",
                    "addressline1", "addressline2", "postalcode", "city", "state", "country",
                    "phone", "income", "numbercars", "numberchildren", "maritalstatus", "age",
                    "creditrating", "ownorrentflag", "employer", "numbercreditcards", "networth")
        )

        # Union the kept base rows with the new churned prospects
        batch_df = batch_base.union(new_prospects)
        write_file(batch_df, f"{cfg.batch_path(batch_id)}/Prospect.csv", ",", dbutils, scale_factor=cfg.sf)
        counts[("Prospect", batch_id)] = prospect_total
        log(f"[Prospect] Batch{batch_id}: {prospect_total} rows ({churn_per_batch} churned)", "DEBUG")

    log(f"[Prospect] {prospect_total} rows, {n_match} matching customers ({n_match*100//prospect_total}%)")
    log("[Prospect] Generation complete")
    return counts


def generate(spark: SparkSession, cfg, dicts: dict, dbutils) -> dict:
    """Top-level entry point matching the old prospect.py interface.

    Delegates to generate_prospect(). This wrapper exists so the orchestrator
    can call prospect.generate() with the same signature as other modules.
    """
    return generate_prospect(spark, cfg, dicts, dbutils)
