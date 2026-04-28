"""Generate FINWIRE records (CMP, SEC, FIN) as fixed-width quarterly files.

Key relationships per TPC-DI spec (section 2.2.2.8):
- CMP records define companies. CIK is the natural key. CMP records feed DimCompany.
- SEC records define securities. Symbol is the natural key. SEC references CMP via CoNameOrCIK.
  SEC feeds DimSecurity, which has FK SK_CompanyID -> DimCompany.
- FIN records are quarterly financials for each active company. One FIN per company per quarter
  from the company's founding quarter onward. FIN references CMP via CoNameOrCIK.
  FIN feeds the Financial table, which has FK SK_CompanyID -> DimCompany.

Temporal integrity: SEC/FIN CoNameOrCIK must match a CMP record that was posted
before the SEC/FIN record's PTS (EffectiveDate <= PTS < EndDate in DimCompany).

CMP/SEC/FIN Generation
-----------------------
Records are distributed across FINWIRE quarters (0..fw_quarters-1):
  - CMP: Large initial burst in Q0 (historical backfill of ~60% of companies), then
    a steady rate of cmp_per_quarter for Q1..Q201. This mirrors DIGen's pattern where
    most companies pre-date the benchmark period.
  - SEC: No records in Q0. Large burst in Q1 (all securities needed for the initial
    market), then sec_per_quarter for Q2+. Q1 absorbs the remainder so totals match.
  - FIN: One financial record per active company per quarter AFTER that company's
    creation quarter. Generated via cross-join of active companies x quarters, filtered
    to fin_quarter_id > creation_quarter.

Quarterly Distribution Formulas (verified at SF=10, 100, 1000)
--------------------------------------------------------------
  CMP: cmp_per_quarter = cmp_total // (fw_quarters + 1)
       Q0 gets the remainder: cmp_total - fw_quarters * cmp_per_quarter
       Q1..Q201 each get cmp_per_quarter
  SEC: sec_per_quarter = sec_total // fw_quarters
       Q0 = 0 (no securities before the market opens)
       Q1 = sec_total - (fw_quarters - 1) * sec_per_quarter (absorbs remainder)
       Q2..Q201 each get sec_per_quarter

_symbols Temp View
-------------------
After SEC generation, active symbols (Status='ACTV') are persisted as the _symbols
temp view with columns: Symbol, creation_quarter, deactivation_quarter, _idx.
This view is consumed by market_data, watch_history, and trade modules to determine
which securities are tradeable on any given day.

_cmp_refs Temp View
--------------------
After CMP generation, company references are persisted as the _cmp_refs temp view
with columns: _idx, CIK, CompanyName, creation_quarter. This view enables:
  - SEC records to look up a valid CMP reference for CoNameOrCIK
  - FIN records to reference their parent company
  - Temporal integrity: SEC/FIN only reference CMPs created before their own quarter

Deactivation Model
-------------------
DIGen's UpdateBlackBox applies 97% new / 1% change / 2% delete per quarter to the
SEC update batch. The 2% delete rate applies to sec_per_quarter (the per-quarter
batch size), NOT to all securities. Total deactivations over the FINWIRE lifetime:
    total_deact = 0.02 * sec_per_quarter * fw_quarters
At SF=10 this yields ~158 deactivations out of ~8000 total securities (~2% of total).
Each deactivated security is assigned a random deactivation_quarter after its
creation_quarter. Securities not selected for deactivation get
deactivation_quarter = fw_quarters + 1 (effectively "never deactivated").
"""

from pyspark.sql import SparkSession, functions as F, Window
from .config import *
from .utils import write_file, seed_for, dict_join, hash_key, dict_count, register_copies_from_staging, _cleanup, log, disk_cache
from .audit import static_audits_available


def _add_calendar_quarter_timing(df, quarter_id_col="quarter_id"):
    """Add calendar quarter timing columns derived from ``quarter_id``.

    Matches DIGen's ``FinwirePTSGenerator``: each updateID maps to exactly one
    calendar quarter (Jan-Mar / Apr-Jun / Jul-Sep / Oct-Dec of FW_BEGIN.year +
    quarter_id // 4). PTS values for records in that updateID are constrained
    to that calendar quarter's boundaries, so the downstream Year/Quarter/
    QtrStartDate fields are always consistent — eliminating duplicate
    (company, quarter, year) rows in the Financial table.

    Adds columns:
      _q_start_s   — unix timestamp (seconds) of 00:00:00 on quarter start day
      _q_duration_s — seconds in the quarter (end 23:59:59 minus start, + 1)
    """
    begin_year = FW_BEGIN_DATE.year
    df = df.withColumn("_cal_year", F.lit(begin_year) + (F.col(quarter_id_col) / 4).cast("int"))
    df = df.withColumn("_cal_q_idx", F.col(quarter_id_col) % 4)
    df = df.withColumn("_q_start_month",
        F.when(F.col("_cal_q_idx") == 0, F.lit(1))
         .when(F.col("_cal_q_idx") == 1, F.lit(4))
         .when(F.col("_cal_q_idx") == 2, F.lit(7))
         .otherwise(F.lit(10)))
    df = df.withColumn("_q_end_month",
        F.when(F.col("_cal_q_idx") == 0, F.lit(3))
         .when(F.col("_cal_q_idx") == 1, F.lit(6))
         .when(F.col("_cal_q_idx") == 2, F.lit(9))
         .otherwise(F.lit(12)))
    df = df.withColumn("_q_end_day",
        F.when(F.col("_cal_q_idx").isin(0, 3), F.lit(31))
         .otherwise(F.lit(30)))
    df = df.withColumn("_q_start_s",
        F.unix_timestamp(
            F.concat(
                F.lpad(F.col("_cal_year").cast("string"), 4, "0"), F.lit("-"),
                F.lpad(F.col("_q_start_month").cast("string"), 2, "0"),
                F.lit("-01 00:00:00")),
            "yyyy-MM-dd HH:mm:ss"))
    df = df.withColumn("_q_end_s",
        F.unix_timestamp(
            F.concat(
                F.lpad(F.col("_cal_year").cast("string"), 4, "0"), F.lit("-"),
                F.lpad(F.col("_q_end_month").cast("string"), 2, "0"), F.lit("-"),
                F.lpad(F.col("_q_end_day").cast("string"), 2, "0"),
                F.lit(" 23:59:59")),
            "yyyy-MM-dd HH:mm:ss"))
    df = df.withColumn("_q_duration_s", F.col("_q_end_s") - F.col("_q_start_s") + F.lit(1))
    return df.drop("_cal_year", "_cal_q_idx", "_q_start_month", "_q_end_month", "_q_end_day", "_q_end_s")


def generate(spark: SparkSession, cfg, dicts: dict, dbutils, symbols_ready_event=None) -> dict:
    """Generate all FINWIRE quarterly files.

    Persists _symbols and _cmp_refs temp views for downstream tables
    (market_data, trade, watch_history, customer).

    The output is a set of fixed-width text files named FINWIRE{YYYY}Q{N}, one per
    calendar quarter. All three record types (CMP, SEC, FIN) are merged, sorted by
    PTS within each file, and written via Spark's partitioned text writer.

    Args:
        spark: Active SparkSession.
        cfg: ScaleConfig with cmp_total, sec_total, fw_quarters, etc.
        dicts: Dictionary data (unused directly; dict_join uses registered views).
        dbutils: Databricks dbutils for file I/O.

    Returns:
        dict with key "counts" mapping (table_name, batch_id) to row counts.
    """
    log("[FINWIRE] Starting generation")

    # =====================================================================
    # CMP: Company records
    # =====================================================================
    # DIGen pattern: large initial burst in Q0, then steady rate per quarter. ~60% of companies appear in Q0 (historical backfill), rest spread across later quarters at a rate of cmp_per_quarter.
    cmp_df = (spark.range(0, cfg.cmp_total).withColumnRenamed("id", "cmp_id")
        # Exact DIGen formula (verified at SF=10,100,1000): cmp_per_quarter = cmp_total // (fw_quarters + 1) Q0 gets: cmp_total - fw_quarters * cmp_per_quarter (the remainder) Q1-Q201 each get: cmp_per_quarter
        .withColumn("quarter_id",
            F.when(F.col("cmp_id") < F.lit(cfg.cmp_total - cfg.fw_quarters * cfg.cmp_per_quarter), F.lit(0))
            .otherwise(
                F.least(
                    ((F.col("cmp_id") - F.lit(cfg.cmp_total - cfg.fw_quarters * cfg.cmp_per_quarter))
                        / F.lit(cfg.cmp_per_quarter) + 1).cast("int"),
                    F.lit(cfg.fw_quarters - 1))))
        # CIK: zero-padded 10-digit company identifier (natural key)
        .withColumn("CIK", F.lpad(F.col("cmp_id").cast("string"), 10, "0"))
        # Prefix with "C" to ensure name is never all-numeric. If the name were all digits, ingest_finwire would misclassify FIN_NAME records as FIN_COMPANYID, causing the Financial->DimCompany join to fail on those records.
        .withColumn("CompanyName", F.concat(F.lit("C"), F.substring(F.md5(F.concat(F.col("cmp_id").cast("string"), F.lit("cname"))), 1, 14)))
        # Status: 97% ACTV (active), 3% INAC (inactive) — matches DIGen's distribution
        .withColumn("Status", F.when(hash_key(F.col("cmp_id"), seed_for("CMP", "st")) % 100 < 97, F.lit("ACTV")).otherwise(F.lit("INAC")))
    )

    # PTS: uniformly distributed within the calendar quarter of quarter_id. See _add_calendar_quarter_timing for why this must be calendar-aligned.
    cmp_df = _add_calendar_quarter_timing(cmp_df)
    cmp_df = (cmp_df
        .withColumn("_q_offset", hash_key(F.col("cmp_id"), seed_for("CMP", "pts")) % F.col("_q_duration_s"))
        .withColumn("PTS", F.date_format((F.col("_q_start_s") + F.col("_q_offset")).cast("timestamp"), "yyyyMMdd-HHmmss"))
        .drop("_q_start_s", "_q_duration_s", "_q_offset"))

    # Dictionary columns via broadcast join: pick values from pre-loaded dictionaries using hash-based indexing for deterministic, reproducible assignment.
    cmp_df = dict_join(cmp_df, "industry_ids", hash_key(F.col("cmp_id"), seed_for("CMP", "ind")), "IndustryID")
    cmp_df = dict_join(cmp_df, "address_lines", hash_key(F.col("cmp_id"), seed_for("CMP", "addr")), "AddrLine1")
    cmp_df = dict_join(cmp_df, "zip_codes", hash_key(F.col("cmp_id"), seed_for("CMP", "zip")), "PostalCode")
    cmp_df = dict_join(cmp_df, "cities", hash_key(F.col("cmp_id"), seed_for("CMP", "city")), "City")
    cmp_df = dict_join(cmp_df, "provinces", hash_key(F.col("cmp_id"), seed_for("CMP", "state")), "StateProvince")
    cmp_df = dict_join(cmp_df, "last_names", hash_key(F.col("cmp_id"), seed_for("CMP", "ceo")), "CEOname")

    # S&P rating: picked from 22 standard rating values (AAA, AA+, ..., D)
    sp_arr = F.array([F.lit(r) for r in SP_RATINGS])  # 22 values

    cmp_df = (cmp_df
        .withColumn("SPrating", sp_arr[(hash_key(F.col("cmp_id"), seed_for("CMP", "sp")) % len(SP_RATINGS)).cast("int")])
        # FoundingDate: random date in [1880-01-01, ~1980] (36500-day range)
        .withColumn("FoundingDate", F.date_format(F.date_add(F.lit("1880-01-01"),
            (hash_key(F.col("cmp_id"), seed_for("CMP", "fd")) % 36500).cast("int")), "yyyyMMdd"))
        # AddrLine2: 90% empty, 10% "Suite NNN"
        .withColumn("AddrLine2", F.when(hash_key(F.col("cmp_id"), seed_for("CMP", "a2n")) % 100 < 90, F.lit(""))
            .otherwise(F.concat(F.lit("Suite "), (hash_key(F.col("cmp_id"), seed_for("CMP", "suite")) % 999 + 1).cast("string"))))
        # Country: 5% empty, 80% of remainder USA, rest Canada
        .withColumn("Country", F.when(hash_key(F.col("cmp_id"), seed_for("CMP", "cn")) % 100 < 5, F.lit(""))
            .when(hash_key(F.col("cmp_id"), seed_for("CMP", "ctry")) % 100 < 80, F.lit("United States of America"))
            .otherwise(F.lit("Canada")))
        .withColumn("Description", F.substring(F.md5(F.concat(F.col("cmp_id").cast("string"), F.lit("desc"))), 1, 20))
    )

    # --- CMP CHANGE and DELETE records (matches DIGen's 97% new / 1% change / 2% delete) ---
    # DIGen generates CHANGE records that modify an existing CIK's attributes (new PTS, possibly different IndustryID/SPrating) and DELETE records that deactivate a CIK (Status=INAC). These create multiple SCD2 versions per CIK in DimCompany. We sample 3% of cmp_ids (1% change + 2% delete) and emit additional rows with same CIK + later quarter_id. Attributes are regenerated with new seeds so the SCD2 versions have meaningfully different values.
    _cmp_pct = hash_key(F.col("cmp_id"), seed_for("CMP", "action_pct")) % 100
    _is_change = _cmp_pct < F.lit(1)
    _is_delete = (_cmp_pct >= F.lit(1)) & (_cmp_pct < F.lit(3))
    cmp_extras = (cmp_df
        # Skip companies created in the last quarter — no later quarter available.
        .filter((_is_change | _is_delete) & (F.col("quarter_id") < F.lit(cfg.fw_quarters - 1)))
        .withColumn("_action",
            F.when(_is_change, F.lit("CHANGE")).otherwise(F.lit("DELETE")))
        # New quarter_id: uniformly random in (creation_quarter, fw_quarters-1]
        .withColumn("_q_remaining",
            F.greatest(F.lit(1), F.lit(cfg.fw_quarters - 1) - F.col("quarter_id")))
        .withColumn("quarter_id",
            F.col("quarter_id") + F.lit(1) +
            (hash_key(F.col("cmp_id"), seed_for("CMP", "action_q")) % F.col("_q_remaining")).cast("int"))
        # For DELETE: Status=INAC. For CHANGE: keep ACTV (attribute-only change).
        .withColumn("Status",
            F.when(F.col("_action") == "DELETE", F.lit("INAC")).otherwise(F.lit("ACTV")))
        # Regenerate attributes with action-specific salt so CHANGE rows have different values from the original (otherwise SCD2 versions are identical).
        .withColumn("_chg_salt",
            F.when(F.col("_action") == "CHANGE",
                hash_key(F.col("cmp_id"), seed_for("CMP", "change_salt"))).otherwise(F.lit(0)))
        .drop("_q_remaining", "_action"))

    # Regenerate time-dependent fields for the extras (PTS in new calendar quarter)
    cmp_extras = _add_calendar_quarter_timing(cmp_extras)
    cmp_extras = (cmp_extras
        .withColumn("_q_offset",
            hash_key(F.col("cmp_id"), seed_for("CMP", "extra_pts")) % F.col("_q_duration_s"))
        .withColumn("PTS",
            F.date_format((F.col("_q_start_s") + F.col("_q_offset")).cast("timestamp"), "yyyyMMdd-HHmmss"))
        .drop("_q_start_s", "_q_duration_s", "_q_offset", "_chg_salt"))

    # --- Persist _cmp_refs temp view (BEFORE unioning extras) ---
    # _cmp_refs must have one row per cmp_id (the creation event), not one per CMP record. SEC/FIN use this for temporal integrity checks on creation.
    cmp_ref = cmp_df.select(
        F.col("cmp_id").alias("_idx"),
        "CIK", "CompanyName",
        F.col("quarter_id").alias("creation_quarter"))
    cmp_ref.createOrReplaceTempView("_cmp_refs")
    cmp_count = cfg.cmp_total

    # Union: original NEW records + CHANGE/DELETE extras, aligned by column name
    cmp_df = cmp_df.unionByName(cmp_extras)

    # Format CMP as fixed-width line (TPC-DI FINWIRE spec field widths)
    cmp_lines = cmp_df.select(
        F.rtrim(F.concat(
            F.rpad("PTS", 15, " "), F.rpad(F.lit("CMP"), 3, " "),
            F.rpad("CompanyName", 60, " "), F.col("CIK"),
            F.rpad("Status", 4, " "), F.rpad("IndustryID", 2, " "),
            F.rpad("SPrating", 4, " "), F.col("FoundingDate"),
            F.rpad("AddrLine1", 80, " "), F.rpad("AddrLine2", 80, " "),
            F.rpad("PostalCode", 12, " "), F.rpad("City", 25, " "),
            F.rpad("StateProvince", 20, " "), F.rpad("Country", 24, " "),
            F.rpad("CEOname", 46, " "), F.rpad("Description", 150, " "),
        )).alias("line"),
        F.col("quarter_id"), F.col("PTS"),
    )

    # =====================================================================
    # SEC: Security records
    # =====================================================================
    def _id_to_symbol_expr(col):
        """Convert an integer sec_id to a 15-character base-26 ticker symbol.

        Generates symbols like 'AAAAAAAAAAAAAAA', 'AAAAAAAAAAAAAAB', etc.
        Pure Spark SQL expression (no UDFs) for Photon compatibility.
        Each position is computed via repeated mod-26 / divide-26 operations.
        """
        chars = []
        remainder = col.cast("long")
        for _ in range(15):
            chars.append(F.chr((remainder % 26 + 65).cast("int")))
            remainder = (remainder / 26).cast("long")
        return F.concat(*reversed(chars))

    ex_arr = F.array([F.lit(e) for e in EXCHANGES])
    it_arr = F.array([F.lit(t) for t in ISSUE_TYPES])

    # SEC quarterly distribution (exact DIGen formula): sec_per_quarter = sec_total // fw_quarters (divided across 202 quarters) Q0 = 0 (no securities before market opens) Q1 = sec_total - (fw_quarters - 1) * sec_per_quarter (absorbs remainder) Q2+ = sec_per_quarter each
    sec_q1 = cfg.sec_total - (cfg.fw_quarters - 1) * cfg.sec_per_quarter

    sec_df = (spark.range(0, cfg.sec_total).withColumnRenamed("id", "sec_id")
        # Q1 gets first sec_q1 records, then Q2+ get sec_per_quarter each
        .withColumn("quarter_id",
            F.when(F.col("sec_id") < F.lit(sec_q1), F.lit(1))
            .otherwise(
                F.least(
                    ((F.col("sec_id") - F.lit(sec_q1)) / F.lit(cfg.sec_per_quarter) + 2).cast("int"),
                    F.lit(cfg.fw_quarters - 1))))
        # Symbol: deterministic 15-char base-26 ticker derived from sec_id
        .withColumn("Symbol", _id_to_symbol_expr(F.col("sec_id")))
        # Status: 97% ACTV, 3% INAC (initial status; deactivation handled separately below)
        .withColumn("Status", F.when(hash_key(F.col("sec_id"), seed_for("SEC", "st")) % 100 < 97, F.lit("ACTV")).otherwise(F.lit("INAC")))
        .withColumn("IssueType", it_arr[(hash_key(F.col("sec_id"), seed_for("SEC", "it")) % len(ISSUE_TYPES)).cast("int")])
        .withColumn("Name", F.substring(F.md5(F.concat(F.col("sec_id").cast("string"), F.lit("sn"))), 1, 20))
        .withColumn("ExID", ex_arr[(hash_key(F.col("sec_id"), seed_for("SEC", "ex")) % len(EXCHANGES)).cast("int")])
        .withColumn("ShOut", (hash_key(F.col("sec_id"), seed_for("SEC", "sh")) % 999900000 + 100000).cast("string"))
        .withColumn("FirstTradeDate", F.date_format(F.date_add(F.lit("1880-01-01"),
            (hash_key(F.col("sec_id"), seed_for("SEC", "ftd")) % 36500).cast("int")), "yyyyMMdd"))
        .withColumn("FirstTradeExchg", F.date_format(F.date_add(F.lit("1880-01-01"),
            (hash_key(F.col("sec_id"), seed_for("SEC", "fte")) % 36500).cast("int")), "yyyyMMdd"))
        .withColumn("Dividend", F.format_string("%.2f", (hash_key(F.col("sec_id"), seed_for("SEC", "div")) % 300) / 100.0))
        # CoNameOrCIK: reference a CMP that was created BEFORE this SEC record (temporal integrity). Map sec_id to a candidate cmp_id; the join below validates temporal ordering.
        .withColumn("_cmp_ref_idx", hash_key(F.col("sec_id"), seed_for("SEC", "ref")) % cmp_count)
    )

    # PTS: uniformly distributed within the calendar quarter of quarter_id.
    sec_df = _add_calendar_quarter_timing(sec_df)
    sec_df = (sec_df
        .withColumn("_q_offset", hash_key(F.col("sec_id"), seed_for("SEC", "pts")) % F.col("_q_duration_s"))
        .withColumn("PTS", F.date_format((F.col("_q_start_s") + F.col("_q_offset")).cast("timestamp"), "yyyyMMdd-HHmmss"))
        .drop("_q_start_s", "_q_duration_s", "_q_offset"))

    # Join to _cmp_refs to resolve the company reference. Broadcast join since _cmp_refs is small (one row per company).
    sec_df = sec_df.join(
        spark.table("_cmp_refs").select(
            F.col("_idx").alias("_cmp_ref_idx"),
            F.col("CIK").alias("_ref_cik"),
            F.col("CompanyName").alias("_ref_name"),
            F.col("creation_quarter").alias("_ref_cq")),
        on="_cmp_ref_idx", how="left")

    # Temporal integrity enforcement: if the referenced company was created AFTER this SEC record's quarter, fall back to CMP 0 (always exists in Q0). 50/50 split between using CIK (numeric) vs CompanyName (string) as the reference. Company 0's actual name (used as temporal fallback for name-based references)
    _cmp0_name = F.concat(F.lit("C"), F.substring(F.md5(F.concat(F.lit("0"), F.lit("cname"))), 1, 14))
    sec_df = sec_df.withColumn("CoNameOrCIK",
        F.when(hash_key(F.col("sec_id"), seed_for("SEC", "cn")) % 100 < 50,
            F.rpad(F.when(F.col("_ref_cq") <= F.col("quarter_id"), F.col("_ref_cik")).otherwise(F.lit("0000000000")), 60, " "))
        .otherwise(
            F.rpad(F.when(F.col("_ref_cq") <= F.col("quarter_id"), F.col("_ref_name")).otherwise(
                _cmp0_name), 60, " ")))

    # --- SEC CHANGE and DELETE records (matches DIGen's 97% new / 1% change / 2% delete) ---
    # Creates multiple DimSecurity SCD2 versions per Symbol. CHANGE keeps Status=ACTV, DELETE sets Status=INAC. All extras have quarter_id > creation quarter_id. Note: the _symbols view filters to ACTV before groupBy, so DELETE records don't affect creation_quarter tracking — but they do produce Inactive DimSecurity versions.
    _sec_pct = hash_key(F.col("sec_id"), seed_for("SEC", "action_pct")) % 100
    _sec_is_change = _sec_pct < F.lit(1)
    _sec_is_delete = (_sec_pct >= F.lit(1)) & (_sec_pct < F.lit(3))
    sec_extras = (sec_df
        # Skip securities created in the last quarter — no later quarter available.
        .filter((_sec_is_change | _sec_is_delete) & (F.col("quarter_id") < F.lit(cfg.fw_quarters - 1)))
        .withColumn("_action",
            F.when(_sec_is_change, F.lit("CHANGE")).otherwise(F.lit("DELETE")))
        .withColumn("_q_remaining",
            F.greatest(F.lit(1), F.lit(cfg.fw_quarters - 1) - F.col("quarter_id")))
        .withColumn("quarter_id",
            F.col("quarter_id") + F.lit(1) +
            (hash_key(F.col("sec_id"), seed_for("SEC", "action_q")) % F.col("_q_remaining")).cast("int"))
        .withColumn("Status",
            F.when(F.col("_action") == "DELETE", F.lit("INAC")).otherwise(F.lit("ACTV")))
        .drop("_q_remaining", "_action"))

    # Regenerate PTS for extras using calendar-quarter-aligned timing
    sec_extras = _add_calendar_quarter_timing(sec_extras)
    sec_extras = (sec_extras
        .withColumn("_q_offset", hash_key(F.col("sec_id"), seed_for("SEC", "extra_pts")) % F.col("_q_duration_s"))
        .withColumn("PTS", F.date_format((F.col("_q_start_s") + F.col("_q_offset")).cast("timestamp"), "yyyyMMdd-HHmmss"))
        .drop("_q_start_s", "_q_duration_s", "_q_offset"))

    # --- Persist _symbols temp view (BEFORE unioning extras) ---
    # Built from NEW sec_df only; creation_quarter = each symbol's NEW record's quarter_id. The deactivation model here is approximate — SEC DELETE records (added below) also deactivate symbols, but the hash-based model gives a deterministic total_deact count.
    total_deact = int(0.02 * cfg.sec_per_quarter * cfg.fw_quarters)
    symbols = (sec_df
        .filter(F.col("Status") == "ACTV")
        .select("Symbol", "quarter_id", "sec_id")
        .groupBy("Symbol").agg(
            F.min("quarter_id").alias("creation_quarter"),
            F.min("sec_id").alias("_sec_id"))
        .withColumn("_deact_draw", F.abs(hash_key(F.col("_sec_id"), seed_for("SEC", "deact"))))
        .withColumn("deactivation_quarter",
            F.when(F.col("_deact_draw") % F.lit(cfg.sec_total) < F.lit(total_deact),
                F.col("creation_quarter") +
                    (F.abs(hash_key(F.col("_sec_id"), seed_for("SEC", "deact_q"))) %
                     F.greatest(F.lit(1), F.lit(cfg.fw_quarters) - F.col("creation_quarter"))))
            .otherwise(F.lit(cfg.fw_quarters + 1)))
        # Order _idx by creation_quarter so WatchHistory's cross-product decomposition picks temporally-valid symbols: gen 0 pairs use _sec_idx in [0, hist_sec_ids), which must map to the oldest symbols (created before customer updates begin). Later gens extend into symbols created in their quarter window. Prior alphabetical ordering caused ~25% of WH ACTV pairs to fall back to _sym0 at SF=5000+, producing a -24.5% drift vs DIGen. _idx cast to long here: downstream consumers (Trade, WatchHistory, DailyMarket) compare this to hash_key(...) expressions that return long. If _idx stayed int, Spark's analyzer inserts a cast(_idx as bigint) into the join condition — that cast can prevent the optimizer from auto-broadcasting _symbols even when it's well under the autoBroadcastJoinThreshold.
        .withColumn("_idx", (F.row_number().over(
            Window.orderBy("creation_quarter", "Symbol")) - 1).cast("long"))
        .select("Symbol", "creation_quarter", "deactivation_quarter", "_idx"))

    # Stage _symbols to Parquet so Trade/WatchHistory/DailyMarket can read it independently of the remaining FINWIRE compute (CMP/FIN union + the 10-25 min text write). This detaches downstream start time from the overall FINWIRE wallclock — previously they waited on f_fw.result(), now they wait on symbols_ready_event set right below.
    symbols, _sym_cleanup = disk_cache(symbols, spark, "FINWIRE symbols",
                                        volume_path=cfg.volume_path, dbutils=dbutils)
    symbols.createOrReplaceTempView("_symbols")
    # Estimate — symbols is a groupBy on SEC NEW records' Symbol; ~sec_total after dedup by Symbol (slight shrinkage from Symbol collisions across quarters).
    log(f"[FINWIRE] Active symbols: ~{cfg.sec_total:,} -> _symbols view (downstream unblocked)")
    if symbols_ready_event is not None:
        symbols_ready_event.set()

    # Union NEW + CHANGE/DELETE records, aligned by column name. Done AFTER _symbols is built so extras don't affect creation_quarter/deactivation_quarter tracking.
    sec_df = sec_df.unionByName(sec_extras)

    # Format SEC as fixed-width line
    sec_lines = sec_df.select(
        F.rtrim(F.concat(
            F.rpad("PTS", 15, " "), F.rpad(F.lit("SEC"), 3, " "),
            F.rpad("Symbol", 15, " "), F.rpad("IssueType", 6, " "),
            F.rpad("Status", 4, " "), F.rpad("Name", 70, " "),
            F.rpad("ExID", 6, " "), F.rpad("ShOut", 13, " "),
            F.col("FirstTradeDate"), F.col("FirstTradeExchg"),
            F.rpad("Dividend", 12, " "), F.col("CoNameOrCIK"),
        )).alias("line"),
        F.col("quarter_id"), F.col("PTS"),
    )

    # =====================================================================
    # FIN: Financial records
    # =====================================================================
    # Per TPC-DI spec + DIGen UpdateBlackBox (97% new, 1% change, 2% delete):
    # - Each ACTIVE company produces one FIN per quarter after its creation
    # - Q0 companies are all genuinely new (all active)
    # - Q1+ CMP records: 97% are new companies, 3% are changes/deletes (not new companies)
    # - Additionally, 2% of cpq existing companies are deactivated each quarter
    # This reduces the active company pool growth to match DIGen's FIN counts.

    quarters_df = spark.range(0, cfg.fw_quarters).withColumnRenamed("id", "fin_quarter_id")

    cmp_q0_count = cfg.cmp_total - cfg.fw_quarters * cfg.cmp_per_quarter

    cmp_quarters = spark.table("_cmp_refs").select(
        F.col("_idx").alias("cmp_id"),
        "CIK", "CompanyName",
        F.col("creation_quarter"))

    # Model DIGen's UpdateBlackBox (97% new, 1% change, 2% delete) for FIN generation:
    # - Q0 companies: all genuinely new (active), so they all produce FIN records
    # - Q1+ companies: ~87.5% are new active companies at SF=10 (95% at SF=1000) The effective "active" rate accounts for: 3% non-new CMP records + cumulative deactivation of existing companies by the 2% delete mechanism.
    # Derived from DIGen: net_per_q / cmp_per_quarter ≈ 0.875 at SF=10, 0.95 at SF=1000 Use formula: active_rate = 1.0 - max(3, cmp_per_quarter * 0.05) / cmp_per_quarter
    _loss_per_q = max(3, int(cfg.cmp_per_quarter * 0.05))
    _active_pct = int(100 * (cfg.cmp_per_quarter - _loss_per_q) / cfg.cmp_per_quarter)

    # Mark which Q1+ companies are "genuinely new active" for FIN purposes. Q0 companies are always active; Q1+ companies pass with probability _active_pct%.
    cmp_quarters = cmp_quarters.withColumn("_is_active_for_fin",
        F.when(F.col("creation_quarter") == 0, F.lit(True))  # Q0: all active
         .otherwise(hash_key(F.col("cmp_id"), seed_for("CMP", "is_active")) % 100 < _active_pct))

    # Repartition cmp_quarters BEFORE the crossJoin so the downstream fan-out (each row × fw_quarters) runs across the executor pool. The default spark.range() partitioning that cmp_df inherits is small (8-16 parts on serverless), so without this each task holds 10-15 GB of in-memory FIN data at SF=10000 and spills.
    #
    # Target partitions = max(8, cfg.sf / 25). Sized so FIN data distributes enough to avoid spill (~1 GB/partition at SF=5000, ~1.2 GB at SF=10000) without monopolising the executor pool — the writer's maxRecordsPerFile option still splits each partition into multiple ~128 MB output files.
    fin_target_parts = max(8, cfg.sf // 25)
    log(f"[FINWIRE] repartitioning cmp_quarters to {fin_target_parts} partitions ahead of crossJoin", "DEBUG")
    cmp_quarters = cmp_quarters.repartition(fin_target_parts)

    # Cross join: only active companies x quarters after creation. Each active company produces one FIN record for every quarter after its creation. quarters_df is small (~202 rows) — optimizer will auto-broadcast under the 200MB threshold.
    fin_base = (cmp_quarters
        .filter(F.col("_is_active_for_fin"))
        .crossJoin(quarters_df)
        .filter(F.col("fin_quarter_id") > F.col("creation_quarter"))
        .withColumn("quarter_id", F.col("fin_quarter_id"))
    )

    # PTS: uniformly distributed within the calendar quarter of quarter_id. Matches DIGen's FinwirePTSGenerator exactly — each quarter_id maps to one calendar quarter, so Year/Quarter/QtrStartDate derived from pts_ts are always consistent per (cmp_id, quarter_id) → Financial has unique (sk_companyid, quarter, year) keys (no FactMarketHistory fan-out).
    fin_base = _add_calendar_quarter_timing(fin_base)
    fin_df = (fin_base
        .withColumn("_fin_seed", F.hash(F.col("cmp_id"), F.col("quarter_id")).cast("long"))
        .withColumn("_q_offset", F.abs(F.hash(F.col("cmp_id"), F.col("quarter_id"), F.lit(seed_for("FIN", "pts"))).cast("long")) % F.col("_q_duration_s"))
        .withColumn("pts_ts", (F.col("_q_start_s") + F.col("_q_offset")).cast("timestamp"))
        .withColumn("PTS", F.date_format("pts_ts", "yyyyMMdd-HHmmss"))
        # Revenue: random up to 10 billion
        .withColumn("_rev", (F.abs(F.hash(F.col("cmp_id"), F.col("quarter_id"), F.lit(seed_for("FIN", "rev"))).cast("long")) % 10000000000).cast("double") + 1.0)
        # Earnings ratio: 0-70% of revenue
        .withColumn("_earn_r", (F.abs(F.hash(F.col("cmp_id"), F.col("quarter_id"), F.lit(seed_for("FIN", "er"))).cast("long")) % 70) / 100.0)
        # Share counts: used both in EPS calc and in the ShOut/DilutedShOut output columns so FI_BASIC_EPS ≈ FI_NET_EARN / FI_OUT_BASIC (the automated_audit 'Financial EPS' check enforces a ±0.4 tolerance).
        .withColumn("_sh_out",
            (F.abs(F.hash(F.col("cmp_id"), F.col("quarter_id"), F.lit(seed_for("FIN", "sh"))).cast("long")) % 999900000 + 100000).cast("double"))
        .withColumn("_dil_sh_out",
            (F.abs(F.hash(F.col("cmp_id"), F.col("quarter_id"), F.lit(seed_for("FIN", "dsh"))).cast("long")) % 999900000 + 100000).cast("double"))
        .withColumn("_earn", F.col("_rev") * F.col("_earn_r"))
        # CoNameOrCIK: reference this FIN's own company (temporal integrity guaranteed since CMP exists by this quarter — FIN is always after creation_quarter)
        .withColumn("CoNameOrCIK",
            F.when(F.abs(F.hash(F.col("cmp_id"), F.col("quarter_id"), F.lit(seed_for("FIN", "cn"))).cast("long")) % 100 < 50,
                F.rpad(F.col("CIK"), 60, " "))
            .otherwise(F.rpad(F.col("CompanyName"), 60, " ")))
    )

    # Format FIN as fixed-width line. Fields include: Year, Quarter, QtrStartDate, PostingDate, Revenue, Earnings, EPS, DilutedEPS, Margin, Inventory, Assets, Liabilities, ShOut, DilutedShOut, CoNameOrCIK
    fin_lines = fin_df.select(
        F.rtrim(F.concat(
            F.rpad(F.col("PTS"), 15, " "), F.rpad(F.lit("FIN"), 3, " "),
            F.rpad(F.date_format("pts_ts", "yyyy"), 4, " "),
            F.rpad(F.quarter("pts_ts").cast("string"), 1, " "),
            F.rpad(F.concat(F.date_format("pts_ts", "yyyy"),
                F.when(F.quarter("pts_ts") == 1, F.lit("0101"))
                 .when(F.quarter("pts_ts") == 2, F.lit("0401"))
                 .when(F.quarter("pts_ts") == 3, F.lit("0701"))
                 .otherwise(F.lit("1001"))), 8, " "),
            F.rpad(F.date_format("pts_ts", "yyyyMMdd"), 8, " "),
            F.rpad(F.format_string("%.2f", F.col("_rev")), 17, " "),
            F.rpad(F.format_string("%.2f", F.col("_earn")), 17, " "),
            # EPS = earnings / _sh_out so FI_BASIC_EPS ≈ FI_NET_EARN / FI_OUT_BASIC
            F.rpad(F.format_string("%.2f", F.col("_earn") / F.col("_sh_out")), 12, " "),
            F.rpad(F.format_string("%.2f", F.col("_earn") / F.col("_dil_sh_out")), 12, " "),
            F.rpad(F.format_string("%.2f", F.col("_earn_r")), 12, " "),
            F.rpad(F.format_string("%.2f", (F.abs(F.hash(F.col("cmp_id"), F.col("quarter_id"), F.lit(seed_for("FIN", "inv"))).cast("long")) % 1000000000).cast("double")), 17, " "),
            F.rpad(F.format_string("%.2f", (F.abs(F.hash(F.col("cmp_id"), F.col("quarter_id"), F.lit(seed_for("FIN", "ast"))).cast("long")) % 1000000000000).cast("double")), 17, " "),
            F.rpad(F.format_string("%.2f", (F.abs(F.hash(F.col("cmp_id"), F.col("quarter_id"), F.lit(seed_for("FIN", "lia"))).cast("long")) % 10000000000).cast("double")), 17, " "),
            F.lpad(F.col("_sh_out").cast("long").cast("string"), 13, " "),
            F.lpad(F.col("_dil_sh_out").cast("long").cast("string"), 13, " "),
            F.col("CoNameOrCIK"),
        )).alias("line"),
        F.col("quarter_id"), F.col("PTS"),
    )

    # =====================================================================
    # Write CMP / SEC / FIN as separate staging dirs (in parallel).
    # =====================================================================
    # The bronze-layer FINWIRE ingest reads all FINWIRE_*.txt files as a single globbed input and distinguishes record type by the 3-char type code at byte position 16, so it's indifferent to which file contains which subset — we don't need to union CMP/SEC/FIN into one output stream. Splitting lets the 3 writes fan out across executors in parallel, and avoids a 244M-row union + columnar→row that previously dominated the FINWIRE stage.
    max_records = int(128 * 1024 * 1024 / 260)  # ~260 bytes per fixed-width line

    # FIN repartition happens earlier, right on cmp_quarters before the crossJoin — see the block there. Doing it here (just before .write) would shuffle after the in-memory 475M-row compute had already been forced onto the narrow spark.range default partitioning, which is what causes the 15GB/task spill. CMP/SEC are small enough to use default partitioning and write directly.
    def _write_subset(df, label):
        staging = f"{cfg.batch_path(1)}/FINWIRE_{label}.txt__staging"
        _cleanup(staging, dbutils)
        df.select("line").write.mode("overwrite") \
            .option("maxRecordsPerFile", max_records).text(staging)
        return staging

    import concurrent.futures as _cf
    with _cf.ThreadPoolExecutor(max_workers=3) as ex:
        futs = {
            "cmp": ex.submit(_write_subset, cmp_lines, "cmp"),
            "sec": ex.submit(_write_subset, sec_lines, "sec"),
            "fin": ex.submit(_write_subset, fin_lines, "fin"),
        }
        staging_dirs = {k: f.result() for k, f in futs.items()}

    # Copy to final FINWIRE_{N}.txt using a shared counter so numbering never collides across the three subsets. Order here is arbitrary — downstream reads all files at once.
    next_idx = 1
    for subset in ("cmp", "sec", "fin"):
        _, next_idx = register_copies_from_staging(
            staging_dirs[subset], f"{cfg.batch_path(1)}/FINWIRE.txt",
            dbutils, start_idx=next_idx)

    # Analytical estimates for FW_CMP / FW_SEC / FW_FIN audit attributes: CMP ≈ cmp_total × 1.03 (NEW + ~3% CHANGE/DELETE extras, excludes last quarter) SEC ≈ sec_total × 1.03 FIN ≈ _active_pct × cmp_per_quarter × fw_quarters × (fw_quarters-1)/2 / fw_quarters + cmp_q0_count × (fw_quarters-1) For dynamic audit regeneration, compute exact values below.
    last_q_exclusion = (cfg.fw_quarters - 1) / cfg.fw_quarters  # ~99.5%
    fw_cmp_extras_est = int(cfg.cmp_total * 0.03 * last_q_exclusion)
    fw_sec_extras_est = int(cfg.sec_total * 0.03 * last_q_exclusion)
    fw_cmp_exact = cfg.cmp_total + fw_cmp_extras_est
    fw_sec_exact = cfg.sec_total + fw_sec_extras_est
    # Active-FIN analytical span: Q0 companies (all active, all fw_quarters-1 FIN records each) + Qk companies (active_pct% each, averaging fewer FIN rows).
    cmp_q0_count = cfg.cmp_total - cfg.fw_quarters * cfg.cmp_per_quarter
    _loss_per_q = max(3, int(cfg.cmp_per_quarter * 0.05))
    _active_pct = (cfg.cmp_per_quarter - _loss_per_q) / max(1, cfg.cmp_per_quarter)
    fw_fin_q0 = cmp_q0_count * (cfg.fw_quarters - 1)
    fw_fin_qn = int(cfg.cmp_per_quarter * _active_pct * (cfg.fw_quarters - 1) * (cfg.fw_quarters - 2) / 2)
    fw_fin_exact = fw_fin_q0 + fw_fin_qn
    total = fw_cmp_exact + fw_sec_exact + fw_fin_exact

    if not static_audits_available(cfg):
        # Dynamic audit regeneration: exact counts. Extras counts are over tiny DFs (~3% of cmp_total/sec_total); the FIN analytical aggregation scans cmp_total rows instead of fin_total (10+ min savings).
        log("[FINWIRE] dynamic audit path — computing exact CMP/SEC extras + FIN span", "DEBUG")
        fw_cmp_extras = cmp_extras.count()
        fw_sec_extras = sec_extras.count()
        fw_cmp_exact = cfg.cmp_total + fw_cmp_extras
        fw_sec_exact = cfg.sec_total + fw_sec_extras
        fw_fin_exact = cmp_quarters.select(
            F.sum(F.when(F.col("_is_active_for_fin"),
                         F.lit(cfg.fw_quarters - 1) - F.col("creation_quarter"))
                   .otherwise(F.lit(0))).alias("fin")
        ).first()["fin"] or 0
        total = fw_cmp_exact + fw_sec_exact + fw_fin_exact
    log(f"[FINWIRE] ~{total} records (CMP={fw_cmp_exact}, SEC={fw_sec_exact}, FIN={fw_fin_exact})")
    log("[FINWIRE] Generation complete")
    return {"counts": {
        ("FINWIRE", 1): total,
        ("FW_CMP", 1): fw_cmp_exact,
        ("FW_SEC", 1): fw_sec_exact,
        ("FW_FIN", 1): fw_fin_exact,
    }}
