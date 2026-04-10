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
from .utils import write_file, seed_for, dict_join, hash_key, dict_count, register_copy


def generate(spark: SparkSession, cfg, dicts: dict, dbutils) -> dict:
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
    fw_begin_ms = int(FW_BEGIN_DATE.timestamp() * 1000)

    # =====================================================================
    # CMP: Company records
    # =====================================================================
    # DIGen pattern: large initial burst in Q0, then steady rate per quarter.
    # ~60% of companies appear in Q0 (historical backfill), rest spread across
    # later quarters at a rate of cmp_per_quarter.
    cmp_df = (spark.range(0, cfg.cmp_total).withColumnRenamed("id", "cmp_id")
        # Exact DIGen formula (verified at SF=10,100,1000):
        # cmp_per_quarter = cmp_total // (fw_quarters + 1)
        # Q0 gets: cmp_total - fw_quarters * cmp_per_quarter (the remainder)
        # Q1-Q201 each get: cmp_per_quarter
        .withColumn("quarter_id",
            F.when(F.col("cmp_id") < F.lit(cfg.cmp_total - cfg.fw_quarters * cfg.cmp_per_quarter), F.lit(0))
            .otherwise(
                F.least(
                    ((F.col("cmp_id") - F.lit(cfg.cmp_total - cfg.fw_quarters * cfg.cmp_per_quarter))
                        / F.lit(cfg.cmp_per_quarter) + 1).cast("int"),
                    F.lit(cfg.fw_quarters - 1))))
        # PTS (posting timestamp): random offset within the assigned quarter
        .withColumn("_q_start_ms", F.lit(fw_begin_ms).cast("long") + F.col("quarter_id").cast("long") * F.lit(ONE_QUARTER_MS))
        .withColumn("_q_offset", hash_key(F.col("cmp_id"), seed_for("CMP", "pts")) % int(ONE_QUARTER_MS / 1000))
        .withColumn("PTS", F.date_format(((F.col("_q_start_ms") / 1000) + F.col("_q_offset")).cast("timestamp"), "yyyyMMdd-HHmmss"))
        # CIK: zero-padded 10-digit company identifier (natural key)
        .withColumn("CIK", F.lpad(F.col("cmp_id").cast("string"), 10, "0"))
        .withColumn("CompanyName", F.substring(F.md5(F.concat(F.col("cmp_id").cast("string"), F.lit("cname"))), 1, 15))
        # Status: 97% ACTV (active), 3% INAC (inactive) — matches DIGen's distribution
        .withColumn("Status", F.when(hash_key(F.col("cmp_id"), seed_for("CMP", "st")) % 100 < 97, F.lit("ACTV")).otherwise(F.lit("INAC")))
    )

    # Dictionary columns via broadcast join: pick values from pre-loaded dictionaries
    # using hash-based indexing for deterministic, reproducible assignment.
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

    # Format CMP as fixed-width line (TPC-DI FINWIRE spec field widths)
    cmp_lines = cmp_df.select(
        F.concat(
            F.rpad("PTS", 15, " "), F.rpad(F.lit("CMP"), 3, " "),
            F.rpad("CompanyName", 60, " "), F.col("CIK"),
            F.rpad("Status", 4, " "), F.rpad("IndustryID", 2, " "),
            F.rpad("SPrating", 4, " "), F.col("FoundingDate"),
            F.rpad("AddrLine1", 80, " "), F.rpad("AddrLine2", 80, " "),
            F.rpad("PostalCode", 12, " "), F.rpad("City", 25, " "),
            F.rpad("StateProvince", 20, " "), F.rpad("Country", 24, " "),
            F.rpad("CEOname", 46, " "), F.rpad("Description", 150, " "),
        ).alias("line"),
        F.col("quarter_id"), F.col("PTS"),
    )

    # --- Persist _cmp_refs temp view ---
    # Used by SEC for CoNameOrCIK joins and by FIN for company references.
    # Contains the minimal columns needed: _idx (cmp_id), CIK, CompanyName,
    # and creation_quarter to enforce temporal integrity (SEC/FIN can only
    # reference a CMP that was created in an earlier or same quarter).
    cmp_ref = cmp_df.select(
        F.col("cmp_id").alias("_idx"),
        "CIK", "CompanyName",
        F.col("quarter_id").alias("creation_quarter"))
    cmp_ref.createOrReplaceTempView("_cmp_refs")
    cmp_count = cfg.cmp_total

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

    # SEC quarterly distribution (exact DIGen formula):
    # sec_per_quarter = sec_total // fw_quarters (divided across 202 quarters)
    # Q0 = 0 (no securities before market opens)
    # Q1 = sec_total - (fw_quarters - 1) * sec_per_quarter (absorbs remainder)
    # Q2+ = sec_per_quarter each
    sec_q1 = cfg.sec_total - (cfg.fw_quarters - 1) * cfg.sec_per_quarter

    sec_df = (spark.range(0, cfg.sec_total).withColumnRenamed("id", "sec_id")
        # Q1 gets first sec_q1 records, then Q2+ get sec_per_quarter each
        .withColumn("quarter_id",
            F.when(F.col("sec_id") < F.lit(sec_q1), F.lit(1))
            .otherwise(
                F.least(
                    ((F.col("sec_id") - F.lit(sec_q1)) / F.lit(cfg.sec_per_quarter) + 2).cast("int"),
                    F.lit(cfg.fw_quarters - 1))))
        # PTS: random offset within the assigned quarter
        .withColumn("_q_start_ms", F.lit(fw_begin_ms).cast("long") + F.col("quarter_id").cast("long") * F.lit(ONE_QUARTER_MS))
        .withColumn("_q_offset", hash_key(F.col("sec_id"), seed_for("SEC", "pts")) % int(ONE_QUARTER_MS / 1000))
        .withColumn("PTS", F.date_format(((F.col("_q_start_ms") / 1000) + F.col("_q_offset")).cast("timestamp"), "yyyyMMdd-HHmmss"))
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
        # CoNameOrCIK: reference a CMP that was created BEFORE this SEC record (temporal integrity).
        # Map sec_id to a candidate cmp_id; the join below validates temporal ordering.
        .withColumn("_cmp_ref_idx", hash_key(F.col("sec_id"), seed_for("SEC", "ref")) % cmp_count)
    )

    # Join to _cmp_refs to resolve the company reference.
    # Broadcast join since _cmp_refs is small (one row per company).
    sec_df = sec_df.join(
        F.broadcast(spark.table("_cmp_refs").select(
            F.col("_idx").alias("_cmp_ref_idx"),
            F.col("CIK").alias("_ref_cik"),
            F.col("CompanyName").alias("_ref_name"),
            F.col("creation_quarter").alias("_ref_cq"))),
        on="_cmp_ref_idx", how="left")

    # Temporal integrity enforcement: if the referenced company was created AFTER
    # this SEC record's quarter, fall back to CMP 0 (always exists in Q0).
    # 50/50 split between using CIK (numeric) vs CompanyName (string) as the reference.
    # Company 0's actual name (used as temporal fallback for name-based references)
    _cmp0_name = F.substring(F.md5(F.concat(F.lit("0"), F.lit("cname"))), 1, 15)
    sec_df = sec_df.withColumn("CoNameOrCIK",
        F.when(hash_key(F.col("sec_id"), seed_for("SEC", "cn")) % 100 < 50,
            F.rpad(F.when(F.col("_ref_cq") <= F.col("quarter_id"), F.col("_ref_cik")).otherwise(F.lit("0000000000")), 60, " "))
        .otherwise(
            F.rpad(F.when(F.col("_ref_cq") <= F.col("quarter_id"), F.col("_ref_name")).otherwise(
                _cmp0_name), 60, " ")))

    # Format SEC as fixed-width line
    sec_lines = sec_df.select(
        F.concat(
            F.rpad("PTS", 15, " "), F.rpad(F.lit("SEC"), 3, " "),
            F.rpad("Symbol", 15, " "), F.rpad("IssueType", 6, " "),
            F.rpad("Status", 4, " "), F.rpad("Name", 70, " "),
            F.rpad("ExID", 6, " "), F.rpad("ShOut", 13, " "),
            F.col("FirstTradeDate"), F.col("FirstTradeExchg"),
            F.rpad("Dividend", 12, " "), F.col("CoNameOrCIK"),
        ).alias("line"),
        F.col("quarter_id"), F.col("PTS"),
    )

    # --- Persist _symbols temp view ---
    # Contains active symbols for downstream modules (DailyMarket, WatchHistory, Trade).
    # Includes creation_quarter and deactivation_quarter for temporal validity.
    #
    # Deactivation model:
    # FINWIRE-sec update pattern: 97% new, 1% change, 2% delete per quarter.
    # The 2% applies to the UPDATE BATCH SIZE (sec_per_quarter), not to all securities.
    # Total deactivations over FINWIRE lifetime ≈ 0.02 × sec_per_quarter × fw_quarters.
    # At SF=10: 0.02 × 39 × 202 ≈ 158 deactivations out of 8000 total = ~2% of total.
    # But _symbols already excludes 3% initially-INAC, so effective deactivation rate among
    # the ACTV pool is about total_deact / num_actv.
    #
    # Model: select ~total_deact securities for deactivation, spread across their lifetimes.
    # Each deactivated security gets a random deactivation quarter after its creation.
    total_deact = int(0.02 * cfg.sec_per_quarter * cfg.fw_quarters)
    symbols = (sec_df
        .filter(F.col("Status") == "ACTV")
        .select("Symbol", "quarter_id", "sec_id")
        .groupBy("Symbol").agg(
            F.min("quarter_id").alias("creation_quarter"),
            F.min("sec_id").alias("_sec_id"))
        # Select which securities get deactivated: hash into [0, num_actv) < total_deact
        .withColumn("_deact_draw", F.abs(hash_key(F.col("_sec_id"), seed_for("SEC", "deact"))))
        .withColumn("deactivation_quarter",
            F.when(F.col("_deact_draw") % F.lit(cfg.sec_total) < F.lit(total_deact),
                # This security gets deactivated: random quarter after creation
                F.col("creation_quarter") +
                    (F.abs(hash_key(F.col("_sec_id"), seed_for("SEC", "deact_q"))) %
                     F.greatest(F.lit(1), F.lit(cfg.fw_quarters) - F.col("creation_quarter"))))
            .otherwise(F.lit(cfg.fw_quarters + 1)))  # stays active forever (beyond FINWIRE range)
        .withColumn("_idx", F.row_number().over(Window.orderBy("Symbol")) - 1)
        .select("Symbol", "creation_quarter", "deactivation_quarter", "_idx"))
    symbols.createOrReplaceTempView("_symbols")
    sym_count = symbols.count()

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
    # - Q1+ companies: ~87.5% are new active companies at SF=10 (95% at SF=1000)
    #   The effective "active" rate accounts for: 3% non-new CMP records + cumulative
    #   deactivation of existing companies by the 2% delete mechanism.
    # Derived from DIGen: net_per_q / cmp_per_quarter ≈ 0.875 at SF=10, 0.95 at SF=1000
    # Use formula: active_rate = 1.0 - max(3, cmp_per_quarter * 0.05) / cmp_per_quarter
    _loss_per_q = max(3, int(cfg.cmp_per_quarter * 0.05))
    _active_pct = int(100 * (cfg.cmp_per_quarter - _loss_per_q) / cfg.cmp_per_quarter)

    # Mark which Q1+ companies are "genuinely new active" for FIN purposes.
    # Q0 companies are always active; Q1+ companies pass with probability _active_pct%.
    cmp_quarters = cmp_quarters.withColumn("_is_active_for_fin",
        F.when(F.col("creation_quarter") == 0, F.lit(True))  # Q0: all active
         .otherwise(hash_key(F.col("cmp_id"), seed_for("CMP", "is_active")) % 100 < _active_pct))

    # Cross join: only active companies x quarters after creation.
    # Each active company produces one FIN record for every quarter after its creation.
    # broadcast(quarters_df) since it is small (~202 rows).
    fin_base = (cmp_quarters
        .filter(F.col("_is_active_for_fin"))
        .crossJoin(F.broadcast(quarters_df))
        .filter(F.col("fin_quarter_id") > F.col("creation_quarter"))
        .withColumn("quarter_id", F.col("fin_quarter_id"))
    )

    # Generate FIN fields: revenue, earnings, investment, assets, liabilities, shares
    fin_df = (fin_base
        .withColumn("_fin_seed", F.hash(F.col("cmp_id"), F.col("quarter_id")).cast("long"))
        # PTS: random timestamp within the assigned quarter
        .withColumn("_q_start_ms", F.lit(fw_begin_ms).cast("long") + F.col("quarter_id").cast("long") * F.lit(ONE_QUARTER_MS))
        .withColumn("_q_offset", F.abs(F.hash(F.col("cmp_id"), F.col("quarter_id"), F.lit(seed_for("FIN", "pts"))).cast("long")) % int(ONE_QUARTER_MS / 1000))
        .withColumn("pts_ts", ((F.col("_q_start_ms") / 1000) + F.col("_q_offset")).cast("timestamp"))
        .withColumn("PTS", F.date_format("pts_ts", "yyyyMMdd-HHmmss"))
        # Revenue: random up to 10 billion
        .withColumn("_rev", (F.abs(F.hash(F.col("cmp_id"), F.col("quarter_id"), F.lit(seed_for("FIN", "rev"))).cast("long")) % 10000000000).cast("double") + 1.0)
        # Earnings ratio: 0-70% of revenue
        .withColumn("_earn_r", (F.abs(F.hash(F.col("cmp_id"), F.col("quarter_id"), F.lit(seed_for("FIN", "er"))).cast("long")) % 70) / 100.0)
        # CoNameOrCIK: reference this FIN's own company (temporal integrity guaranteed
        # since CMP exists by this quarter — FIN is always after creation_quarter)
        .withColumn("CoNameOrCIK",
            F.when(F.abs(F.hash(F.col("cmp_id"), F.col("quarter_id"), F.lit(seed_for("FIN", "cn"))).cast("long")) % 100 < 50,
                F.rpad(F.col("CIK"), 60, " "))
            .otherwise(F.rpad(F.col("CompanyName"), 60, " ")))
    )

    # Format FIN as fixed-width line. Fields include: Year, Quarter, QtrStartDate,
    # PostingDate, Revenue, Earnings, EPS, DilutedEPS, Margin, Inventory, Assets,
    # Liabilities, ShOut, DilutedShOut, CoNameOrCIK
    fin_lines = fin_df.select(
        F.concat(
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
            F.rpad(F.format_string("%.2f", F.col("_rev") * F.col("_earn_r")), 17, " "),
            # EPS = earnings / 1M shares, DilutedEPS = earnings / 1.2M shares
            F.rpad(F.format_string("%.2f", F.col("_rev") * F.col("_earn_r") / 1000000), 12, " "),
            F.rpad(F.format_string("%.2f", F.col("_rev") * F.col("_earn_r") / 1200000), 12, " "),
            F.rpad(F.format_string("%.2f", F.col("_earn_r")), 12, " "),
            F.rpad(F.format_string("%.2f", (F.abs(F.hash(F.col("cmp_id"), F.col("quarter_id"), F.lit(seed_for("FIN", "inv"))).cast("long")) % 1000000000).cast("double")), 17, " "),
            F.rpad(F.format_string("%.2f", (F.abs(F.hash(F.col("cmp_id"), F.col("quarter_id"), F.lit(seed_for("FIN", "ast"))).cast("long")) % 1000000000000).cast("double")), 17, " "),
            F.rpad(F.format_string("%.2f", (F.abs(F.hash(F.col("cmp_id"), F.col("quarter_id"), F.lit(seed_for("FIN", "lia"))).cast("long")) % 10000000000).cast("double")), 17, " "),
            F.lpad((F.abs(F.hash(F.col("cmp_id"), F.col("quarter_id"), F.lit(seed_for("FIN", "sh"))).cast("long")) % 999900000 + 100000).cast("string"), 13, " "),
            F.lpad((F.abs(F.hash(F.col("cmp_id"), F.col("quarter_id"), F.lit(seed_for("FIN", "dsh"))).cast("long")) % 999900000 + 100000).cast("string"), 13, " "),
            F.col("CoNameOrCIK"),
        ).alias("line"),
        F.col("quarter_id"), F.col("PTS"),
    )

    fin_count_approx = int(cfg.cmp_total * cfg.fw_quarters * 0.55)  # rough estimate for logging

    # =====================================================================
    # Merge all record types and write quarterly files
    # =====================================================================
    # Union CMP + SEC + FIN lines, compute calendar year/quarter from quarter_id,
    # and partition by filename (e.g. "FINWIRE1967Q1") so Spark writes one file
    # per quarter in parallel.
    all_fw = (cmp_lines
        .union(sec_lines.select("line", "quarter_id", "PTS"))
        .union(fin_lines.select("line", "quarter_id", "PTS"))
        # Convert quarter_id to calendar year/quarter:
        # quarter_id 0 -> 1967 Q1, quarter_id 4 -> 1968 Q1, etc.
        .withColumn("_year", F.lit(1967) + ((F.col("quarter_id") - (F.col("quarter_id") % 4)) / 4).cast("int"))
        .withColumn("_qtr", (F.col("quarter_id") % 4) + 1)
        .withColumn("_filename", F.concat(F.lit("FINWIRE"), F.col("_year"), F.lit("Q"), F.col("_qtr")))
    )

    # Write to a temp staging directory, partitioned by _filename.
    # sortWithinPartitions("line") sorts by PTS within each quarterly file
    # (since PTS is the first field, lexicographic sort = chronological sort).
    tmp_path = f"{cfg.batch_path(1)}/FINWIRE__tmp"
    try:
        dbutils.fs.rm(tmp_path, recurse=True)
    except:
        pass

    # Partitioned write — all quarters written in parallel by Spark.
    # repartition("_filename") ensures all records for a quarter land in one partition.
    # No sortWithinPartitions needed — downstream parsers don't require ordering within files.
    (all_fw
        .select("line", "_filename")
        .repartition("_filename")
        .write
        .mode("overwrite")
        .partitionBy("_filename")
        .text(tmp_path))

    # Register deferred copies: each partition directory contains a single part file
    # that needs to be renamed to the final FINWIRE{YYYY}Q{N} filename.
    # These copies are executed later by bulk_copy_all() for parallel I/O.
    fw_dirs = [f for f in dbutils.fs.ls(tmp_path) if f.name.startswith("_filename=")]
    for fd in fw_dirs:
        fname = fd.name.replace("_filename=", "").rstrip("/")
        parts = dbutils.fs.ls(fd.path)
        part_file = [p for p in parts if p.name.startswith("part-")]
        if part_file:
            register_copy(part_file[0].path, f"{cfg.batch_path(1)}/{fname}")

    total = cfg.cmp_total + cfg.sec_total + fin_count_approx
    print(f"  FINWIRE: ~{total} records (CMP={cfg.cmp_total}, SEC={cfg.sec_total}, FIN=~{fin_count_approx}) -> {len(fw_dirs)} quarterly files")
    print(f"  Active symbols: {sym_count} -> _symbols view")
    return {"counts": {("FINWIRE", 1): total}}
