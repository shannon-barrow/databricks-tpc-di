"""TPC-DI Data Generation Configuration and Constants.

This module defines all scale-independent constants and the ScaleConfig class
that computes scale-dependent row counts and parameters for the TPC-DI
(Transaction Processing Performance Council - Decision Support Integration)
benchmark data generator.

The constants and formulas here replicate the behavior of the official Java-based
DIGen.jar tool (see TPC-DI Specification v1.1.0, Section 4 "Scaling and Database
Population"). The goal is to produce identical output from a distributed PySpark
implementation.

Terminology:
  - SF (Scale Factor): The user-facing scale factor (e.g., 10, 100, 1000).
    This is the number that appears in the benchmark name (e.g., "SF=100").
  - internal_sf: SF * 1000. DIGen internally multiplies the scale factor by 1000
    to compute row counts. All row-count formulas use internal_sf as their base.
  - Batch: DIGen produces a "historical load" (Batch 1) plus incremental batches
    (Batch 2, 3). Each incremental batch represents one day of new activity.
"""

from datetime import datetime, timedelta

# ---------------------------------------------------------------------------
# Global seed used for all deterministic random generation.
# Must match DIGen to produce reproducible, comparable output.
# ---------------------------------------------------------------------------
GLOBAL_SEED = 1234567890

# ---------------------------------------------------------------------------
# Number of incremental update batches beyond the historical load (Batch 1).
# DIGen Spec Section 4.3: "Two additional batches of incremental data."
# Batch 2 and Batch 3 each represent one additional day of market activity.
# ---------------------------------------------------------------------------
NUM_INCREMENTAL_BATCHES = 2

# ---------------------------------------------------------------------------
# Key dates defining the temporal boundaries of generated data.
#
# FIRST_BATCH_DATE: The "current date" for the historical load. All historical
#   data is generated as if this is "today" (DIGen: batchDate).
# FIRST_BATCH_DATE_END: One day after FIRST_BATCH_DATE; used as the exclusive
#   upper bound for date ranges (DIGen: batchDateEnd = batchDate + 1 day).
# DATE_BEGIN / DATE_END: The full range of the Date dimension table (DimDate).
#   Covers 1950-01-01 to 2021-01-01 (~71 years of calendar rows).
# ---------------------------------------------------------------------------
FIRST_BATCH_DATE = datetime(2017, 7, 7)
FIRST_BATCH_DATE_END = datetime(2017, 7, 8)
DATE_BEGIN = datetime(1950, 1, 1)
DATE_END = datetime(2021, 1, 1)

# ---------------------------------------------------------------------------
# FINWIRE file dates.
#
# FINWIRE files simulate quarterly financial news wire feeds. They span from
# FW_BEGIN_DATE (50 years before FIRST_BATCH_DATE, approximated as 1967-07-07)
# through FIRST_BATCH_DATE, covering ~202 quarters.
#
# ONE_QUARTER_MS: Duration of one calendar quarter in milliseconds
#   (365.25/4 * 86400 * 1000 = 7,889,400,000 ms). Used by DIGen to compute
#   quarter boundaries from epoch timestamps.
# ---------------------------------------------------------------------------
FW_BEGIN_DATE = datetime(1967, 7, 7)
FW_END_DATE = FIRST_BATCH_DATE
ONE_QUARTER_MS = 7889400000

# ---------------------------------------------------------------------------
# Trade dates.
#
# Trades span ~5 years (1826 days ~ 365.25 * 5) before FIRST_BATCH_DATE.
# DIGen: TradeBeginDate = computeDate(FIRST_BATCH_DATE, Calendar.DATE, -1826)
# This gives approximately 5 years of historical trade activity.
# ---------------------------------------------------------------------------
# DIGen: THistBeginDate = computeDate(FIRST_BATCH_DATE_START, Calendar.DATE, -1826)
# Java Calendar.DATE subtraction includes the start day, giving 2012-07-06 (not 2012-07-07).
# Python timedelta(1827) matches Java's computeDate(..., -1826).
# Trade placement range extends through incremental batch dates — DIGen generates trades
# for ALL days including the incremental period. The batch cutoff determines which go
# to Batch1 vs Batch2/3. Trades placed on incremental batch dates produce incremental
# Trade entries + CT/HH in those batches.
TRADE_BEGIN_DATE = FIRST_BATCH_DATE - timedelta(days=1827)
TRADE_END_DATE = FIRST_BATCH_DATE_END + timedelta(days=NUM_INCREMENTAL_BATCHES)

# ---------------------------------------------------------------------------
# DailyMarket dates.
#
# DailyMarket covers 2 years before FIRST_BATCH_DATE, minus one day.
# DIGen computes:
#   DMBeginDate = computeDate(FIRST_BATCH_DATE_START, Calendar.YEAR, -2) - ONE_DAY
#               = 2015-07-07 - 1 day = 2015-07-06
#   DMEndDate   = FIRST_BATCH_DATE - ONE_DAY
#               = 2017-07-07 - 1 day = 2017-07-06
# Batch 1 historical covers [DM_BEGIN_DATE, DM_END_DATE] inclusive.
# 2017-07-07 is handled by the Batch 2 incremental (inc_date = FIRST_BATCH_DATE).
# The automated_audit 'FactMarketHistory SK_DateID' check requires every
# Batch 1 row to have date < LastDay (=FIRST_BATCH_DATE), so DM_END_DATE
# must be strictly less than FIRST_BATCH_DATE.
# ---------------------------------------------------------------------------
DM_BEGIN_DATE = datetime(FIRST_BATCH_DATE.year - 2, FIRST_BATCH_DATE.month, FIRST_BATCH_DATE.day) - timedelta(days=1)
DM_END_DATE = FIRST_BATCH_DATE - timedelta(days=1)

# ---------------------------------------------------------------------------
# WatchHistory dates.
#
# Customer watch lists span the same 5-year window as trades (1826 days).
# DIGen: WHBeginDate = TradeBeginDate, WHEndDate = TradeEndDate.
# ---------------------------------------------------------------------------
WH_BEGIN_DATE = FIRST_BATCH_DATE - timedelta(days=1826)
WH_END_DATE = FIRST_BATCH_DATE

# ---------------------------------------------------------------------------
# CustomerMgmt (CustomerMgmt.xml) dates.
#
# Customer management activity spans ~10 years (3652 days ~ 365.25 * 10)
# before FIRST_BATCH_DATE through FIRST_BATCH_DATE_END.
# This represents the full lifecycle of customer account creation, updates,
# and closures leading up to the benchmark date.
# ---------------------------------------------------------------------------
CM_BEGIN_DATE = FIRST_BATCH_DATE - timedelta(days=3652)
CM_END_DATE = FIRST_BATCH_DATE_END

# ---------------------------------------------------------------------------
# Status types used across multiple entities (trades, accounts, etc.).
# DIGen Spec Table 4-1: StatusType reference data.
# These are loaded into the StatusType dimension table.
# ---------------------------------------------------------------------------
STATUS_IDS = ["ACTV", "CMPT", "CNCL", "PNDG", "SBMT", "INAC"]
STATUS_NAMES = ["Active", "Completed", "Canceled", "Pending", "Submitted", "Inactive"]

# ---------------------------------------------------------------------------
# Trade types defining the five kinds of trades in the benchmark.
# Format: (trade_type_id, trade_type_name, is_sell, is_market_order)
# DIGen Spec Table 4-2: TradeType reference data.
#   TMB = Market Buy   (not a sell, is a market order)
#   TMS = Market Sell   (is a sell, is a market order)
#   TSL = Stop Loss     (is a sell, is a market order)
#   TLS = Limit Sell    (is a sell, not a market order)
#   TLB = Limit Buy     (not a sell, not a market order)
# ---------------------------------------------------------------------------
TRADETYPE_DATA = [
    ("TMB", "Market Buy", "0", "1"),
    ("TMS", "Market Sell", "1", "1"),
    ("TSL", "Stop Loss", "1", "1"),
    ("TLS", "Limit Sell", "1", "0"),
    ("TLB", "Limit Buy", "0", "0"),
]

# ---------------------------------------------------------------------------
# Standard & Poor's credit rating codes.
# Used in Security and Company records to simulate credit ratings.
# Ordered from best (AAA) to worst (D), matching real-world S&P scale.
# ---------------------------------------------------------------------------
SP_RATINGS = [
    "AAA", "AA+", "AA", "AA-", "A+", "A", "A-",
    "BBB+", "BBB", "BBB-", "BB+", "BB", "BB-",
    "B+", "B", "B-", "CCC+", "CCC", "CCC-", "CC", "C", "D",
]

# ---------------------------------------------------------------------------
# Stock exchange names. Securities are distributed across these four exchanges.
# ---------------------------------------------------------------------------
EXCHANGES = ["NYSE", "NASDAQ", "AMEX", "PCX"]

# ---------------------------------------------------------------------------
# Security issue types. Each security is assigned one of these types.
# COMMON = common stock; PREF_A through PREF_D = preferred stock classes.
# ---------------------------------------------------------------------------
ISSUE_TYPES = ["COMMON", "PREF_A", "PREF_B", "PREF_C", "PREF_D"]

# ---------------------------------------------------------------------------
# HR (Human Resources) job codes for generating the HR.csv file.
#
# BROKER_JOBCODE (314): The single job code that identifies employees who are
#   brokers. Brokers are referenced by trades (t_exec_name).
# OTHER_JOBCODES: Non-broker job codes for remaining HR employees.
# BROKER_PCT: ~30% of HR employees are brokers (DIGen: brokerPct = 0.3).
# ---------------------------------------------------------------------------
BROKER_JOBCODE = 314
OTHER_JOBCODES = [647, 47, 364, 48, 393, 266, 534, 658, 262]
BROKER_PCT = 0.3

# ---------------------------------------------------------------------------
# File size soft limit for splitting large output files.
# DIGen splits files when they approach ~128 MB. We use 140 MB as a soft
# limit to allow up to ~10% overshoot before triggering a new file split.
# This mainly affects DailyMarket and Trade files at large scale factors.
# ---------------------------------------------------------------------------
MAX_FILE_BYTES = 140 * 1024 * 1024

# ---------------------------------------------------------------------------
# Standard TPC-DI scale factors as defined in the specification.
# The benchmark is officially defined for these SFs; other values may work
# but are not part of the formal benchmark.
# ---------------------------------------------------------------------------
STANDARD_SCALE_FACTORS = [10, 100, 1000, 5000, 10000, 20000]


class ScaleConfig:
    """Compute all scale-dependent parameters for TPC-DI data generation.

    The TPC-DI specification (Section 4.1 "Scaling Parameters") defines row counts
    as linear functions of the scale factor. DIGen internally uses
    ``internal_sf = SF * 1000`` as the multiplier base. All row-count formulas
    below derive from the DIGen Java source and have been verified to produce
    matching audit counts at SF=10, SF=100, and SF=1000.

    Relationship between sf, internal_sf, and row counts:
        sf          -- User-facing scale factor (e.g. 10, 100, 1000).
        internal_sf -- sf * 1000. This is the actual multiplier used in formulas.
                       For SF=10: internal_sf = 10,000
                       For SF=100: internal_sf = 100,000
                       For SF=1000: internal_sf = 1,000,000

    Example at SF=10 (internal_sf=10,000):
        hr_rows        = 5 * 10,000     =    50,000  HR employees
        cmp_total      = 0.5 * 10,000   =     5,000  companies
        sec_total      = 0.8 * 10,000   =     8,000  securities
        num_customers  = 0.005 * 10,000 =        50  active customers
        num_accounts   = 0.01 * 10,000  =       100  brokerage accounts
        trade_total    = 130 * 10,000   = 1,300,000  historical trades
        prospect_total = 5 * 10,000     =    50,000  prospect records
        wh_total       = 300 * 10,000   = 3,000,000  watch history entries

    Args:
        scale_factor: The benchmark scale factor (e.g. 10, 100, 1000).
        catalog: Unity Catalog catalog name for constructing output Volume paths.
    """

    def __init__(self, scale_factor: int, catalog: str):
        self.sf = scale_factor
        # DIGen multiplies the user-facing SF by 1000 to get the internal scaling
        # base. All row counts are expressed as a coefficient times internal_sf.
        # See DIGen source: DataGenParameters.java, "sf = scaleFactor * 1000".
        self.internal_sf = scale_factor * 1000
        self.catalog = catalog

        # Output paths - write to spark_datagen subfolder to avoid clobbering DIGen output
        self.volume_path = f"/Volumes/{catalog}/tpcdi_raw_data/tpcdi_volume/spark_datagen/sf={scale_factor}"
        self.batch_path = lambda b: f"{self.volume_path}/Batch{b}"

        # ----- Row counts -----
        # Each formula: coefficient * internal_sf
        # Coefficients come from DIGen's DataGenParameters.java constants.

        # HR.csv: Total employees in the brokerage firm.
        # DIGen: HRScaling = 5 => 5 * internal_sf rows.
        self.hr_rows = 5 * self.internal_sf

        # Companies: Total number of publicly traded companies.
        # DIGen: CMPScaling = 0.5 => int(0.5 * internal_sf).
        self.cmp_total = int(0.5 * self.internal_sf)

        # Securities: Total number of securities (stocks) across all companies.
        # DIGen: SECScaling = 0.8 => int(0.8 * internal_sf).
        # Roughly 1.6 securities per company (some companies have multiple issues).
        self.sec_total = int(0.8 * self.internal_sf)

        # Customers: Total active retail customers.
        # DIGen: CustomerScaling = 0.005 => int(0.005 * internal_sf).
        self.num_customers = int(0.005 * self.internal_sf)

        # Accounts: Total brokerage accounts (roughly 2 per customer).
        # DIGen: AccountScaling = 0.01 => int(0.01 * internal_sf).
        self.num_accounts = int(0.01 * self.internal_sf)

        # CustomerMgmt.xml: Total customer management actions over the 10-year span.
        # DIGen: CMScaling = 5 => 5 * internal_sf actions (NEW, UPD, ADDACCT, etc.).
        self.cm_final_row_count = 5 * self.internal_sf

        # Prospect.csv: Total prospect records mailed to potential customers.
        # DIGen: ProspectScaling = 5 => 5 * internal_sf rows.
        self.prospect_total = 5 * self.internal_sf

        # Trade: Total historical trades across the 5-year trade window.
        # DIGen: THistScaling = 130 => 130 * internal_sf trades.
        # These are distributed across trade_days (~1826 days).
        self.trade_total = 130 * self.internal_sf

        # Incremental trades per batch = trades_per_day (one day per batch)
        # DIGen: TSTradesPerDay = ceil(THistScaling * SF / TSUpdateCount)

        # WatchHistory: Total watch list entries over the 5-year watch window.
        # DIGen: WHScaling = 300 => 300 * internal_sf entries.
        # These are paired ACTV/CNCL events generated via a GrowingCrossProduct
        # of customers x securities over the watch date range.
        self.wh_total = 300 * self.internal_sf

        # ----- FINWIRE parameters -----
        # FINWIRE files contain quarterly batches of company (CMP), security (SEC),
        # and financial (FIN) records spanning FW_BEGIN_DATE to FW_END_DATE.
        #
        # fw_quarters = 202: The number of complete quarters from 1967-07-07 to
        #   2017-07-07. Verified empirically; DIGen iterates quarter-by-quarter.
        #
        # cmp_per_quarter: Companies introduced per quarter. DIGen distributes
        #   cmp_total across (fw_quarters + 1) = 203 slots (the +1 accounts for
        #   the initial quarter). Integer division; max(1,...) prevents zero.
        #
        # sec_per_quarter: Securities introduced per quarter. DIGen distributes
        #   sec_total across fw_quarters = 202 slots.
        #
        # These formulas were verified to produce exact CMP/SEC/FIN counts
        # matching DIGen audit data at SF=10, SF=100, and SF=1000.
        self.fw_quarters = 202
        self.cmp_per_quarter = max(1, self.cmp_total // (self.fw_quarters + 1))  # divide by 203
        self.sec_per_quarter = max(1, self.sec_total // self.fw_quarters)  # divide by 202

        # ----- Trade timing -----
        # trade_days: Number of calendar days in the trade window (~1826).
        # trades_per_day: Ceiling division of total trades by trade days.
        #   The -(-a // b) idiom computes ceil(a / b) using integer arithmetic.
        #   Each incremental batch adds one day's worth of new trades.
        self.trade_days = (TRADE_END_DATE - TRADE_BEGIN_DATE).days
        self.trades_per_day = -(-self.trade_total // max(1, self.trade_days))  # ceil division

        # ----- Incremental trade rows -----
        # Each incremental batch (Batch 2, 3) generates one day of trade activity.
        # DIGen produces both new trade inserts and status-update rows:
        #   - ~39.4% of rows are new trade inserts (PNDG status)
        #   - ~60.6% are status transitions for existing trades (PNDG->SBMT->CMPT/CNCL)
        # trade_inc_new: New trades per batch (= trades_per_day).
        # trade_inc: Total rows per batch (new + updates), matching DIGen's ratio.
        #   Dividing by 0.394 inverts the 39.4% ratio to get the full row count.
        self.trade_inc_new = self.trades_per_day
        self.trade_inc = int(self.trades_per_day / 0.394)  # total rows matching DIGen ratio

        # ----- WatchHistory ACTV count estimate (post-dedup) -----
        # The integer dedup on (w_c_id, _sym_join_idx) removes <0.1% of rows —
        # the cross-product space is vastly larger than num_sec, so collisions
        # are rare. At SF=5000: 1,199,999,700 pre-dedup → 1,199,613,974 post = 99.97%.
        # Use 0.9997 as the survival rate. The .limit() on CNCL prevents overshoot.
        self.wh_actv_count = int(self.wh_total * 0.8 * 0.9997)

        # ----- Trade account pool -----
        # Count of brokerage accounts that existed by TRADE_BEGIN_DATE. Since
        # CA_IDs are assigned sequentially as CustomerMgmt actions execute (in
        # time order), a simple `ca_id < n_available_accounts` yields exactly
        # the set of accounts created before trades began. Trade picks via
        # hash % n_available_accounts and uses the hash directly as CA_ID —
        # no enumeration, no join to a pool DataFrame, no dependency on
        # CustomerMgmt completing. Closures are ignored (matches DIGen's
        # relativeTimeFrame reference: only creation time matters).
        cust_per_update = int(0.005 * self.internal_sf)
        acct_per_update = int(0.01 * self.internal_sf)
        _new_custs = int(cust_per_update * 0.7)
        _new_accts = int(acct_per_update * 0.7)
        _change_custs = int(cust_per_update * 0.2)
        _change_accts = int(acct_per_update * 0.2)
        _del_custs = int(cust_per_update * 0.1)
        _del_accts = int(acct_per_update * 0.1)
        _addaccts = _new_accts - _new_custs
        _rows_per_update = (_new_custs + _addaccts + _change_accts + _del_accts
                            + _change_custs + _del_custs)
        _cm_final = self.cm_final_row_count
        _update_last_id = (_cm_final - _new_accts) // _rows_per_update
        _hist_size = _cm_final - _update_last_id * _rows_per_update
        _n_created = _hist_size + _update_last_id * _new_accts
        _trade_fraction = ((TRADE_BEGIN_DATE - CM_BEGIN_DATE).total_seconds()
                           / (CM_END_DATE - CM_BEGIN_DATE).total_seconds())
        self.n_available_accounts = max(_hist_size, int(_n_created * _trade_fraction))

        # Analytical broker count estimate: 30% of HR employees become brokers
        # via hash-based sampling with ~0.01% variance. Trade uses this as a
        # modulus to compute _broker_idx; it does NOT join to _brokers, so the
        # tiny variance between estimate and exact count is harmless. Lets
        # Trade start without waiting on HR when static audits are available.
        self.n_brokers_estimate = int(self.hr_rows * BROKER_PCT)

        # ----- DailyMarket parameters -----
        # dm_days: Number of days in the DailyMarket date range (732 days for
        #   the period 2015-07-06 to 2017-07-07).
        self.dm_days = (DM_END_DATE - DM_BEGIN_DATE).days
