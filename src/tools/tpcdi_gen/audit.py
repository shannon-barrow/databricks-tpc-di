"""Generate audit files for the TPC-DI benchmark pipeline.

The TPC-DI spec requires audit files that record metadata about the generated
data so the benchmark pipeline can validate its output. This module produces:

1. Generator_audit.csv (one file, top-level):
   Records the data generation parameters: scale factor, date ranges, scaling
   constants, and configuration values. The TPC-DI pipeline reads these to
   verify it processed the correct volume of data. Written to the volume root
   (not inside a batch directory).

2. Batch{N}_audit.csv (one per batch, top-level):
   Records the FirstDay and LastDay dates for each batch. The pipeline uses
   these to validate temporal boundaries of incremental loads.

3. Per-table audit files (inside Batch1 directory):
   HR_audit.csv, Trade_audit.csv, DailyMarket_audit.csv, WatchHistory_audit.csv,
   Prospect_audit.csv, CustomerMgmt_audit.csv, TradeHistory_audit.csv,
   CashTransaction_audit.csv, HoldingHistory_audit.csv, and one per static
   reference table. Each records expected row counts and derived metrics
   (e.g. P_C_MATCHING for prospect-customer match count, WH_ACTIVE for
   active watch items).

All audit files use CSV format with header:
   DataSet, BatchID ,Date , Attribute , Value, DValue
(Note: the spacing matches DIGen's exact format for compatibility.)
"""

import os
from datetime import datetime, timedelta, timezone
from .config import *
from .utils import write_text, log


def static_audits_available(cfg) -> bool:
    """Return True if a pre-computed audit snapshot exists for cfg.sf.

    Generators call this to decide whether to skip log-only / audit-only
    count() queries — when a static snapshot will be copied at the end of
    the run, the audit CSVs get exact pre-computed values regardless of
    what the generators return.

    In augmented_incremental mode the benchmark doesn't read audits at
    all (orchestrator skips audit emit), AND several dynamic-regen blocks
    read from staging paths or B2/B3 temp views that don't exist when
    augmented mode writes to Delta and skips B2/B3. Treat as "available"
    so every generator takes the analytical-estimate path and never
    hits those broken read sites.
    """
    if getattr(cfg, "augmented_incremental", False):
        return True
    user_sf = int(cfg.internal_sf // 1000)
    return os.path.isdir(os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "static_audits", f"sf={user_sf}"))


def generate(cfg, record_counts: dict, gen_start_time: datetime, dbutils):
    """Write audit files.

    If a static audit snapshot exists for this scale factor at
    ``tpcdi_gen/static_audits/sf={sf}/``, those pre-computed files are copied
    to the output volume instead of recomputing. Otherwise, audit files are
    regenerated dynamically using ``record_counts`` populated by each
    generator during waves 1-3.

    Args:
        cfg: ScaleConfig with volume_path, internal_sf, and other parameters.
        record_counts: dict mapping (table_name, batch_id) to integer row counts
            (only used for the dynamic-regeneration path).
        gen_start_time: retained for signature compatibility; unused.
        dbutils: Databricks dbutils for file I/O.
    """
    if not _try_copy_static_audits(cfg, dbutils):
        log("[Audit] no static snapshot for this SF; regenerating dynamically")
        _gen_generator_audit(cfg, dbutils)
        _gen_batch_audits(cfg, dbutils)
        _gen_table_audits(cfg, record_counts, dbutils)
        _gen_incremental_table_audits(cfg, record_counts, dbutils)

    print(f"  Audit files generated.")


def _try_copy_static_audits(cfg, dbutils) -> bool:
    """Copy pre-computed audit files from the repo into the output volume.

    Looks for ``tpcdi_gen/static_audits/sf={user_sf}/`` next to this module.
    Returns True if all expected files were copied, False otherwise (in
    which case the caller falls back to dynamic generation).
    """
    user_sf = int(cfg.internal_sf // 1000)
    src_root = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            "static_audits", f"sf={user_sf}")
    if not os.path.isdir(src_root):
        return False

    import io, sys
    copied = 0
    for root, _dirs, files in os.walk(src_root):
        rel = os.path.relpath(root, src_root)
        for fn in files:
            if not fn.endswith("_audit.csv"):
                continue
            src = os.path.join(root, fn)
            with open(src) as f:
                content = f.read()
            dst = f"{cfg.volume_path}/{fn}" if rel in (".", "") \
                else f"{cfg.volume_path}/{rel}/{fn}"
            # Silence dbutils.fs.put output noise.
            _old = sys.stdout
            sys.stdout = io.StringIO()
            try:
                dbutils.fs.put(dst, content, overwrite=True)
            finally:
                sys.stdout = _old
            copied += 1

    log(f"[Audit] static snapshot sf={user_sf}: {copied} audit files copied")
    return copied > 0


def _gen_generator_audit(cfg, dbutils):
    """Write Generator_audit.csv with data generation parameters.

    Records all scaling constants, date ranges, and configuration values
    that the TPC-DI pipeline needs to validate its output. These values
    match exactly what DIGen.jar would produce for the same scale factor.
    """
    lines = ["DataSet, BatchID ,Date , Attribute , Value, DValue"]
    lines.append(f"Generator,1,,SF,,{float(cfg.internal_sf)}")
    lines.append(f"Generator,1,,AuditingAndReportingSystemActive,,1.0")
    lines.append(f"Generator,1,,ONE_DAY_IN_MILLISECONDS,,86400000")
    lines.append(f"Generator,1,,ONE_QUARTER_IN_MILLISECONDS,,{ONE_QUARTER_MS}")
    lines.append(f"Generator,1,,ONE_YEAR_IN_MILLISECONDS,,31557600000")
    lines.append(f"Generator,1,,PERCENT_NULL,,0.05")
    lines.append(f"Generator,1,{FIRST_BATCH_DATE.strftime('%Y-%m-%d')},FIRST_BATCH_DATE_START,,")
    lines.append(f"Generator,1,,NUMBER_OF_INCREMENTAL_BATCHES,,{float(NUM_INCREMENTAL_BATCHES)}")
    lines.append(f"Generator,1,{DATE_BEGIN.strftime('%Y-%m-%d')},DateBeginDate,,")
    lines.append(f"Generator,1,{DATE_END.strftime('%Y-%m-%d')},DateEndDate,,")
    lines.append(f"Generator,1,,HRScaling,,5.0")
    lines.append(f"Generator,1,,HRBrokerScale,,{BROKER_PCT}")
    lines.append(f"Generator,1,,CScaling,,0.005")
    lines.append(f"Generator,1,,AScaling,,0.01")
    lines.append(f"Generator,1,,CMScaling,,5.0")
    lines.append(f"Generator,1,,CMFinalRowCount,,{float(cfg.cm_final_row_count)}")
    lines.append(f"Generator,1,,PHistScaling,,5.0")
    lines.append(f"Generator,1,,PMatchPct,,0.25")
    lines.append(f"Generator,1,,FWCMPScaling,,0.5")
    lines.append(f"Generator,1,,FWSECScaling,,0.8")
    lines.append(f"Generator,1,,FWQuarters,,{float(cfg.fw_quarters)}")
    lines.append(f"Generator,1,,THistScaling,,130.0")
    lines.append(f"Generator,1,,TIncScaling,,1.3")
    lines.append(f"Generator,1,,TLBPerc,,0.3")
    lines.append(f"Generator,1,,TLSPerc,,0.3")
    lines.append(f"Generator,1,,TMBPerc,,0.2")
    lines.append(f"Generator,1,,TMSPerc,,0.2")
    lines.append(f"Generator,1,,TSUpdateCount,,{float(cfg.trade_days)}")
    lines.append(f"Generator,1,,TSTradesPerDay,,{cfg.trades_per_day}")
    lines.append(f"Generator,1,,WHScalingH,,300.0")
    write_text('\n'.join(lines) + '\n', f"{cfg.volume_path}/Generator_audit.csv", dbutils)


def _gen_batch_audits(cfg, dbutils):
    """Write Batch{N}_audit.csv for each batch with FirstDay/LastDay dates.

    Each batch audit file records the temporal boundaries:
      - FirstDay: always DATE_BEGIN (the start of the Date dimension)
      - LastDay: FIRST_BATCH_DATE for batch 1, then +1 day per incremental batch
    """
    for batch_id in range(1, NUM_INCREMENTAL_BATCHES + 2):
        lines = ["DataSet, BatchID ,Date , Attribute , Value, DValue"]
        lines.append(f"Batch,{batch_id},{DATE_BEGIN.strftime('%Y-%m-%d')},FirstDay,,")
        if batch_id == 1:
            lines.append(f"Batch,{batch_id},{FIRST_BATCH_DATE.strftime('%Y-%m-%d')},LastDay,,")
        else:
            inc_date = FIRST_BATCH_DATE + timedelta(days=batch_id - 1)
            lines.append(f"Batch,{batch_id},{inc_date.strftime('%Y-%m-%d')},LastDay,,")
        write_text('\n'.join(lines) + '\n', f"{cfg.volume_path}/Batch{batch_id}_audit.csv", dbutils)


_AUDIT_HEADER = "DataSet, BatchID ,Date , Attribute , Value, DValue\n"


def _audit_row(dataset: str, batch: int, attribute: str, value) -> str:
    """Format one audit CSV row. ``value`` may be int or None (for date-only rows)."""
    v = "" if value is None else str(value)
    return f"{dataset},{batch},,{attribute},{v},\n"


def _gen_table_audits(cfg, counts, dbutils):
    """Write per-table ``*_audit.csv`` files into the Batch1 directory.

    Shape matches DIGen's Audit XML config output so the pipeline's ``Audit``
    view (a ``read_files`` union over every ``*_audit.csv``) exposes the rows
    that ``automated_audit.sql`` checks against.

    Attributes emitted per file:
      HR_audit.csv          — DimBroker.HR_BROKERS
      CustomerMgmt_audit.csv — DimAccount.{CA_ADDACCT, CA_CLOSEACCT, CA_UPDACCT,
                                           CA_ID_HIST=-1};
                               DimCustomer.{C_NEW, C_UPDCUST, C_ID_HIST=0,
                                            C_INACT, C_DOB_TO, C_DOB_TY,
                                            C_TIER_INV}
      Trade_audit.csv       — DimTrade.{T_Records, T_NEW, T_CanceledTrades,
                                        T_InvalidCommision, T_InvalidCharge}
      DailyMarket_audit.csv — FactMarketHistory.DM_RECORDS
      WatchHistory_audit.csv — FactWatches.{WH_ACTIVE, WH_RECORDS}
      HoldingHistory_audit.csv — FactHoldings.HH_RECORDS
      FINWIRE_audit.csv     — DimSecurity.{FW_SEC, FW_SEC_DUP=-1};
                               DimCompany.{FW_CMP, FW_CMP_DUP=-1, FW_FIN,
                                           FW_FIN_DUP=-1}

    ``CA_ID_HIST=-1`` and ``C_ID_HIST=0`` / ``FW_*_DUP=-1`` are literal sentinel
    values DIGen's generator never implemented — we emit them unchanged so the
    automated_audit.sql arithmetic (which sums these in) stays correct.
    """
    bp = cfg.batch_path(1)

    # --- HR / DimBroker ---
    write_text(_AUDIT_HEADER + _audit_row("DimBroker", 1, "HR_BROKERS",
                                           counts.get(("HR_BROKERS", 1), 0)),
               f"{bp}/HR_audit.csv", dbutils)

    # --- CustomerMgmt (Batch1) ---
    cm_lines = [
        _audit_row("DimAccount", 1, "CA_ADDACCT",   counts.get(("CM_ADDACCT", 1), 0)),
        _audit_row("DimAccount", 1, "CA_CLOSEACCT", counts.get(("CM_CLOSEACCT", 1), 0)),
        _audit_row("DimAccount", 1, "CA_UPDACCT",   counts.get(("CM_UPDACCT", 1), 0)),
        _audit_row("DimAccount", 1, "CA_ID_HIST",   -1),
        _audit_row("DimCustomer", 1, "C_NEW",       counts.get(("CM_NEW", 1), 0)),
        _audit_row("DimCustomer", 1, "C_UPDCUST",   counts.get(("CM_UPDCUST", 1), 0)),
        _audit_row("DimCustomer", 1, "C_ID_HIST",   0),
        _audit_row("DimCustomer", 1, "C_INACT",     counts.get(("CM_INACT", 1), 0)),
        _audit_row("DimCustomer", 1, "C_DOB_TO",    counts.get(("CM_DOB_TO", 1), 0)),
        _audit_row("DimCustomer", 1, "C_DOB_TY",    counts.get(("CM_DOB_TY", 1), 0)),
        _audit_row("DimCustomer", 1, "C_TIER_INV",  counts.get(("CM_TIER_INV", 1), 0)),
    ]
    write_text(_AUDIT_HEADER + "".join(cm_lines), f"{bp}/CustomerMgmt_audit.csv", dbutils)

    # --- Trade ---
    trade_total = counts.get(("Trade", 1), 0)
    trade_lines = [
        _audit_row("DimTrade", 1, "T_Records",          trade_total),
        _audit_row("DimTrade", 1, "T_NEW",              trade_total),
        _audit_row("DimTrade", 1, "T_CanceledTrades",   counts.get(("T_CanceledTrades", 1), 0)),
        _audit_row("DimTrade", 1, "T_InvalidCommision", counts.get(("T_InvalidCommision", 1), 0)),
        _audit_row("DimTrade", 1, "T_InvalidCharge",    counts.get(("T_InvalidCharge", 1), 0)),
    ]
    write_text(_AUDIT_HEADER + "".join(trade_lines), f"{bp}/Trade_audit.csv", dbutils)

    # --- DailyMarket / FactMarketHistory ---
    write_text(_AUDIT_HEADER + _audit_row("FactMarketHistory", 1, "DM_RECORDS",
                                           counts.get(("DailyMarket", 1), 0)),
               f"{bp}/DailyMarket_audit.csv", dbutils)

    # --- WatchHistory / FactWatches: exact counts from the generator ---
    wh_total = counts.get(("WatchHistory", 1), 0)
    wh_actv = counts.get(("WH_ACTV", 1), 0)
    wh_lines = [
        _audit_row("FactWatches", 1, "WH_ACTIVE",  wh_actv),
        _audit_row("FactWatches", 1, "WH_RECORDS", wh_total),
    ]
    write_text(_AUDIT_HEADER + "".join(wh_lines), f"{bp}/WatchHistory_audit.csv", dbutils)

    # --- HoldingHistory / FactHoldings ---
    write_text(_AUDIT_HEADER + _audit_row("FactHoldings", 1, "HH_RECORDS",
                                           counts.get(("HoldingHistory", 1), 0)),
               f"{bp}/HoldingHistory_audit.csv", dbutils)

    # --- TradeHistory / DimTradeHistory attributes ---
    # DIGen emits TH_Records, TH_TLBTrades, TH_TLSTrades, TH_TMBTrades, TH_TMSTrades, TH_CanceledLTrades plus CT_Records and CT_Trades as a single TradeHistory_audit.csv per batch.
    th_lines = [
        _audit_row("DimTradeHistory", 1, "TH_Records",         counts.get(("TH_Records", 1), 0)),
        _audit_row("DimTradeHistory", 1, "TH_TLBTrades",       counts.get(("TH_TLBTrades", 1), 0)),
        _audit_row("DimTradeHistory", 1, "TH_TLSTrades",       counts.get(("TH_TLSTrades", 1), 0)),
        _audit_row("DimTradeHistory", 1, "TH_TMBTrades",       counts.get(("TH_TMBTrades", 1), 0)),
        _audit_row("DimTradeHistory", 1, "TH_TMSTrades",       counts.get(("TH_TMSTrades", 1), 0)),
        _audit_row("DimTradeHistory", 1, "TH_CanceledLTrades", counts.get(("TH_CanceledLTrades", 1), 0)),
        _audit_row("DimTradeHistory", 1, "CT_Records",         counts.get(("CT_Records", 1), 0)),
        _audit_row("DimTradeHistory", 1, "CT_Trades",          counts.get(("CT_Trades", 1), 0)),
    ]
    write_text(_AUDIT_HEADER + "".join(th_lines), f"{bp}/TradeHistory_audit.csv", dbutils)

    # --- FINWIRE: per-record-type counts + unimplemented DUP sentinels ---
    # FW_FIN / FW_FIN_DUP go under DataSet='Financial' (not DimCompany) per DIGen's actual buf.append("Financial") — the XML comment says DimCompany but the code emits Financial, which is what automated_audit.sql expects.
    fw_lines = [
        _audit_row("DimSecurity", 1, "FW_SEC",     counts.get(("FW_SEC", 1), 0)),
        _audit_row("DimSecurity", 1, "FW_SEC_DUP", -1),
        _audit_row("DimCompany",  1, "FW_CMP",     counts.get(("FW_CMP", 1), 0)),
        _audit_row("DimCompany",  1, "FW_CMP_DUP", -1),
        _audit_row("Financial",   1, "FW_FIN",     counts.get(("FW_FIN", 1), 0)),
        _audit_row("Financial",   1, "FW_FIN_DUP", -1),
    ]
    write_text(_AUDIT_HEADER + "".join(fw_lines), f"{bp}/FINWIRE_audit.csv", dbutils)

    # --- Prospect B1: P_RECORDS = total historical rows, P_NEW = same
    # (all rows are new at B1), P_C_MATCHING = count of prospects matching existing customers. Batches 2/3 emit their own per-batch files (see _gen_incremental_table_audits below). ---
    p_total = counts.get(("Prospect", 1), 0)
    p_match = counts.get(("Prospect_Matching", 1), 0)
    p_new = counts.get(("P_NEW", 1), p_total)
    prospect_lines = [
        _audit_row("Prospect", 1, "P_RECORDS",    p_total),
        _audit_row("Prospect", 1, "P_C_MATCHING", p_match),
        _audit_row("Prospect", 1, "P_NEW",        p_new),
    ]
    write_text(_AUDIT_HEADER + "".join(prospect_lines), f"{bp}/Prospect_audit.csv", dbutils)


def _gen_incremental_table_audits(cfg, counts, dbutils):
    """Write per-dataset audit files for Batch 2+ (incremental batches).

    Mirrors the DIGen Customer-Audit / Account-Audit / Trade-Audit configs,
    which emit per-batch files under ``Batch{N}/`` directories:

      Batch{N}/Customer_audit.csv:
        DimCustomer.{C_NEW, C_UPDCUST, C_ID_HIST=0, C_INACT,
                     C_DOB_TO, C_DOB_TY, C_TIER_INV}
      Batch{N}/Account_audit.csv:
        DimAccount.{CA_ADDACCT, CA_CLOSEACCT, CA_UPDACCT, CA_ID_HIST=-1}
      Batch{N}/Trade_audit.csv:
        DimTrade.{T_NEW, T_CanceledTrades, T_InvalidCommision, T_InvalidCharge}

    Counts come from the generators' CDC-aware aggregations (CI_*, AI_*,
    TI_* keys in the counts dict).
    """
    for b in range(2, NUM_INCREMENTAL_BATCHES + 2):
        bp = cfg.batch_path(b)

        # Fact tables — per-batch incremental row counts the automated_audit cumulative-delta checks require at every BatchID in (1, 2, 3).
        wh_b = counts.get(("WatchHistory", b), 0)
        wh_actv_b = counts.get(("WH_ACTV", b), 0)
        wh_lines = [
            _audit_row("FactWatches", b, "WH_ACTIVE",  wh_actv_b),
            _audit_row("FactWatches", b, "WH_RECORDS", wh_b),
        ]
        write_text(_AUDIT_HEADER + "".join(wh_lines), f"{bp}/WatchHistory_audit.csv", dbutils)

        write_text(_AUDIT_HEADER + _audit_row("FactMarketHistory", b, "DM_RECORDS",
                                               counts.get(("DailyMarket", b), 0)),
                   f"{bp}/DailyMarket_audit.csv", dbutils)

        write_text(_AUDIT_HEADER + _audit_row("FactHoldings", b, "HH_RECORDS",
                                               counts.get(("HoldingHistory", b), 0)),
                   f"{bp}/HoldingHistory_audit.csv", dbutils)

        # DimTradeHistory CT_Records/CT_Trades per incremental batch. No TH_* attributes in incrementals since DIGen doesn't generate incremental TradeHistory.txt (only Trade.txt CDC updates).
        th_lines = [
            _audit_row("DimTradeHistory", b, "CT_Records", counts.get(("CT_Records", b), 0)),
            _audit_row("DimTradeHistory", b, "CT_Trades",  counts.get(("CT_Trades", b), 0)),
        ]
        write_text(_AUDIT_HEADER + "".join(th_lines), f"{bp}/TradeHistory_audit.csv", dbutils)

        # Per-batch Prospect audit. P_RECORDS = total rows in this batch's Prospect.csv, P_NEW = rows churned (added) in this batch, P_C_MATCHING = cumulative matching prospects through this batch.
        p_total_b = counts.get(("Prospect", b), 0)
        p_match_b = counts.get(("Prospect_Matching", b), 0)
        p_new_b = counts.get(("P_NEW", b), 0)
        prospect_lines_b = [
            _audit_row("Prospect", b, "P_RECORDS",    p_total_b),
            _audit_row("Prospect", b, "P_C_MATCHING", p_match_b),
            _audit_row("Prospect", b, "P_NEW",        p_new_b),
        ]
        write_text(_AUDIT_HEADER + "".join(prospect_lines_b), f"{bp}/Prospect_audit.csv", dbutils)

        # Customer_audit.csv (Customer.txt CDC-based)
        cust_lines = [
            _audit_row("DimCustomer", b, "C_NEW",       counts.get(("CI_NEW", b), 0)),
            _audit_row("DimCustomer", b, "C_UPDCUST",   counts.get(("CI_UPDCUST", b), 0)),
            _audit_row("DimCustomer", b, "C_ID_HIST",   0),
            _audit_row("DimCustomer", b, "C_INACT",     counts.get(("CI_INACT", b), 0)),
            _audit_row("DimCustomer", b, "C_DOB_TO",    counts.get(("CI_DOB_TO", b), 0)),
            _audit_row("DimCustomer", b, "C_DOB_TY",    counts.get(("CI_DOB_TY", b), 0)),
            _audit_row("DimCustomer", b, "C_TIER_INV",  counts.get(("CI_TIER_INV", b), 0)),
        ]
        write_text(_AUDIT_HEADER + "".join(cust_lines), f"{bp}/Customer_audit.csv", dbutils)

        # Account_audit.csv (Account.txt CDC-based)
        acct_lines = [
            _audit_row("DimAccount", b, "CA_ADDACCT",   counts.get(("AI_ADDACCT", b), 0)),
            _audit_row("DimAccount", b, "CA_CLOSEACCT", counts.get(("AI_CLOSEACCT", b), 0)),
            _audit_row("DimAccount", b, "CA_UPDACCT",   counts.get(("AI_UPDACCT", b), 0)),
            _audit_row("DimAccount", b, "CA_ID_HIST",   -1),
        ]
        write_text(_AUDIT_HEADER + "".join(acct_lines), f"{bp}/Account_audit.csv", dbutils)

        # Trade_audit.csv (Trade.txt CDC-based). DimTrade's incremental ETL MERGEs on tradeid: cdc_flag='I' INSERTs new rows, cdc_flag='U' UPDATEs existing rows in place. T_NEW counts only the inserts — that's what drives DimTrade's cumulative row-count growth, which the 'DimTrade row count' check compares against running sum(T_NEW). T_Records is the raw incremental row count (I+U).
        t_records = counts.get(("Trade", b), 0)
        trade_lines = [
            _audit_row("DimTrade", b, "T_Records",          t_records),
            _audit_row("DimTrade", b, "T_NEW",              counts.get(("TI_NEW", b), 0)),
            _audit_row("DimTrade", b, "T_CanceledTrades",   counts.get(("TI_CanceledTrades", b), 0)),
            _audit_row("DimTrade", b, "T_InvalidCommision", counts.get(("TI_InvalidCommision", b), 0)),
            _audit_row("DimTrade", b, "T_InvalidCharge",    counts.get(("TI_InvalidCharge", b), 0)),
        ]
        write_text(_AUDIT_HEADER + "".join(trade_lines), f"{bp}/Trade_audit.csv", dbutils)


