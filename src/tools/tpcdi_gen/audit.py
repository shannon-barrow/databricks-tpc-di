"""Generate audit files and generation report.

Audit File Generation
----------------------
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

4. digen_report.txt (one file, top-level):
   Human-readable generation summary with start/end times, scale factor,
   per-batch record totals, and overall throughput (records/second).

All audit files use CSV format with header:
   DataSet, BatchID ,Date , Attribute , Value, DValue
(Note: the spacing matches DIGen's exact format for compatibility.)
"""

from datetime import datetime, timedelta, timezone
from .config import *
from .utils import write_text


def generate(cfg, record_counts: dict, gen_start_time: datetime, dbutils):
    """Write all audit files and digen_report.txt.

    Called as the final step (Wave 4) after all data generation is complete,
    so record_counts contains the actual (or estimated) row counts for every
    table and batch produced during Waves 1-3.

    Args:
        cfg: ScaleConfig with volume_path, internal_sf, and other parameters.
        record_counts: dict mapping (table_name, batch_id) to integer row counts.
        gen_start_time: UTC datetime when generation started (for elapsed time).
        dbutils: Databricks dbutils for file I/O.
    """
    gen_end_time = datetime.now(tz=timezone.utc)
    # Handle both timezone-aware and naive start times (backward compatibility)
    if gen_start_time.tzinfo is None:
        gen_end_time = gen_end_time.replace(tzinfo=None)
    elapsed = (gen_end_time - gen_start_time).total_seconds()

    _gen_generator_audit(cfg, dbutils)
    _gen_batch_audits(cfg, dbutils)
    _gen_table_audits(cfg, record_counts, dbutils)
    _gen_incremental_table_audits(cfg, record_counts, dbutils)
    _gen_report(cfg, record_counts, gen_start_time, gen_end_time, elapsed, dbutils)

    print(f"  Audit files and report generated.")


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

    # --- WatchHistory / FactWatches: WH_ACTIVE ≈ 80% (ACTV vs CNCL ratio from gen) ---
    wh_total = counts.get(("WatchHistory", 1), 0)
    wh_lines = [
        _audit_row("FactWatches", 1, "WH_ACTIVE",  int(wh_total * 0.8)),
        _audit_row("FactWatches", 1, "WH_RECORDS", wh_total),
    ]
    write_text(_AUDIT_HEADER + "".join(wh_lines), f"{bp}/WatchHistory_audit.csv", dbutils)

    # --- HoldingHistory / FactHoldings ---
    write_text(_AUDIT_HEADER + _audit_row("FactHoldings", 1, "HH_RECORDS",
                                           counts.get(("HoldingHistory", 1), 0)),
               f"{bp}/HoldingHistory_audit.csv", dbutils)

    # --- FINWIRE: per-record-type counts + unimplemented DUP sentinels ---
    # FW_FIN / FW_FIN_DUP go under DataSet='Financial' (not DimCompany) per
    # DIGen's actual buf.append("Financial") — the XML comment says DimCompany
    # but the code emits Financial, which is what automated_audit.sql expects.
    fw_lines = [
        _audit_row("DimSecurity", 1, "FW_SEC",     counts.get(("FW_SEC", 1), 0)),
        _audit_row("DimSecurity", 1, "FW_SEC_DUP", -1),
        _audit_row("DimCompany",  1, "FW_CMP",     counts.get(("FW_CMP", 1), 0)),
        _audit_row("DimCompany",  1, "FW_CMP_DUP", -1),
        _audit_row("Financial",   1, "FW_FIN",     counts.get(("FW_FIN", 1), 0)),
        _audit_row("Financial",   1, "FW_FIN_DUP", -1),
    ]
    write_text(_AUDIT_HEADER + "".join(fw_lines), f"{bp}/FINWIRE_audit.csv", dbutils)

    # --- Prospect (all 3 batches emit a Batch1 audit; the ETL's Prospect table
    # carries a BatchID column derived at load time so we just emit once per
    # batch matching DIGen's per-batch Prospect-audit output). ---
    p_total = counts.get(("Prospect", 1), 0)
    p_match = counts.get(("Prospect_Matching", 1), 0)
    prospect_lines = [
        _audit_row("Prospect", 1, "P_RECORDS",    p_total),
        _audit_row("Prospect", 1, "P_C_MATCHING", p_match),
        _audit_row("Prospect", 1, "P_NEW",        p_total),
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

        # Fact tables — per-batch incremental row counts the automated_audit
        # cumulative-delta checks require at every BatchID in (1, 2, 3).
        wh_b = counts.get(("WatchHistory", b), 0)
        wh_lines = [
            _audit_row("FactWatches", b, "WH_ACTIVE",  int(wh_b * 0.8)),
            _audit_row("FactWatches", b, "WH_RECORDS", wh_b),
        ]
        write_text(_AUDIT_HEADER + "".join(wh_lines), f"{bp}/WatchHistory_audit.csv", dbutils)

        write_text(_AUDIT_HEADER + _audit_row("FactMarketHistory", b, "DM_RECORDS",
                                               counts.get(("DailyMarket", b), 0)),
                   f"{bp}/DailyMarket_audit.csv", dbutils)

        write_text(_AUDIT_HEADER + _audit_row("FactHoldings", b, "HH_RECORDS",
                                               counts.get(("HoldingHistory", b), 0)),
                   f"{bp}/HoldingHistory_audit.csv", dbutils)

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

        # Trade_audit.csv (Trade.txt CDC-based).
        # DimTrade is SCD Type 2 — every incremental row (I and U) creates a new
        # DimTrade version. The 'DimTrade row count' check compares batch row
        # count against T_NEW, so T_NEW = total incremental trades for the batch,
        # not just the inserts.
        t_records = counts.get(("Trade", b), 0)
        trade_lines = [
            _audit_row("DimTrade", b, "T_Records",          t_records),
            _audit_row("DimTrade", b, "T_NEW",              t_records),
            _audit_row("DimTrade", b, "T_CanceledTrades",   counts.get(("TI_CanceledTrades", b), 0)),
            _audit_row("DimTrade", b, "T_InvalidCommision", counts.get(("TI_InvalidCommision", b), 0)),
            _audit_row("DimTrade", b, "T_InvalidCharge",    counts.get(("TI_InvalidCharge", b), 0)),
        ]
        write_text(_AUDIT_HEADER + "".join(trade_lines), f"{bp}/Trade_audit.csv", dbutils)


def _gen_report(cfg, counts, start, end, elapsed, dbutils):
    """Write digen_report.txt with generation summary.

    Produces a human-readable report matching DIGen's output format, including:
      - Start/end timestamps
      - Generator version identification
      - Per-batch record totals
      - Overall throughput (records/second)
      - Command options used (scale factor)
    """
    total = sum(v for k, v in counts.items() if k[0] != "HR_BROKERS")
    rps = total / elapsed if elapsed > 0 else 0
    lines = [
        "TPC-DI Data Generation Report",
        "=============================",
        "",
        f"Start Time: {start.strftime('%Y-%m-%dT%H:%M:%S+0000')}",
        f"End Time: {end.strftime('%Y-%m-%dT%H:%M:%S+0000')}",
        f"Generator: Spark Data Generator",
        f"Scale Factor: {cfg.sf}",
    ]
    # Per-batch totals (excludes HR_BROKERS which is a derived metric, not a record count)
    for batch_id in range(1, NUM_INCREMENTAL_BATCHES + 2):
        bt = sum(v for k, v in counts.items() if k[1] == batch_id and k[0] != "HR_BROKERS")
        lines.append(f"AuditTotalRecordsSummaryWriter - TotalRecords for Batch{batch_id}: {bt}")
    lines.append(f"AuditTotalRecordsSummaryWriter - TotalRecords all Batches: {total} {rps:.2f} records/second")
    lines.append(f"\nCommand options used: -sf {cfg.sf}")

    write_text('\n'.join(lines) + '\n', f"{cfg.volume_path}/digen_report.txt", dbutils)
