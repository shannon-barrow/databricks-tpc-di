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


def _gen_table_audits(cfg, counts, dbutils):
    """Write per-table audit files into the Batch1 directory.

    Each audit file records expected row counts and derived metrics that the
    TPC-DI pipeline uses to validate its ETL output. Key derived metrics:
      - HR_BROKERS: count of brokers (jobcode 314) for DimBroker validation
      - WH_ACTIVE: ~80% of WatchHistory total (active watch items)
      - P_C_MATCHING: ~25% of Prospect total (customer-matching prospects)
      - C_NEW: ~60% of CustomerMgmt rows (new customer actions)
    """
    bp = cfg.batch_path(1)
    hdr = "DataSet, BatchID ,Date , Attribute , Value, DValue\n"

    # Per-table audit files with expected counts from the generation run
    write_text(hdr + f"DimBroker,1,,HR_BROKERS,{counts.get(('HR_BROKERS',1),0)},\n", f"{bp}/HR_audit.csv", dbutils)
    write_text(hdr + f"DimTrade,1,,T_Records,{counts.get(('Trade',1),0)},\n", f"{bp}/Trade_audit.csv", dbutils)
    write_text(hdr + f"FactMarketHistory,1,,DM_RECORDS,{counts.get(('DailyMarket',1),0)},\n", f"{bp}/DailyMarket_audit.csv", dbutils)

    # WatchHistory: WH_ACTIVE = ~80% of total (approximate active watch ratio)
    wh_total = counts.get(("WatchHistory", 1), 0)
    write_text(hdr + f"FactWatches,1,,WH_ACTIVE,{int(wh_total * 0.8)},\nFactWatches,1,,WH_RECORDS,{wh_total},\n", f"{bp}/WatchHistory_audit.csv", dbutils)

    # Prospect: P_C_MATCHING = ~25% (PMatchPct from spec), P_NEW = total (batch 1 = all new)
    p_total = counts.get(("Prospect", 1), 0)
    write_text(hdr + f"Prospect,1,,P_C_MATCHING,{int(p_total * 0.25)},\nProspect,1,,P_RECORDS,{p_total},\nProspect,1,,P_NEW,{p_total},\n", f"{bp}/Prospect_audit.csv", dbutils)

    # CustomerMgmt: C_NEW = ~60% of total rows are new customer actions
    new_custs = int(cfg.cm_final_row_count * 0.6)
    write_text(hdr + f"DimCustomer,1,,C_NEW,{new_custs},\n", f"{bp}/CustomerMgmt_audit.csv", dbutils)
    write_text(hdr + f"DimTradeHistory,1,,TH_Records,{counts.get(('TradeHistory',1),0)},\n", f"{bp}/TradeHistory_audit.csv", dbutils)
    write_text(hdr + f"DimTradeHistory,1,,CT_Records,{counts.get(('CashTransaction',1),0)},\n", f"{bp}/CashTransaction_audit.csv", dbutils)
    write_text(hdr + f"FactHoldings,1,,HH_RECORDS,{counts.get(('HoldingHistory',1),0)},\n", f"{bp}/HoldingHistory_audit.csv", dbutils)

    # Static reference table audits
    for tbl in ["StatusType", "TaxRate", "Date", "Time", "Industry", "TradeType"]:
        write_text(hdr + f"{tbl},1,,{tbl}_Records,{counts.get((tbl,1),0)},\n", f"{bp}/{tbl}_audit.csv", dbutils)


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
