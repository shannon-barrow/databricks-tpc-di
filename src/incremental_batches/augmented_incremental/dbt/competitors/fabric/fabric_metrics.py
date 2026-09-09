#!/usr/bin/env python3
"""Fabric DW query-history metrics for the augmented-incremental Fabric-DW
benchmark. Laptop script (pyodbc + Entra SP), analogue of rs_metrics.py.

Pulls per-batch / per-statement / live rollups from the warehouse's
`queryinsights.exec_requests_history` view (Fabric DW's SQL query history).

Scope: unlike Redshift's session `query_group`, Fabric DW has no per-session
tag. Two attribution paths:
  1. dbt-emitted statements carry dbt's query_comment (a JSON-ish comment with
     the node's unique_id) in the query text → regex the model name / batch out.
  2. Our own _fab_conn statements can carry OPTION(LABEL='{...}') → surfaces in
     exec_requests_history.label. dbt statements won't have our LABEL, so for
     per-batch dbt wall we group by the time windows of the dbt run instead.

⚠ COST: exec_requests_history gives duration, not CU-seconds. Fabric bills
Spark/SQL by capacity CU-seconds; the authoritative per-operation CU number is
in the **Fabric Capacity Metrics app** (a Power BI semantic model), not a SQL
view. For a hand TCO, use total duration × F64 hourly (as we did for fabric_ss/
nee), or pull CU-seconds from Capacity Metrics. This script reports duration.

Env:
  FABRIC_WH_HOST, FABRIC_WH_NAME, FABRIC_TENANT_ID,
  FABRIC_CLIENT_ID, FABRIC_CLIENT_SECRET

Usage:
  python3 fabric_metrics.py per-batch [wh_db] [sf]
  python3 fabric_metrics.py raw   [wh_db] [sf]
  python3 fabric_metrics.py live  [wh_db]
"""
from __future__ import annotations

import argparse, os, struct, sys

DEFAULT_WH_DB = "shannon_aug_fabric_dbt"
DEFAULT_SF = "10"


def _connect():
    try:
        import pyodbc
    except ImportError:
        sys.exit("pip install pyodbc msal first (and install msodbcsql18)")
    try:
        import msal
    except ImportError:
        sys.exit("pip install msal first")
    host = os.environ.get("FABRIC_WH_HOST")
    db   = os.environ.get("FABRIC_WH_NAME", "tpcdi_fabric_dw")
    tid  = os.environ.get("FABRIC_TENANT_ID")
    cid  = os.environ.get("FABRIC_CLIENT_ID")
    csec = os.environ.get("FABRIC_CLIENT_SECRET")
    if not all([host, tid, cid, csec]):
        sys.exit("set FABRIC_WH_HOST, FABRIC_TENANT_ID, FABRIC_CLIENT_ID, FABRIC_CLIENT_SECRET")
    app = msal.ConfidentialClientApplication(
        cid, authority=f"https://login.microsoftonline.com/{tid}", client_credential=csec)
    res = app.acquire_token_for_client(scopes=["https://database.windows.net/.default"])
    if "access_token" not in res:
        sys.exit(f"token error: {res.get('error_description')}")
    tok = res["access_token"].encode("utf-16-le")
    token_struct = struct.pack("<i", len(tok)) + tok
    conn_str = (f"Driver={{ODBC Driver 18 for SQL Server}};Server={host},1433;"
                f"Database={db};Encrypt=yes;TrustServerCertificate=no;")
    return pyodbc.connect(conn_str, attrs_before={1256: token_struct}, autocommit=True)


def q_per_batch(*, wh_db: str, sf: str) -> str:
    """Per-batch wall from exec_requests_history: group the dbt statements that
    wrote into the run schema by day. Wall = MIN(start)..MAX(end) of each day's
    statements. dbt's query_comment embeds the target relation, so we scope by
    the run schema name appearing in the command text."""
    run_schema = f"{wh_db}_{sf}".lower()
    return f"""
    SELECT
      CONVERT(date, start_time)                              AS run_day,
      COUNT(*)                                               AS stmt_count,
      MIN(start_time)                                        AS first_stmt,
      MAX(end_time)                                          AS last_stmt,
      DATEDIFF(SECOND, MIN(start_time), MAX(end_time))       AS wall_sec,
      SUM(total_elapsed_time_ms) / 1000.0                    AS sum_elapsed_sec
    FROM queryinsights.exec_requests_history
    WHERE start_time >= DATEADD(HOUR, -48, SYSUTCDATETIME())
      AND (command LIKE '%[{run_schema}].%' OR command LIKE '%{run_schema}.%')
    GROUP BY CONVERT(date, start_time)
    ORDER BY run_day
    """


def q_raw(*, wh_db: str, sf: str) -> str:
    run_schema = f"{wh_db}_{sf}".lower()
    return f"""
    SELECT TOP 50 start_time, end_time, status, total_elapsed_time_ms,
      LEFT(command, 200) AS cmd
    FROM queryinsights.exec_requests_history
    WHERE start_time >= DATEADD(HOUR, -48, SYSUTCDATETIME())
      AND (command LIKE '%{run_schema}%')
    ORDER BY start_time DESC
    """


def q_live(*, wh_db: str) -> str:
    run_schema = f"{wh_db}".lower()
    return f"""
    SELECT session_id, status,
      DATEDIFF(SECOND, start_time, SYSUTCDATETIME()) AS running_sec,
      LEFT(command, 200) AS cmd
    FROM queryinsights.exec_requests_history
    WHERE status IN ('Running','Suspended')
      AND command LIKE '%{run_schema}%'
    ORDER BY start_time
    """


def _print_rows(cur):
    cols = [d[0] for d in cur.description] if cur.description else []
    print("\t".join(cols))
    for row in cur.fetchall():
        print("\t".join("" if v is None else str(v) for v in row))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["per-batch", "raw", "live"])
    ap.add_argument("wh_db", nargs="?", default=DEFAULT_WH_DB)
    ap.add_argument("sf", nargs="?", default=DEFAULT_SF)
    a = ap.parse_args()
    sql = {"per-batch": q_per_batch, "raw": q_raw}.get(a.cmd)
    sql = sql(wh_db=a.wh_db, sf=a.sf) if a.cmd != "live" else q_live(wh_db=a.wh_db)
    conn = _connect()
    with conn.cursor() as cur:
        cur.execute(sql)
        _print_rows(cur)
    conn.close()


if __name__ == "__main__":
    main()
