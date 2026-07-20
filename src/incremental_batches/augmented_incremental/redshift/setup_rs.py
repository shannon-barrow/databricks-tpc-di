# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "psycopg2-binary",
# ]
# ///
# Per-run Redshift setup.
# Dispatches DDL/CTAS to Redshift; no Spark compute beyond the JDBC client.
# Steps:
#   1. Bootstrap the staging schema tpcdi_staging_sf{sf} if missing
#      (imports rs_staging_bootstrap, seeds Delta -> parquet -> COPY). Idempotent.
#   2. CREATE the per-run schema {database}.{wh_db}_{sf}.
#   3. CTAS the 22 staging tables into it (Redshift has no zero-copy clone).
#   4. Pre-create the 6 streaming bronze tables empty
#      (dbt fills them per batch via the rs_bronze_copy_prehook macro).
#   5. Emit batch_date_ls for the parent's for_each loop.
#
# Auth: connection creds from the `tpcdi_redshift` secret scope (see _rs_conn).

# COMMAND ----------

dbutils.widgets.text("database",        "dev", "Redshift database (default 'dev')")
dbutils.widgets.text("wh_db",           "", "wh_db prefix; final schema = {wh_db}_{scale_factor}")
dbutils.widgets.dropdown("scale_factor","10", ["10","100","1000","5000","10000","20000"])
dbutils.widgets.text("secret_scope",    "tpcdi_redshift", "Databricks secret scope")
dbutils.widgets.text("incremental_batches_to_run", "365",
                     "Number of batches the for_each loop runs")
dbutils.widgets.text("databricks_catalog", "main",
                     "Databricks UC catalog where tpcdi_incremental_staging_{sf} lives")
dbutils.widgets.text("tpcdi_directory", "/Volumes/main/tpcdi_raw_data/tpcdi_volume/",
                     "UC external volume root")
dbutils.widgets.text("s3_volume_prefix", "s3://REPLACE-ME/tpcdi/",
                     "S3 prefix matching the UC volume backing")
dbutils.widgets.text("aws_region",      "us-west-2", "Region for COPY")
# IDEMPOTENT MODE (default). When YES, skip work that's already done:
#   - bootstrap skips staging tables that are present with matching counts
#   - CTAS skips per-table if run-schema table already has the staging row count
#   - bronze DDLs use CREATE TABLE IF NOT EXISTS
# Set force_reset=YES to fully recreate (drop run schema, re-CTAS everything).
dbutils.widgets.dropdown("force_reset", "NO", ["NO", "YES"],
                         "YES = drop the run schema and re-CTAS everything (slow). "
                         "NO = pick up where a prior partial run left off.")

database         = dbutils.widgets.get("database")
wh_db            = dbutils.widgets.get("wh_db")
scale_factor     = dbutils.widgets.get("scale_factor")
secret_scope     = dbutils.widgets.get("secret_scope")
incremental_n    = int(dbutils.widgets.get("incremental_batches_to_run"))
databricks_catalog = dbutils.widgets.get("databricks_catalog")
tpcdi_directory  = dbutils.widgets.get("tpcdi_directory").rstrip("/") + "/"
s3_volume_prefix = dbutils.widgets.get("s3_volume_prefix")
aws_region       = dbutils.widgets.get("aws_region")
force_reset      = dbutils.widgets.get("force_reset").upper() == "YES"

if not wh_db:
    raise ValueError("wh_db is required")

target_schema  = f"{wh_db}_{scale_factor}".lower()    # Redshift identifiers are lowercase by default
staging_schema = f"tpcdi_staging_sf{scale_factor}".lower()
print(f"target  = {database}.{target_schema}")
print(f"staging = {database}.{staging_schema} (CTAS source; must exist)")

# COMMAND ----------

# MAGIC %run ./_rs_conn

# COMMAND ----------

import sys, os
try:
    _nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None)
    if _nb_path and not _nb_path.startswith("/Workspace"):
        _nb_path = "/Workspace" + _nb_path
    _module_dir = os.path.dirname(_nb_path) if _nb_path else os.getcwd()
except Exception:
    _module_dir = os.getcwd()
if _module_dir not in sys.path:
    sys.path.insert(0, _module_dir)
import rs_staging_bootstrap as bootstrap

parquet_root = f"{tpcdi_directory}staging_parquet_rs/sf={scale_factor}"
iam_role     = rs_iam_role(secret_scope=secret_scope)

# COMMAND ----------

# Bootstrap the staging schema if missing.
# Idempotent — a no-op when all 22 tables are present with matching row counts.
#
# Each cell opens and closes its own connection.
# A phase can run over an hour at SF=20k, and idle Redshift Serverless SSL
# sockets get dropped, so a long-lived conn would go stale between cells.
_conn = rs_connect(database=database, secret_scope=secret_scope,
                   query_group={"wh_db": wh_db, "scale_factor": scale_factor,
                                "task": "setup_rs", "phase": "bootstrap"})
try:
    _boot = bootstrap.ensure_staging_environment(
        _conn,
        database=database,
        target_schema=staging_schema,
        src_catalog=databricks_catalog,
        src_schema=f"tpcdi_incremental_staging_{scale_factor}",
        parquet_root=parquet_root,
        volume_root=tpcdi_directory,
        s3_volume_prefix=s3_volume_prefix,
        iam_role=iam_role,
        aws_region=aws_region,
        spark=spark,
        dbutils=dbutils,
        secret_scope=secret_scope,
        parallel=8,
    )
    print(f"[bootstrap] {_boot}")
finally:
    _conn.close()

# COMMAND ----------

# Ensure the per-run schema exists.
# force_reset=YES drops it first (re-CTAS everything).
# Default keeps existing tables so the CTAS step below can skip any already
# present with matching row counts.
_conn = rs_connect(database=database, secret_scope=secret_scope,
                   query_group={"wh_db": wh_db, "scale_factor": scale_factor,
                                "task": "setup_rs", "phase": "schema_init"})
try:
    with _conn.cursor() as cur:
        if force_reset:
            cur.execute(f'DROP SCHEMA IF EXISTS "{target_schema}" CASCADE')
            print(f"[reset] dropped {database}.{target_schema} (force_reset=YES)")
        cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{target_schema}"')
    print(f"[ok] schema {database}.{target_schema} ready (force_reset={force_reset})")
finally:
    _conn.close()

# COMMAND ----------

# Per-table distribution + sort layout, declared at CREATE time (Redshift
# DISTKEY/SORTKEY/DISTSTYLE are immutable).
# Small reference/dim tables use DISTSTYLE ALL (replicated, local joins);
# large facts use DISTKEY on the main join column.
# See PORT_NOTES.md for the strategy rationale.
#   value = (distribution_spec, sortkey_cols)
#   distribution_spec: "ALL" | "EVEN" | "KEY(col)"
TABLE_LAYOUTS = {
    # Facts (large) — DISTKEY on join column, SORTKEY on date(s) for prune
    "factmarkethistory":          ("KEY(sk_securityid)", ("sk_dateid", "sk_securityid", "sk_companyid")),
    "factwatches":                ("KEY(sk_customerid)", ("sk_dateid_dateremoved", "sk_customerid", "sk_securityid")),
    "factholdings":               ("KEY(sk_customerid)", ("sk_dateid", "sk_customerid", "sk_securityid")),
    "factcashbalances":           ("KEY(sk_customerid)", ("sk_dateid", "sk_customerid")),

    # Dims (medium-large) — DISTKEY on natural key, SORTKEY on enddate for SCD2 prune
    "dimtrade":                   ("KEY(sk_securityid)", ("sk_closedateid", "sk_brokerid", "sk_securityid")),
    "dimcustomer":                ("KEY(customerid)",    ("enddate", "customerid")),
    "dimaccount":                 ("KEY(accountid)",     ("enddate", "accountid")),

    # Small dims / snapshot tables — DISTSTYLE ALL
    "currentaccountbalances":     ("ALL", ("accountid",)),
    "dimbroker":                  ("ALL", ("brokerid",)),
    "dimsecurity":                ("ALL", ("symbol",)),    # ~16M, big-ish but heavily joined
    "dimcompany":                 ("ALL", ("companyid",)),
    "dimdate":                    ("ALL", ("sk_dateid",)),
    "dimtime":                    ("ALL", ("sk_timeid",)),

    # Tiny reference — DISTSTYLE ALL
    "taxrate":                    ("ALL", ()),
    "industry":                   ("ALL", ()),
    "tradetype":                  ("ALL", ()),
    "statustype":                 ("ALL", ()),
    "batchdate":                  ("ALL", ()),

    # Bronze staging (event-time-clustered)
    "bronzedailymarket":          ("KEY(dm_s_symb)",     ("dm_date",)),

    # Quarterly financial data
    "financial":                  ("KEY(sk_companyid)",  ("fi_year", "fi_qtr")),
    "companyyeareps":             ("KEY(sk_companyid)",  ("qtr_start_date",)),

    # Cash transaction history — large, time-ordered
    "cashtransactionhistorical":  ("KEY(accountid)",     ("event_dt", "ct_dts")),
}

# Canonical 22 staging tables (matches the SF/BQ sides). Guards TABLE_LAYOUTS
# against drift.
STAGING_TABLES_EXPECTED = {
    "bronzedailymarket", "factmarkethistory", "factwatches", "dimtrade",
    "factholdings", "factcashbalances", "cashtransactionhistorical",
    "financial", "companyyeareps", "dimaccount", "dimcustomer",
    "currentaccountbalances", "dimbroker", "dimsecurity", "dimcompany",
    "dimtime", "dimdate", "taxrate", "industry", "tradetype",
    "statustype", "batchdate",
}
_missing_layouts = STAGING_TABLES_EXPECTED - set(TABLE_LAYOUTS)
if _missing_layouts:
    raise RuntimeError(
        f"TABLE_LAYOUTS is missing layout decisions for: {sorted(_missing_layouts)}"
    )

# COMMAND ----------

# CTAS the 22 tables in parallel (independent writes, one connection each).
import concurrent.futures as _cf
import time as _time

def _ctas_one(table_name: str) -> tuple[str, float, str]:
    """CTAS one table into the run schema. Idempotent: skips if the target
    already holds the staging row count. Returns (name, elapsed_s, status)."""
    dist_spec, sortkey_cols = TABLE_LAYOUTS[table_name]

    # Build the layout clauses. `DISTKEY(col)` alone implies `DISTSTYLE KEY`.
    if dist_spec == "ALL":
        dist_sql = "DISTSTYLE ALL"
    elif dist_spec == "EVEN":
        dist_sql = "DISTSTYLE EVEN"
    elif dist_spec.startswith("KEY("):
        col = dist_spec[len("KEY("):-1]
        dist_sql = f"DISTKEY({col})"
    else:
        raise ValueError(f"Unknown distribution_spec for {table_name}: {dist_spec}")

    sort_sql = f"SORTKEY({', '.join(sortkey_cols)})" if sortkey_cols else ""

    t0 = _time.time()
    # Own connection per thread — psycopg2 connections aren't thread-safe.
    local_conn = rs_connect(
        database=database, secret_scope=secret_scope,
        query_group={"task": "setup_rs", "phase": "ctas", "table": table_name,
                     "wh_db": wh_db, "scale_factor": scale_factor},
    )
    try:
        with local_conn.cursor() as cur:
            # Skip if the target already has the staging row count (lets a
            # partial prior run resume without re-CTAS'ing finished tables).
            cur.execute("""
                SELECT EXISTS (
                  SELECT 1 FROM information_schema.tables
                  WHERE table_schema=%s AND table_name=%s
                )
            """, (target_schema, table_name))
            target_exists = cur.fetchone()[0]
            if target_exists:
                cur.execute(f'SELECT COUNT(*) FROM "{target_schema}"."{table_name}"')
                target_rows = cur.fetchone()[0]
                cur.execute(f'SELECT COUNT(*) FROM "{staging_schema}"."{table_name}"')
                staging_rows = cur.fetchone()[0]
                if target_rows == staging_rows and target_rows > 0:
                    return (table_name, _time.time() - t0, f"skip ({target_rows:,} rows already present)")
                # Mismatch — drop and re-CTAS for clean state.
                cur.execute(f'DROP TABLE "{target_schema}"."{table_name}"')
            # Run the CTAS.
            sql = f'''
                CREATE TABLE "{target_schema}"."{table_name}"
                    {dist_sql}
                    {sort_sql}
                AS
                SELECT * FROM "{staging_schema}"."{table_name}"
            '''.strip()
            cur.execute(sql)
    finally:
        local_conn.close()
    return (table_name, _time.time() - t0, "ctas")


t_clone = _time.time()
print(f"[parallel] CTAS check/run {len(STAGING_TABLES_EXPECTED)} tables (8 concurrent)...")
with _cf.ThreadPoolExecutor(max_workers=8) as ex:
    futures = {ex.submit(_ctas_one, t): t for t in sorted(STAGING_TABLES_EXPECTED)}
    for f in _cf.as_completed(futures):
        try:
            name, wall, status = f.result()
            print(f"[ctas] {name:32s} {wall:7.2f}s  {status}")
        except Exception as e:
            name = futures[f]
            print(f"[FAIL] {name:32s}  {type(e).__name__}: {e}")
            raise
print(f"[parallel] CTAS done in {_time.time() - t_clone:.1f}s")

# COMMAND ----------

# Pre-create the 6 streaming bronze tables empty.
# They have no staging source — dbt fills them each batch via
# rs_bronze_copy_prehook (CREATE TEMP TABLE LIKE this + COPY from S3 + INSERT).
# The pre-hook's `LIKE this` requires the table to already exist, hence these DDLs.
# Types mirror the Databricks bronze layer in setup_dbt.py
# (STRING->VARCHAR, TINYINT->SMALLINT, DOUBLE->DOUBLE PRECISION);
# VARCHAR widths are upper bounds only.
#
# account_updates_from_customer is deliberately NOT pre-created.
# Its model has no pre_hook (it's a pure SELECT off bronzecustomer+dimaccount),
# so dbt CTAS's it on first run.
# Pre-creating it would make dbt rewrite+reorder the columns, breaking
# dimaccount's by-position UNION against bronzeaccount.
BRONZE_DDLS = {
    "bronzecustomer": """
        cdc_flag        VARCHAR(1),
        cdc_dsn         BIGINT,
        customerid      BIGINT,
        taxid           VARCHAR(20),
        status          VARCHAR(10),
        lastname        VARCHAR(40),
        firstname       VARCHAR(40),
        middleinitial   VARCHAR(1),
        gender          VARCHAR(1),
        tier            SMALLINT,
        dob             DATE,
        addressline1    VARCHAR(80),
        addressline2    VARCHAR(80),
        postalcode      VARCHAR(20),
        city            VARCHAR(40),
        stateprov       VARCHAR(20),
        country         VARCHAR(30),
        c_ctry_1        VARCHAR(10),
        c_area_1        VARCHAR(10),
        c_local_1       VARCHAR(15),
        c_ext_1         VARCHAR(10),
        c_ctry_2        VARCHAR(10),
        c_area_2        VARCHAR(10),
        c_local_2       VARCHAR(15),
        c_ext_2         VARCHAR(10),
        c_ctry_3        VARCHAR(10),
        c_area_3        VARCHAR(10),
        c_local_3       VARCHAR(15),
        c_ext_3         VARCHAR(10),
        email1          VARCHAR(80),
        email2          VARCHAR(80),
        lcl_tx_id       VARCHAR(20),
        nat_tx_id       VARCHAR(20),
        update_dt       DATE
    """,
    "bronzeaccount": """
        cdc_flag    VARCHAR(1),
        cdc_dsn     BIGINT,
        accountid   BIGINT,
        brokerid    BIGINT,
        customerid  BIGINT,
        accountdesc VARCHAR(80),
        taxstatus   SMALLINT,
        status      VARCHAR(10),
        update_dt   DATE
    """,
    "bronzecashtransaction": """
        cdc_flag VARCHAR(1),
        cdc_dsn  BIGINT,
        accountid BIGINT,
        ct_dts   TIMESTAMP,
        ct_amt   DOUBLE PRECISION,
        ct_name  VARCHAR(100),
        event_dt DATE
    """,
    "bronzeholdings": """
        cdc_flag       VARCHAR(1),
        cdc_dsn        BIGINT,
        hh_h_t_id      BIGINT,
        hh_t_id        BIGINT,
        hh_before_qty  INTEGER,
        hh_after_qty   INTEGER,
        event_dt       DATE
    """,
    "bronzetrade": """
        cdc_flag    VARCHAR(1),
        cdc_dsn     BIGINT,
        tradeid     BIGINT,
        t_dts       TIMESTAMP,
        status      VARCHAR(10),
        t_tt_id     VARCHAR(10),
        cashflag    SMALLINT,
        t_s_symb    VARCHAR(20),
        quantity    INTEGER,
        bidprice    DOUBLE PRECISION,
        t_ca_id     BIGINT,
        executedby  VARCHAR(80),
        tradeprice  DOUBLE PRECISION,
        fee         DOUBLE PRECISION,
        commission  DOUBLE PRECISION,
        tax         DOUBLE PRECISION,
        event_dt    DATE
    """,
    "bronzewatches": """
        cdc_flag VARCHAR(1),
        cdc_dsn  BIGINT,
        w_c_id   BIGINT,
        w_s_symb VARCHAR(20),
        w_dts    TIMESTAMP,
        w_action VARCHAR(10),
        event_dt DATE
    """,
}

# DISTKEY on natural ID, SORTKEY on batch-date (mirrors the setup_dbt.py
# CLUSTER BY choices).
BRONZE_LAYOUTS = {
    "bronzecustomer":                ("KEY(customerid)", ("update_dt",)),
    "bronzeaccount":                 ("KEY(accountid)",  ("update_dt",)),
    "bronzecashtransaction":         ("KEY(accountid)",  ("event_dt",)),
    "bronzeholdings":                ("KEY(hh_t_id)",    ("event_dt",)),
    "bronzetrade":                   ("KEY(tradeid)",    ("event_dt",)),
    "bronzewatches":                 ("KEY(w_c_id)",     ("event_dt",)),
}

def _dist_clause(spec: str) -> str:
    if spec == "ALL":  return "DISTSTYLE ALL"
    if spec == "EVEN": return "DISTSTYLE EVEN"
    if spec.startswith("KEY("):
        col = spec[len("KEY("):-1]
        return f"DISTKEY({col})"
    raise ValueError(f"unknown distribution_spec: {spec}")

# Fresh conn for bronze DDLs (CTAS above can take hours).
_conn = rs_connect(database=database, secret_scope=secret_scope,
                   query_group={"wh_db": wh_db, "scale_factor": scale_factor,
                                "task": "setup_rs", "phase": "bronze_ddls"})
try:
    with _conn.cursor() as cur:
        for tbl, cols_sql in BRONZE_DDLS.items():
            dist_spec, sortkey_cols = BRONZE_LAYOUTS[tbl]
            sort_sql = f"SORTKEY({', '.join(sortkey_cols)})" if sortkey_cols else ""
            # IF NOT EXISTS preserves any dbt-loaded rows on re-run; use
            # force_reset=YES to change the schema.
            ddl = f'''
                CREATE TABLE IF NOT EXISTS "{target_schema}"."{tbl}" (
                  {cols_sql.strip().rstrip(",")}
                )
                {_dist_clause(dist_spec)}
                {sort_sql}
            '''.strip()
            cur.execute(ddl)
            print(f"[bronze-ddl] {tbl:32s} OK")
finally:
    _conn.close()

# COMMAND ----------

# Emit the batch-date list for the parent's for_each loop. Window starts
# 2016-07-06 (AUG_FILES_DATE_START), matching the other setup notebooks.
import datetime as dt
incr_start = dt.date(2016, 7, 6)
batches = [(incr_start + dt.timedelta(days=i)).isoformat()
           for i in range(incremental_n)]
dbutils.jobs.taskValues.set("batch_date_ls", batches)
print(f"emitted batch_date_ls: {len(batches)} dates, first={batches[0]}, last={batches[-1]}")

# COMMAND ----------

print("[done] Redshift setup complete.")
