# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "psycopg2-binary",
# ]
# ///
# One-time-per-benchmark setup: expose a producer workgroup's already-built
# `tpcdi_staging_sf{sf}` schema to a (smaller/cheaper) consumer workgroup via a
# Redshift datashare, then surface it on the consumer as local late-binding
# VIEWS in `dev.tpcdi_staging_sf{sf}`.
#
# WHY THIS EXISTS
# ---------------
# Redshift Serverless has no zero-copy clone and each workgroup is its own
# namespace with isolated managed storage. The 150 GB SF=20k staging load is
# expensive, so we bootstrap it ONCE on a producer workgroup (e.g. the 8-RPU
# xsmall) and share it read-only to whatever workgroup actually runs the
# benchmark (e.g. medium / large). Staging is read-only after bootstrap — it
# only ever serves as the CTAS source that resets the benchmark tables — so a
# read-only datashare is a perfect fit.
#
# The consumer sees the staging at the IDENTICAL FQN as the producer
# (`dev.tpcdi_staging_sf{sf}.<table>`), because the views live in the
# consumer's local `dev` under the same schema name. setup_rs's bootstrap
# detection (`information_schema.tables` + COUNT(*)>0) therefore finds all 22
# tables present and SKIPS the re-load, going straight to the per-run CTAS.
#
# This is one-time per benchmark run and is NOT part of the measured
# perf/TCO — it's pre-flight plumbing, analogous to the one-time staging
# bootstrap on the other engines.
#
# GOTCHAS baked in below (learned the hard way):
#   - Redshift `DROP DATABASE` / `DROP DATASHARE` have NO `IF EXISTS` — guard
#     with a catalog lookup first.
#   - A datashare must be `PUBLICACCESSIBLE TRUE` for a publicly-accessible
#     consumer workgroup to read it (all these workgroups have public
#     endpoints). The GRANT ... TO NAMESPACE still scopes it to one consumer.
#   - Cross-database / datashare views must be `WITH NO SCHEMA BINDING`
#     (late-binding). Late-binding views DO appear in `information_schema.tables`,
#     so setup_rs's detection still sees them.
#
# Auth: user/database are plain params; only the password is a UC secret,
# referenced by full path in rs_password_secret. The producer/consumer are
# addressed by explicit host so this can wire any pair of workgroups.

import psycopg2

dbutils.widgets.text("scale_factor",  "20000", "Scale factor")
# Workgroup endpoints are supplied at run time (no default — they embed your
# AWS account id + region, e.g.
#   <workgroup>.<account-id>.<region>.redshift-serverless.amazonaws.com).
dbutils.widgets.text("producer_host", "",
    "Producer workgroup endpoint (owns the built staging)")
dbutils.widgets.text("consumer_host", "",
    "Consumer workgroup endpoint (runs the benchmark)")
dbutils.widgets.text("database", "dev", "Redshift database (plain value)")
dbutils.widgets.text("rs_user",  "", "Redshift user (plain value)")
dbutils.widgets.text("rs_password_secret", "main.tpcdi_redshift.password",
    "Full UC secret path for the Redshift password (catalog.schema.key)")

sf            = int(dbutils.widgets.get("scale_factor"))
PROD_HOST     = dbutils.widgets.get("producer_host").strip()
CONS_HOST     = dbutils.widgets.get("consumer_host").strip()
RS_DATABASE   = dbutils.widgets.get("database").strip() or "dev"
RS_USER       = dbutils.widgets.get("rs_user").strip()
RS_PASSWORD_SECRET = dbutils.widgets.get("rs_password_secret").strip()

if not PROD_HOST or not CONS_HOST:
    raise ValueError(
        "producer_host and consumer_host are required (Redshift Serverless "
        "workgroup endpoints). Pass them as job/notebook parameters.")

def _secret_from_path(path):
    catalog, schema, key = path.split(".", 2)
    return dbutils.secrets.get(catalog=catalog, schema=schema, key=key)

SCHEMA   = f"tpcdi_staging_sf{sf}".lower()      # local schema on consumer (matches producer FQN)
SHARE    = f"{SCHEMA}_share"                    # datashare name on producer
SHARE_DB = "tpcdi_staging_share"               # consumer-side db created from the share (plumbing)

# The canonical 22 staging tables (mirrors setup_rs.STAGING_TABLES_EXPECTED).
TABLES = [
    "batchdate", "bronzedailymarket", "cashtransactionhistorical", "companyyeareps",
    "currentaccountbalances", "dimaccount", "dimbroker", "dimcompany", "dimcustomer",
    "dimdate", "dimsecurity", "dimtime", "dimtrade", "factcashbalances", "factholdings",
    "factmarkethistory", "factwatches", "financial", "industry", "statustype",
    "taxrate", "tradetype",
]

def _conn(host):
    c = psycopg2.connect(
        host=host, port=5439,
        user=RS_USER, password=_secret_from_path(RS_PASSWORD_SECRET),
        dbname=RS_DATABASE,
        sslmode="require", connect_timeout=60,
        keepalives=1, keepalives_idle=30, keepalives_interval=10, keepalives_count=3,
    )
    c.autocommit = True
    return c

log = []
def run(cur, sql, label=None):
    try:
        cur.execute(sql)
        log.append(f"OK   {label or sql[:70]}")
    except Exception as e:
        log.append(f"FAIL {label or sql[:70]} :: {type(e).__name__}: {str(e)[:160]}")
        raise

# COMMAND ----------

# 0) Discover the two namespace GUIDs (needed for GRANT + CREATE DATABASE).
pc = _conn(PROD_HOST); pcur = pc.cursor()
pcur.execute("SELECT current_namespace"); PROD_NS = pcur.fetchone()[0]
cc = _conn(CONS_HOST); ccur = cc.cursor()
ccur.execute("SELECT current_namespace"); CONS_NS = ccur.fetchone()[0]
log.append(f"producer ns = {PROD_NS}")
log.append(f"consumer ns = {CONS_NS}")

# 1) Consumer: drop any prior share-db first (removes the dependency on the
#    share). Redshift DROP DATABASE has no IF EXISTS — check pg_database.
ccur.execute("SELECT 1 FROM pg_database WHERE datname=%s", (SHARE_DB,))
if ccur.fetchone():
    run(ccur, f"DROP DATABASE {SHARE_DB}", "consumer: drop old share-db")
else:
    log.append(f"OK   consumer: no old share-db {SHARE_DB} to drop")

# 2) Producer: (re)create the publicly-accessible datashare, add the staging
#    schema + all its tables, and grant usage to the consumer namespace.
pcur.execute("SELECT 1 FROM svv_datashares WHERE share_name=%s AND share_type='OUTBOUND'", (SHARE,))
if pcur.fetchone():
    run(pcur, f"DROP DATASHARE {SHARE}", "producer: drop old share")
else:
    log.append(f"OK   producer: no old share {SHARE} to drop")
run(pcur, f"CREATE DATASHARE {SHARE} PUBLICACCESSIBLE TRUE", "producer: create share (publicaccessible)")
run(pcur, f"ALTER DATASHARE {SHARE} ADD SCHEMA {SCHEMA}", "producer: add schema")
run(pcur, f"ALTER DATASHARE {SHARE} ADD ALL TABLES IN SCHEMA {SCHEMA}", "producer: add all tables")
run(pcur, f"GRANT USAGE ON DATASHARE {SHARE} TO NAMESPACE '{CONS_NS}'", "producer: grant to consumer ns")

# 3) Consumer: mount the share as a db, create the matching local schema, and
#    create one late-binding view per staging table.
run(ccur, f"CREATE DATABASE {SHARE_DB} FROM DATASHARE {SHARE} OF NAMESPACE '{PROD_NS}'", "consumer: create db from share")
run(ccur, f'CREATE SCHEMA IF NOT EXISTS "{SCHEMA}"', "consumer: create local schema")
for t in TABLES:
    run(ccur, f'DROP VIEW IF EXISTS "{SCHEMA}"."{t}"', f"drop view {t}")
    run(ccur, f'CREATE VIEW "{SCHEMA}"."{t}" AS SELECT * FROM {SHARE_DB}.{SCHEMA}.{t} WITH NO SCHEMA BINDING', f"view {t}")

# COMMAND ----------

# 4) SELF-VERIFY: run setup_rs's exact detection query + per-view counts so we
#    KNOW the bootstrap will skip before we trigger a multi-hour benchmark.
ccur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema=%s ORDER BY 1", (SCHEMA,))
seen = [r[0] for r in ccur.fetchall()]
missing = sorted(set(TABLES) - set(seen))
counts = {}
for t in TABLES:
    try:
        ccur.execute(f'SELECT COUNT(*) FROM "{SCHEMA}"."{t}"'); counts[t] = ccur.fetchone()[0]
    except Exception as e:
        counts[t] = f"ERR {str(e)[:80]}"
bad = [t for t, n in counts.items() if not isinstance(n, int) or n == 0]
skip_will_fire = (not missing) and (not bad)

log.append(f"\n[VERIFY] information_schema.tables sees {len(seen)}/22 in {SCHEMA}")
log.append(f"[VERIFY] missing: {missing or 'NONE'}")
log.append(f"[VERIFY] zero/error views: {bad or 'NONE'}")
log.append(f"[VERIFY] counts: {counts}")
log.append(f"\n[RESULT] bootstrap-skip will fire on consumer: {skip_will_fire}")

pc.close(); cc.close()
if not skip_will_fire:
    raise RuntimeError("share/views not fully readable on consumer — see VERIFY above")
dbutils.notebook.exit("\n".join(log))
