# Databricks notebook source
# ONE-TIME per scale factor — bronze-only counterpart to seed_staging_py.py.
#
# Reads the 7 bronze tables produced by augmented_staging with
# `generate_bronze_staging=YES`:
#   main.tpcdi_incremental_staging_{sf}.bronze<dataset>
# and writes them to native Snowflake:
#   TPCDI_TEST.STAGING_SF{sf}.bronze<dataset>
#
# Used as the federation-free path for SF=10 dev. For SF=20k production
# use onetime_stg_snowflake_dt_bronze_tables.py instead (faster — uses
# the federated Iceberg catalog).
#
# Auth: same Databricks secret scope as seed_staging_py / setup_sf.
#
# Why bronze tables ONLY (not silver/gold)? The Snowflake DT variant
# builds silver/gold via the DT DAG from cumulative bronze; the existing
# silver/gold staging tables aren't needed by the DT pipeline. The
# existing seed_staging_py loads silver/gold for the dbt-Snowflake variant.

# COMMAND ----------

# Pin typing-extensions BEFORE the connector import path — older runtime
# bundles a version that doesn't expose `deprecated`, which pyopenssl 25+
# requires. Without this, OpenSSL.crypto fails to import.
%pip install --quiet "typing-extensions>=4.13" snowflake-connector-python pandas pyarrow

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.dropdown("scale_factor","10", ["10","100","1000","5000","10000","20000"])
dbutils.widgets.text("snowflake_database", "TPCDI_TEST")
dbutils.widgets.text("secret_scope",       "tpcdi_snowflake")
dbutils.widgets.text("snowflake_warehouse", "", "Override the Snowflake warehouse (empty = use secret_scope.warehouse)")

src_catalog        = dbutils.widgets.get("catalog")
scale_factor       = dbutils.widgets.get("scale_factor")
sf_db              = dbutils.widgets.get("snowflake_database")
secret_scope       = dbutils.widgets.get("secret_scope")
warehouse_override = dbutils.widgets.get("snowflake_warehouse")

src_schema = f"tpcdi_incremental_staging_{scale_factor}"
sf_schema  = f"STAGING_SF{scale_factor}"
print(f"src = {src_catalog}.{src_schema}  →  sink = {sf_db}.{sf_schema}")

# COMMAND ----------

def _secret(name, default=None):
    try: return dbutils.secrets.get(scope=secret_scope, key=name)
    except Exception: return default

account   = _secret("account")
user      = _secret("user")
role      = _secret("role") or "ACCOUNTADMIN"
warehouse = warehouse_override or _secret("warehouse") or "BARROW_XS_GEN2"
pk_pem    = _secret("private_key")

if not (account and user and pk_pem):
    raise RuntimeError(f"Secret scope '{secret_scope}' missing account/user/private_key")

from cryptography.hazmat.primitives import serialization
pk = serialization.load_pem_private_key(pk_pem.encode(), password=None)
pk_der = pk.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption(),
)

# COMMAND ----------

import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

conn = snowflake.connector.connect(
    account=account, user=user, role=role,
    warehouse=warehouse, database=sf_db, private_key=pk_der,
)
cur = conn.cursor()
cur.execute(f"CREATE SCHEMA IF NOT EXISTS {sf_db}.{sf_schema}")
cur.execute(f"USE SCHEMA {sf_db}.{sf_schema}")
print(f"[ok] {sf_db}.{sf_schema} ready")

# COMMAND ----------

BRONZE_TABLES = [
    "bronzeaccount",
    "bronzecashtransaction",
    "bronzecustomer",
    "bronzedailymarket",
    "bronzeholdings",
    "bronzetrade",
    "bronzewatches",
]

# COMMAND ----------

import time, pandas as pd
for t in BRONZE_TABLES:
    src_fq = f"{src_catalog}.{src_schema}.{t}"
    t0 = time.time()
    try:
        spdf = spark.read.table(src_fq).toPandas()
    except Exception as e:
        print(f"[skip] {src_fq} not found: {e}")
        continue
    # Drop Spark PlanMetrics in df.attrs (chokes write_pandas auto_create_table).
    pdf = pd.DataFrame(spdf.values, columns=[c.upper() for c in spdf.columns])
    pdf = pdf.astype({c.upper(): spdf.dtypes[c_orig] for c, c_orig in
                       zip(pdf.columns, spdf.columns)})
    target = t.upper()
    cur.execute(f"DROP TABLE IF EXISTS {sf_db}.{sf_schema}.{target}")
    success, nchunks, nrows, _ = write_pandas(
        conn, pdf, target,
        database=sf_db, schema=sf_schema,
        auto_create_table=True, overwrite=False, quote_identifiers=False,
    )
    if not success:
        raise RuntimeError(f"write_pandas failed for {t}")
    print(f"[seed] {t}: {nrows:,} rows  ({time.time()-t0:.1f}s)")

# COMMAND ----------

cur.close()
conn.close()
print(f"[done] {sf_db}.{sf_schema}.bronze* seeded — setup_sf_dt can now SELECT * FROM these into bronze*_raw.")
