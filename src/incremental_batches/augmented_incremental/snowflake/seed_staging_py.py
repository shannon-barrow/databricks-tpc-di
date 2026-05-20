# Databricks notebook source
# Serverless-compatible variant of seed_staging.py — uses the pure-Python
# `snowflake-connector-python` library instead of the JVM-based
# snowflake-spark connector (sc._jvm isn't available on serverless).
#
# ONE-TIME per scale factor. Copies
#   {catalog}.tpcdi_incremental_staging_{sf}.{table}
# to
#   TPCDI_TEST.STAGING_SF{sf}.{table}
# on Snowflake, via `spark.read.table(...).toPandas()` -> `write_pandas`.
#
# Suitable for SF<=20000 where each individual staging table fits comfortably
# in driver memory (largest at SF=20000 is dimaccount at ~30M rows, ~3GB pandas
# — still feasible on a serverless driver, but bump driver tier if it OOMs).

# COMMAND ----------

%pip install --quiet snowflake-connector-python pandas pyarrow

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.dropdown("scale_factor","10", ["10","100","1000","5000","10000","20000"])
dbutils.widgets.text("snowflake_database", "TPCDI_TEST")
dbutils.widgets.text("secret_scope",       "tpcdi_snowflake")

src_catalog  = dbutils.widgets.get("catalog")
scale_factor = dbutils.widgets.get("scale_factor")
sf_db        = dbutils.widgets.get("snowflake_database")
secret_scope = dbutils.widgets.get("secret_scope")

src_schema = f"tpcdi_incremental_staging_{scale_factor}"
sf_schema  = f"STAGING_SF{scale_factor}"
print(f"src = {src_catalog}.{src_schema}")
print(f"sink = {sf_db}.{sf_schema}")

# COMMAND ----------

def _secret(name, default=None):
    try: return dbutils.secrets.get(scope=secret_scope, key=name)
    except Exception: return default

account   = _secret("account")
user      = _secret("user")
role      = _secret("role") or "ACCOUNTADMIN"
warehouse = _secret("warehouse") or "BARROW_XS_GEN2"
pk_pem    = _secret("private_key")

if not (account and user and pk_pem):
    raise RuntimeError(f"Secret scope '{secret_scope}' is missing account/user/private_key")

# COMMAND ----------

# Snowflake connector wants the private key as DER bytes, not PEM. Convert.
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
    account=account,
    user=user,
    role=role,
    warehouse=warehouse,
    database=sf_db,
    private_key=pk_der,
)
cur = conn.cursor()
cur.execute(f"CREATE SCHEMA IF NOT EXISTS {sf_db}.{sf_schema}")
cur.execute(f"USE SCHEMA {sf_db}.{sf_schema}")
print(f"[ok] {sf_db}.{sf_schema} exists")

# COMMAND ----------

STAGING_TABLES = [
    "taxrate", "dimdate", "industry", "tradetype", "dimbroker",
    "dimsecurity", "statustype", "dimcompany", "dimtime", "financial",
    "companyyeareps", "currentaccountbalances", "dimaccount",
]

# COMMAND ----------

import time
for t in STAGING_TABLES:
    src_fq = f"{src_catalog}.{src_schema}.{t}"
    t0 = time.time()
    try:
        pdf = spark.read.table(src_fq).toPandas()
    except Exception as e:
        print(f"[skip] {src_fq} not found: {e}")
        continue

    # Snowflake column names are case-sensitive when quoted. write_pandas
    # uppercases by default; force matching by setting column names upper.
    pdf.columns = [c.upper() for c in pdf.columns]

    # OVERWRITE: drop + recreate via auto_create_table=True. write_pandas
    # infers the schema from the DataFrame dtypes.
    target = t.upper()
    cur.execute(f"DROP TABLE IF EXISTS {sf_db}.{sf_schema}.{target}")
    success, nchunks, nrows, _ = write_pandas(
        conn, pdf, target,
        database=sf_db, schema=sf_schema,
        auto_create_table=True, overwrite=False,
        quote_identifiers=False,
    )
    if not success:
        raise RuntimeError(f"write_pandas failed for {t}")
    print(f"[seed] {t}: {nrows:,} rows  ({time.time()-t0:.1f}s)")

# COMMAND ----------

cur.close()
conn.close()
print(f"[done] {sf_db}.{sf_schema} seeded. Per-run setup_sf.py can now CLONE from it.")
