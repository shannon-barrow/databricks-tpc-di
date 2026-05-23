# Databricks notebook source
# Per-batch raw-table load for the Dynamic Tables variant. Replaces the
# dbt bronze ingestion step.
#
# For each of the 6 bronze CDC tables, COPY INTO the day's file from the
# Snowflake stage into the corresponding `bronze*_raw` regular table. The
# bronze* dynamic tables then incrementally re-evaluate their projection
# over the new raw rows.
#
# Also handles DailyMarket — but that goes straight into `bronzedailymarket`
# (a regular cloned table, not a raw → DT pair), mirroring the dbt-variant
# flow where bronzedailymarket is read as a stage file.
#
# Idempotency: COPY INTO with the default LOAD HISTORY tracking. Each file
# path includes {batch_date}, so the same physical file is loaded once. Re-
# running the same batch_date is a no-op (Snowflake skips already-loaded
# files). The bronze* DTs also dedup on cdc_dsn defensively.

import json, time

# COMMAND ----------

dbutils.widgets.text("catalog",        "TPCDI_TEST")
dbutils.widgets.text("wh_db",          "")
dbutils.widgets.dropdown("scale_factor", "10", ["10","100","1000","5000","10000","20000"])
dbutils.widgets.text("batch_date",     "")
dbutils.widgets.text("snowflake_stage","TPCDI_STAGE")
dbutils.widgets.text("secret_scope",   "tpcdi_snowflake")
dbutils.widgets.text("snowflake_warehouse", "BARROW_MED_GEN2")

catalog          = dbutils.widgets.get("catalog")
wh_db            = dbutils.widgets.get("wh_db")
scale_factor     = dbutils.widgets.get("scale_factor")
batch_date       = dbutils.widgets.get("batch_date")
snowflake_stage  = dbutils.widgets.get("snowflake_stage")
secret_scope     = dbutils.widgets.get("secret_scope")
warehouse        = dbutils.widgets.get("snowflake_warehouse")

if not (wh_db and batch_date):
    raise ValueError("wh_db and batch_date are required")

target_schema = f"{wh_db}_{scale_factor}"
stage_root    = f"@{snowflake_stage}/{target_schema}/{batch_date}"
print(f"target = {catalog}.{target_schema}")
print(f"stage  = {stage_root}")

# COMMAND ----------

# MAGIC %run ../_sf_conn

# COMMAND ----------

conn = sf_connect(
    database=catalog,
    schema=target_schema,
    secret_scope=secret_scope,
    warehouse=warehouse,
    query_tag={
        "batch_date":   batch_date,
        "wh_db":        wh_db,
        "scale_factor": scale_factor,
        "table_format": "dynamic_tables",
        "task":         "seed_raw",
    },
)
print(f"[ok] connected to Snowflake; warehouse = {warehouse}")
cur = conn.cursor()

# COMMAND ----------

# Each entry: (raw_table_name, file_basename, column_list_positional)
#
# column_list_positional matches the schemas declared in setup_sf_dt.py's
# _raw() calls; positional ($1..$N) is the canonical Snowflake stage-read
# pattern.
COPY_TARGETS = [
    ("bronzeaccount_raw", "Account.txt", """
        $1::string  AS cdc_flag,
        $2::bigint  AS cdc_dsn,
        $3::bigint  AS accountid,
        $4::bigint  AS brokerid,
        $5::bigint  AS customerid,
        $6::string  AS accountdesc,
        $7::tinyint AS taxstatus,
        $8::string  AS status,
        $9::date    AS update_dt
    """),
    ("bronzecashtransaction_raw", "CashTransaction.txt", """
        $1::string    AS cdc_flag,
        $2::bigint    AS cdc_dsn,
        $3::bigint    AS accountid,
        $4::timestamp AS ct_dts,
        $5::float     AS ct_amt,
        $6::string    AS ct_name,
        $7::date      AS event_dt
    """),
    ("bronzecustomer_raw", "Customer.txt", """
        $1::string  AS cdc_flag,
        $2::bigint  AS cdc_dsn,
        $3::bigint  AS customerid,
        $4::string  AS taxid,
        $5::string  AS status,
        $6::string  AS lastname,
        $7::string  AS firstname,
        $8::string  AS middleinitial,
        $9::string  AS gender,
        $10::tinyint AS tier,
        $11::date   AS dob,
        $12::string AS addressline1,
        $13::string AS addressline2,
        $14::string AS postalcode,
        $15::string AS city,
        $16::string AS stateprov,
        $17::string AS country,
        $18::string AS c_ctry_1,
        $19::string AS c_area_1,
        $20::string AS c_local_1,
        $21::string AS c_ext_1,
        $22::string AS c_ctry_2,
        $23::string AS c_area_2,
        $24::string AS c_local_2,
        $25::string AS c_ext_2,
        $26::string AS c_ctry_3,
        $27::string AS c_area_3,
        $28::string AS c_local_3,
        $29::string AS c_ext_3,
        $30::string AS email1,
        $31::string AS email2,
        $32::string AS lcl_tx_id,
        $33::string AS nat_tx_id,
        $34::date   AS update_dt
    """),
    ("bronzeholdings_raw", "HoldingHistory.txt", """
        $1::string AS cdc_flag,
        $2::bigint AS cdc_dsn,
        $3::bigint AS hh_h_t_id,
        $4::bigint AS hh_t_id,
        $5::int    AS hh_before_qty,
        $6::int    AS hh_after_qty,
        $7::date   AS event_dt
    """),
    ("bronzetrade_raw", "Trade.txt", """
        $1::string    AS cdc_flag,
        $2::bigint    AS cdc_dsn,
        $3::bigint    AS tradeid,
        $4::timestamp AS t_dts,
        $5::string    AS status,
        $6::string    AS t_tt_id,
        $7::tinyint   AS cashflag,
        $8::string    AS t_s_symb,
        $9::int       AS quantity,
        $10::float    AS bidprice,
        $11::bigint   AS t_ca_id,
        $12::string   AS executedby,
        $13::float    AS tradeprice,
        $14::float    AS fee,
        $15::float    AS commission,
        $16::float    AS tax,
        $17::date     AS event_dt
    """),
    ("bronzewatches_raw", "WatchHistory.txt", """
        $1::string    AS cdc_flag,
        $2::bigint    AS cdc_dsn,
        $3::bigint    AS w_c_id,
        $4::string    AS w_s_symb,
        $5::timestamp AS w_dts,
        $6::string    AS w_action,
        $7::date      AS event_dt
    """),
]

# COMMAND ----------

# Serial COPY INTO. Each one is one statement and Snowflake's COPY parallelizes
# the file read internally; we don't gain much from issuing them concurrently
# at this scale. Stays simple, single connection.
_t_all = time.time()
for (table, basename, cols) in COPY_TARGETS:
    _t0 = time.time()
    sql = f"""
        COPY INTO {catalog}.{target_schema}.{table}
        FROM (
          SELECT {cols.strip().rstrip(',')}
          FROM {stage_root}/{basename}
        )
        FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = '|' SKIP_HEADER = 0)
        ON_ERROR = 'ABORT_STATEMENT'
    """
    cur.execute(sql)
    res = cur.fetchall()
    # res returns rows like [('Account.txt', 'LOADED', n_rows, n_rows, ...), ...]
    # or empty if file was skipped (already loaded).
    n_rows = sum(int(r[2]) for r in res if r and len(r) > 2 and isinstance(r[2], (int, str)) and str(r[2]).isdigit())
    print(f"[copy] {table:30s} {n_rows:>10,} rows  ({time.time() - _t0:5.1f}s)")

# Append today's DailyMarket.txt rows into bronzedailymarket (regular table —
# cloned by setup_sf_dt.py). dbt variant reads this from the stage at query
# time; for DT variant we materialize into the cloned table so the DT DAG
# sees a normal table source.
cur.execute(f"""
    COPY INTO {catalog}.{target_schema}.bronzedailymarket
    FROM (
      SELECT
        $1::string AS cdc_flag,
        $2::bigint AS cdc_dsn,
        $3::date   AS dm_date,
        $4::string AS dm_s_symb,
        $5::float  AS dm_close,
        $6::float  AS dm_high,
        $7::float  AS dm_low,
        $8::int    AS dm_vol
      FROM {stage_root}/DailyMarket.txt
    )
    FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = '|' SKIP_HEADER = 0)
    ON_ERROR = 'ABORT_STATEMENT'
""")
print(f"[copy] bronzedailymarket (appended this batch's DailyMarket.txt)")

print(f"[done] seed_raw {batch_date} complete in {time.time() - _t_all:.1f}s")
cur.close()
conn.close()
