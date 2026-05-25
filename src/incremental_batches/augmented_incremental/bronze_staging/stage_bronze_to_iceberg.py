# Databricks notebook source
# MAGIC %md
# MAGIC # Stage Bronze Events → Iceberg (for Snowflake DT variant)
# MAGIC
# MAGIC Reads `tpcdi_raw_data.<source>{sf}` Delta tables filtered to
# MAGIC `stg_target='tables'` (the historical pre-2016-07-06 partition
# MAGIC normally consumed by the historical SCD2 builds), projects to the
# MAGIC same bronze schema the dbt-Snowflake bronze models expect, and
# MAGIC writes one Iceberg-UniForm Delta table per dataset at
# MAGIC `main.tpcdi_incremental_staging_{sf}.bronze<dataset>`.
# MAGIC
# MAGIC These tables are read by Snowflake via the federated UC Iceberg
# MAGIC catalog (`STAGING_SF{sf}_DBX.bronze<dataset>`) and consumed
# MAGIC directly from there — no native Snowflake bronze copy. The DT
# MAGIC variant's `setup_sf_dt.py` seeds `bronze*_raw` via
# MAGIC `INSERT … SELECT * FROM STAGING_SF{sf}_DBX.bronze<dataset>`,
# MAGIC preserving Iceberg-native types through the federation.
# MAGIC
# MAGIC Projections mirror the corresponding `stage_files/<Dataset>.py`
# MAGIC notebooks line-for-line — same source filter (other than
# MAGIC `stg_target` swap) and same column mappings — so the historical
# MAGIC bronze events and the per-batch CSV drops have identical shape.
# MAGIC
# MAGIC Gated by the parent workflow's `generate_bronze_staging=YES`
# MAGIC parameter; this notebook is only added to the DAG when the flag
# MAGIC is explicitly set.

# COMMAND ----------

import sys

dbutils.widgets.dropdown("dataset", "customer",
                          ["customer", "account", "cashtransaction",
                           "dailymarket", "holdings", "trade", "watches"])
dbutils.widgets.dropdown("scale_factor", "10",
                          ["10", "100", "1000", "5000", "10000", "20000"])
dbutils.widgets.text("catalog",        "main")
dbutils.widgets.text("staging_schema", "tpcdi_incremental_staging",
                     "Base name; the per-SF suffix is appended automatically")

dataset        = dbutils.widgets.get("dataset").strip().lower()
scale_factor   = dbutils.widgets.get("scale_factor").strip()
catalog        = dbutils.widgets.get("catalog").strip()
staging_schema = dbutils.widgets.get("staging_schema").strip()

src_db = f"{catalog}.tpcdi_raw_data"
tgt_db = f"{catalog}.{staging_schema}_{scale_factor}"
tgt_table = f"{tgt_db}.bronze{dataset}"
print(f"[stage_bronze] {dataset} → {tgt_table}")

# Ensure target schema exists (idempotent — augmented_staging Stage 0 normally
# creates it but this notebook may be run standalone for ad-hoc seeding).
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {tgt_db}")

# COMMAND ----------

# Per-dataset (column-list, source view SQL) — projections are 1:1 copies of
# the corresponding stage_files notebook, only changing the stg_target filter
# from 'files' (per-batch) to 'tables' (historical).
PROJECTIONS = {
    "customer": (
        "cdc_flag STRING, cdc_dsn BIGINT, customerid BIGINT, taxid STRING, "
        "status STRING, lastname STRING, firstname STRING, middleinitial STRING, "
        "gender STRING, tier TINYINT, dob DATE, addressline1 STRING, "
        "addressline2 STRING, postalcode STRING, city STRING, stateprov STRING, "
        "country STRING, c_ctry_1 STRING, c_area_1 STRING, c_local_1 STRING, "
        "c_ext_1 STRING, c_ctry_2 STRING, c_area_2 STRING, c_local_2 STRING, "
        "c_ext_2 STRING, c_ctry_3 STRING, c_area_3 STRING, c_local_3 STRING, "
        "c_ext_3 STRING, email1 STRING, email2 STRING, lcl_tx_id STRING, "
        "nat_tx_id STRING, update_dt DATE",
        f"""
        SELECT
          CASE WHEN ActionType = 'NEW' THEN 'I' ELSE 'U' END AS cdc_flag,
          cdc_dsn,
          customerid,
          taxid,
          CASE WHEN ActionType IN ('CLOSEACCT', 'INACT') THEN 'INAC' ELSE 'ACTV' END AS status,
          lastname, firstname, middleinitial, gender, tier, dob,
          addressline1, addressline2, postalcode, city, stateprov, country,
          c_ctry_1, c_area_1, c_local_1, c_ext_1,
          c_ctry_2, c_area_2, c_local_2, c_ext_2,
          c_ctry_3, c_area_3, c_local_3, c_ext_3,
          email1, email2, lcl_tx_id, nat_tx_id,
          to_date(update_ts) AS update_dt
        FROM {src_db}.customermgmt{scale_factor}
        WHERE stg_target = 'tables'
          AND ActionType IN ('NEW', 'INACT', 'UPDCUST')
        """,
    ),
    "account": (
        "cdc_flag STRING, cdc_dsn BIGINT, accountid BIGINT, brokerid BIGINT, "
        "customerid BIGINT, accountdesc STRING, taxstatus TINYINT, "
        "status STRING, update_dt DATE",
        f"""
        SELECT
          CASE WHEN ActionType IN ('NEW', 'ADDACCT') THEN 'I' ELSE 'U' END AS cdc_flag,
          cdc_dsn,
          accountid, brokerid, customerid, accountdesc, taxstatus,
          CASE WHEN ActionType IN ('CLOSEACCT', 'INACT') THEN 'INAC' ELSE 'ACTV' END AS status,
          to_date(update_ts) AS update_dt
        FROM {src_db}.customermgmt{scale_factor}
        WHERE stg_target = 'tables'
          AND ActionType NOT IN ('UPDCUST', 'INACT')
        """,
    ),
    "cashtransaction": (
        "cdc_flag STRING, cdc_dsn BIGINT, accountid BIGINT, ct_dts TIMESTAMP, "
        "ct_amt DOUBLE, ct_name STRING, event_dt DATE",
        f"""
        SELECT
          'I' AS cdc_flag,
          cdc_dsn, accountid, ct_dts, ct_amt, ct_name,
          to_date(ct_dts) AS event_dt
        FROM {src_db}.cashtransaction{scale_factor}
        WHERE stg_target = 'tables'
        """,
    ),
    "dailymarket": (
        "cdc_flag STRING, cdc_dsn BIGINT, dm_date DATE, dm_s_symb STRING, "
        "dm_close DOUBLE, dm_high DOUBLE, dm_low DOUBLE, dm_vol INT",
        f"""
        SELECT
          'I' AS cdc_flag,
          cdc_dsn, dm_date, dm_s_symb, dm_close, dm_high, dm_low, dm_vol
        FROM {src_db}.dailymarket{scale_factor}
        WHERE stg_target = 'tables'
        """,
    ),
    "holdings": (
        "cdc_flag STRING, cdc_dsn BIGINT, hh_h_t_id BIGINT, hh_t_id BIGINT, "
        "hh_before_qty INT, hh_after_qty INT, event_dt DATE",
        f"""
        SELECT
          'I' AS cdc_flag,
          cdc_dsn, hh_h_t_id, hh_t_id, hh_before_qty, hh_after_qty, event_dt
        FROM {src_db}.holdinghistory{scale_factor}
        WHERE stg_target = 'tables'
        """,
    ),
    "trade": (
        "cdc_flag STRING, cdc_dsn BIGINT, tradeid BIGINT, t_dts TIMESTAMP, "
        "status STRING, t_tt_id STRING, cashflag TINYINT, t_s_symb STRING, "
        "quantity INT, bidprice DOUBLE, t_ca_id BIGINT, executedby STRING, "
        "tradeprice DOUBLE, fee DOUBLE, commission DOUBLE, tax DOUBLE, "
        "event_dt DATE",
        f"""
        SELECT
          CASE
            WHEN row_number() OVER (PARTITION BY th.tradeid ORDER BY th.th_dts) = 1
            THEN 'I' ELSE 'U'
          END AS cdc_flag,
          th.cdc_dsn,
          th.tradeid,
          th.th_dts AS t_dts,
          th.status,
          t.t_tt_id,
          t.t_is_cash AS cashflag,
          t.t_s_symb,
          t.quantity,
          t.bidprice,
          t.t_ca_id,
          t.executedby,
          CASE WHEN th.status = 'CMPT' THEN t.tradeprice END AS tradeprice,
          CASE WHEN th.status = 'CMPT' THEN t.fee        END AS fee,
          CASE WHEN th.status = 'CMPT' THEN t.commission END AS commission,
          CASE WHEN th.status = 'CMPT' THEN t.tax        END AS tax,
          to_date(th.th_dts) AS event_dt
        FROM {src_db}.tradehistory{scale_factor} th
        JOIN {src_db}.trade{scale_factor} t
          ON t.t_id = th.tradeid
        WHERE th.stg_target = 'tables'
        """,
    ),
    "watches": (
        "cdc_flag STRING, cdc_dsn BIGINT, w_c_id BIGINT, w_s_symb STRING, "
        "w_dts TIMESTAMP, w_action STRING, event_dt DATE",
        f"""
        SELECT
          'I' AS cdc_flag,
          cdc_dsn, w_c_id, w_s_symb, w_dts, w_action,
          to_date(w_dts) AS event_dt
        FROM {src_db}.watchhistory{scale_factor}
        WHERE stg_target = 'tables'
        """,
    ),
}

if dataset not in PROJECTIONS:
    raise ValueError(f"unknown dataset {dataset!r} — expected one of {list(PROJECTIONS)}")
schema_ddl, source_sql = PROJECTIONS[dataset]

# COMMAND ----------

# Plain Delta — no CLUSTER BY (pure pass-through tables, full-scan only)
# and no UniForm/IcebergCompatV2 (added lazily by setup_sf_dt's one-time
# bootstrap path via ALTER TABLE, only when Snowflake federation needs it).
create_ddl = f"""
CREATE OR REPLACE TABLE {tgt_table} (
  {schema_ddl}
)
USING DELTA
TBLPROPERTIES (
  'delta.autoOptimize.autoCompact'   = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.columnMapping.mode'         = 'name'
)
"""

print(f"[ddl] CREATE OR REPLACE {tgt_table}")
spark.sql(create_ddl)

# COMMAND ----------

# Single INSERT — full historical events partition. Driver-side projection
# only; Spark distributes the read+write.
insert_sql = f"INSERT INTO {tgt_table}\n{source_sql.strip()}"
print(f"[insert] {dataset}: writing historical events")
import time as _t
_t0 = _t.time()
spark.sql(insert_sql)
_n = spark.sql(f"SELECT COUNT(*) AS n FROM {tgt_table}").collect()[0]["n"]
print(f"[done] {tgt_table}: {_n:,} rows in {_t.time() - _t0:.1f}s")

# Surface row count as a task value so audit_emit / debug can see it.
dbutils.jobs.taskValues.set(key=f"bronze_staging_count_{dataset}", value=int(_n))
