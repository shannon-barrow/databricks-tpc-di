# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "snowflake-connector-python",
# ]
# ///
# Branch: FactHoldings.
#
# Thread A: COPY bronzeholdings (direct dep).
# Thread B: COPY bronzecustomer; COPY bronzeaccount; COPY bronzetrade;
#           ALTER REFRESH dimtrade. dimtrade's REFRESH cascades DOWNSTREAM
#           through dimaccount → AUFC + dimcustomer.
# Converge → ALTER REFRESH factholdings.

# COMMAND ----------

dbutils.widgets.text("catalog",        "TPCDI_TEST")
dbutils.widgets.text("wh_db",          "")
dbutils.widgets.dropdown("scale_factor", "10", ["10","100","1000","5000","10000","20000"])
dbutils.widgets.text("batch_date",     "")
dbutils.widgets.text("account", "", "Snowflake account identifier (plain value, not a secret)")
dbutils.widgets.text("sf_user", "", "Snowflake user (plain value, not a secret)")
dbutils.widgets.text("role", "", "Snowflake role to assume (plain value; empty = connector default)")
dbutils.widgets.text("sf_credential_secret", "main.tpcdi_raw_data.snowflake_cred_secret",
                     "Full UC secret path to the Snowflake credential (password OR PEM private key)")
dbutils.widgets.text("snowflake_warehouse", "BARROW_MED_GEN2")
dbutils.widgets.text("snowflake_stage", "<db>.<schema>.<your_stage>")

catalog       = dbutils.widgets.get("catalog")
wh_db         = dbutils.widgets.get("wh_db")
scale_factor  = dbutils.widgets.get("scale_factor")
batch_date    = dbutils.widgets.get("batch_date")
account       = dbutils.widgets.get("account")
sf_user       = dbutils.widgets.get("sf_user")
role          = dbutils.widgets.get("role") or None
sf_credential_secret = dbutils.widgets.get("sf_credential_secret")
warehouse     = dbutils.widgets.get("snowflake_warehouse")
stage         = dbutils.widgets.get("snowflake_stage")

if not (wh_db and batch_date):
    raise ValueError("wh_db and batch_date are required")

target_schema = f"{wh_db}_{scale_factor}"
stage_root    = f"@{stage}/{target_schema}/{batch_date}"
print(f"target = {catalog}.{target_schema}, stage_root = {stage_root}")

# COMMAND ----------

# MAGIC %run ../_sf_conn

# COMMAND ----------

# MAGIC %run ./_seed_helpers

# COMMAND ----------

results = run_branch(
    sf_connect_fn=sf_connect,
    sf_connect_kwargs=dict(
        database=catalog, schema=target_schema,
        account=account, user=sf_user, role=role,
        sf_credential_secret=sf_credential_secret,
        warehouse=warehouse,
    ),
    catalog=catalog, target_schema=target_schema, stage_root=stage_root,
    query_tag_base={
        "batch_date":   batch_date,
        "wh_db":        wh_db,
        "scale_factor": scale_factor,
        "table_format": "dynamic_tables",
        "task":         "branch_factholdings",
    },
    independent_bronze="bronzeholdings",
    chain_bronzes=["bronzecustomer", "bronzeaccount", "bronzetrade"],
    chain_dim_refresh="dimtrade",
    leaf_dt="factholdings",
)

dbutils.jobs.taskValues.set("branch_results", results)
dbutils.jobs.taskValues.set("branch_wall_s", results["branch_wall_s"])
