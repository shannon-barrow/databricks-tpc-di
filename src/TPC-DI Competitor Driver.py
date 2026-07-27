# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///

# MAGIC %md
# MAGIC # TPC-DI Competitor Benchmark Driver
# MAGIC
# MAGIC Creates the **competitor** (non-Databricks) TPC-DI benchmark workflows —
# MAGIC Snowflake, Amazon Redshift Serverless, or Google BigQuery — that run the
# MAGIC same Augmented Incremental workload via dbt.
# MAGIC
# MAGIC This is the competitor counterpart to `TPC-DI Driver`. It does **not**
# MAGIC generate data and does **not** create the Databricks benchmark. You must
# MAGIC have **already run the Databricks Augmented Incremental benchmark** (from
# MAGIC `TPC-DI Driver`) to generate and stage the data the competitor reads.
# MAGIC
# MAGIC ## How to run
# MAGIC 1. Run the **Setup** cell (bootstraps `tpcdi_config`, imports the generators).
# MAGIC 2. Run the **Widgets** cell; pick a **Competitor** first, then run it again
# MAGIC    to reveal that competitor's inputs.
# MAGIC 3. Fill the inputs, then run the **Create** cell — it prints a link to the
# MAGIC    created workflow.
# MAGIC
# MAGIC ## Why a notebook (not the app)
# MAGIC A notebook runs **as you**, so it creates the competitor job with your own
# MAGIC identity and permissions — no service principal, no extra grants. It works
# MAGIC OOTB on any workspace, exactly like the Databricks driver notebook.
# MAGIC
# MAGIC ## Only same-cloud competitors are offered
# MAGIC TPC-DI data is generated in Databricks and read by the competitor in the
# MAGIC **same cloud/region** (a different region incurs egress). So the
# MAGIC Competitor dropdown only lists engines valid for this workspace's cloud:
# MAGIC AWS → Redshift + Snowflake, GCP → BigQuery + Snowflake, Azure → Snowflake.
# MAGIC
# MAGIC ## Credentials use Unity Catalog secrets
# MAGIC Secret fields take a **full UC secret path** `catalog.schema.key` to a
# MAGIC secret you already created (the job reads it at run time via
# MAGIC `dbutils.secrets.get`). Requires a runtime that supports UC secrets
# MAGIC (DBR 17.3 LTS+ or serverless env v4+). See
# MAGIC https://docs.databricks.com/aws/en/security/secrets/unity-catalog-secrets

# COMMAND ----------

# DBTITLE 1,Setup: bootstrap tpcdi_config + import workflow-generator functions
# MAGIC %run ./tools/setup

# COMMAND ----------

# DBTITLE 1,Widgets: pick a competitor (cloud-filtered), then its inputs
# Progressive widgets, same pattern as the Databricks driver: the Competitor
# dropdown is filtered to this workspace's cloud; picking one and re-running
# reveals that engine's inputs. Shared inputs (scale factor, catalog, schema
# prefix, job-name prefix, batches) always show.

_cloud = tpcdi_config.cloud_provider
_valid = competitors_for_cloud(_cloud)   # e.g. AWS -> ['snowflake','redshift']

dbutils.widgets.dropdown("competitor", _valid[0], _valid, "01 Competitor")
competitor = dbutils.widgets.get("competitor")

# Shared inputs (all competitors).
dbutils.widgets.dropdown("scale_factor", "10",
                         ["10", "100", "1000", "5000", "10000", "20000"],
                         "02 Scale Factor")
dbutils.widgets.text("catalog", "main", "03 UC Catalog")
dbutils.widgets.text("wh_db", tpcdi_config.default_wh,
                     "04 Target Schema Prefix (schema = prefix_sf)")
dbutils.widgets.text("job_name_prefix", tpcdi_config.default_job_name,
                     "05 Job Name Prefix")
dbutils.widgets.text("interactive_cluster_id", "",
                     "06 Interactive Cluster ID (blank = serverless)")

# Per-competitor inputs. Each engine's widgets are prefixed so they don't
# collide; only the selected competitor's are read in the Create cell.
if competitor == "snowflake":
    dbutils.widgets.text("sf_account", "", "SF: Account (<org>-<account>)")
    dbutils.widgets.text("sf_user", "", "SF: User")
    dbutils.widgets.text("sf_warehouse", "", "SF: Warehouse")
    dbutils.widgets.text("sf_stage", "", "SF: External Stage (<db>.<schema>.<stage>)")
    # Snowflake needs TWO UC secrets, both named for WHAT THEY UNLOCK so each is
    # created once per deployment and reused (collisions intended):
    #   - the login credential (password OR PEM key), named from the SF user
    #   - the Databricks PAT used for federation, named from the SF account
    # catalog/schema default to main.default; set sf_user / sf_account and
    # re-run to see the defaults update.
    dbutils.widgets.text("secret_catalog", "main", "SF: Secret catalog")
    dbutils.widgets.text("secret_schema", "default", "SF: Secret schema")
    try:
        _sf_user_now = dbutils.widgets.get("sf_user")
    except Exception:
        _sf_user_now = ""
    try:
        _sf_account_now = dbutils.widgets.get("sf_account")
    except Exception:
        _sf_account_now = ""
    dbutils.widgets.text("sf_credential_secret_name",
                         default_secret_name("snowflake", _sf_user_now, kind="cred"),
                         "SF: Credential secret name (password OR PEM key)")
    dbutils.widgets.text("sf_dbx_pat_secret_name",
                         default_secret_name("snowflake", _sf_account_now, kind="dbx_pat"),
                         "SF: Databricks PAT secret name (for federation)")
elif competitor == "redshift":
    dbutils.widgets.text("rs_host", "", "RS: Workgroup endpoint")
    dbutils.widgets.text("rs_user", "", "RS: User")
    dbutils.widgets.text("rs_iam_role", "", "RS: IAM role ARN (for COPY)")
    dbutils.widgets.text("rs_s3_volume_prefix", "", "RS: S3 volume prefix (s3://.../tpcdi/)")
    dbutils.widgets.text("rs_database", "dev", "RS: Database")
    # UC secret for the password. Named for WHAT IT UNLOCKS (the Redshift user),
    # so it's created once per deployment and reused by anyone on the team —
    # collisions are intended. The name defaults from rs_user; set rs_user and
    # re-run to see the default update. catalog/schema default to main.default.
    dbutils.widgets.text("secret_catalog", "main", "RS: Secret catalog")
    dbutils.widgets.text("secret_schema", "default", "RS: Secret schema")
    try:
        _rs_user_now = dbutils.widgets.get("rs_user")
    except Exception:
        _rs_user_now = ""
    dbutils.widgets.text("rs_password_secret_name",
                         default_secret_name("redshift", _rs_user_now, kind="pw"),
                         "RS: Password secret name (in catalog.schema above)")
elif competitor == "bigquery":
    dbutils.widgets.text("bq_project", "", "BQ: Project id")
    dbutils.widgets.text("bq_gcs_volume_prefix", "", "BQ: GCS volume prefix (gs://.../tpcdi/)")
    dbutils.widgets.text("bq_sa_json_secret", "main.tpcdi_bigquery.sa_json",
                         "BQ: Service-account JSON secret path")

print(f"Cloud: {_cloud}  |  competitors available: {_valid}")
print(f"Selected competitor: {competitor}")
print("Fill the inputs above, then run the Create cell.")

# COMMAND ----------

# DBTITLE 1,Create the competitor benchmark workflow
scale_factor    = int(dbutils.widgets.get("scale_factor"))
catalog         = dbutils.widgets.get("catalog")
wh_db           = dbutils.widgets.get("wh_db")
job_name_prefix = dbutils.widgets.get("job_name_prefix")
_cluster_id     = dbutils.widgets.get("interactive_cluster_id").strip() or None
tpcdi_directory = f"/Volumes/{catalog}/tpcdi_raw_data/tpcdi_volume/"

if competitor == "snowflake":
    # Assemble both full UC secret paths from catalog.schema.name.
    _sec_cat = dbutils.widgets.get("secret_catalog")
    _sec_sch = dbutils.widgets.get("secret_schema")
    sf_credential_secret = (f"{_sec_cat}.{_sec_sch}."
                            f"{dbutils.widgets.get('sf_credential_secret_name')}")
    dbx_pat_secret = (f"{_sec_cat}.{_sec_sch}."
                      f"{dbutils.widgets.get('sf_dbx_pat_secret_name')}")
    engine_params = dict(
        account=dbutils.widgets.get("sf_account"),
        sf_user=dbutils.widgets.get("sf_user"),
        snowflake_warehouse=dbutils.widgets.get("sf_warehouse"),
        snowflake_stage=dbutils.widgets.get("sf_stage"),
        sf_credential_secret=sf_credential_secret,
        dbx_pat_secret=dbx_pat_secret,
    )
elif competitor == "redshift":
    # Assemble the full UC secret path from catalog.schema.name.
    _sec_cat = dbutils.widgets.get("secret_catalog")
    _sec_sch = dbutils.widgets.get("secret_schema")
    _sec_name = dbutils.widgets.get("rs_password_secret_name")
    rs_password_secret = f"{_sec_cat}.{_sec_sch}.{_sec_name}"
    engine_params = dict(
        rs_host=dbutils.widgets.get("rs_host"),
        rs_user=dbutils.widgets.get("rs_user"),
        rs_iam_role=dbutils.widgets.get("rs_iam_role"),
        rs_password_secret=rs_password_secret,
        s3_volume_prefix=dbutils.widgets.get("rs_s3_volume_prefix"),
        aws_region=tpcdi_config.region if hasattr(tpcdi_config, "region") else "us-west-2",
        database=dbutils.widgets.get("rs_database"),
    )
elif competitor == "bigquery":
    engine_params = dict(
        catalog_project=dbutils.widgets.get("bq_project"),
        gcs_volume_prefix=dbutils.widgets.get("bq_gcs_volume_prefix"),
        sa_json_secret=dbutils.widgets.get("bq_sa_json_secret"),
        bq_location="us-central1",
        databricks_catalog=catalog,
    )

# BigQuery's engine `catalog` is the BQ project (distinct from the UC catalog).
_effective_catalog = (engine_params.pop("catalog_project")
                      if competitor == "bigquery" else catalog)

# Validate the UC secret(s) this run references. We DON'T block on a missing /
# inaccessible secret — the job is still created referencing it (like the raw
# data, the secret is created once per deployment and reused). We just tell the
# user what they need to do before the job will actually run.
_secret_paths = [v for k, v in engine_params.items() if k.endswith("_secret")]
for _sp in _secret_paths:
    _r = check_uc_secret(_sp, tpcdi_config.api_call)
    if _r["state"] == "ok":
        print(f"✅ UC secret {_sp} — exists and you can read it.")
    elif _r["state"] == "no_access":
        print(f"⚠️  UC secret {_sp} — EXISTS but you lack READ access. "
              f"Request access from its owner: {_r['owner']}. "
              f"The job will be created, but it will fail until you have access.")
    elif _r["state"] == "missing":
        print(f"⚠️  UC secret {_sp} — NOT created yet. The job will be created "
              f"referencing this path; create the secret in Unity Catalog "
              f"(Catalog Explorer → the schema → Create secret, or the "
              f"/api/2.1/unity-catalog/secrets API) before running the job.")
    else:
        print(f"⚠️  UC secret path {_sp} — {_r['detail']}")

parent_job_id = generate_competitor_workflow(
    engine=competitor,
    scale_factor=scale_factor,
    catalog=_effective_catalog,
    wh_db=wh_db,
    tpcdi_directory=tpcdi_directory,
    repo_src_path=tpcdi_config.repo_src_path,
    api_call=tpcdi_config.api_call,
    name_prefix=job_name_prefix,
    interactive_cluster_id=_cluster_id,
    engine_params=engine_params,
)
displayHTML(f"<h2><a href=/#job/{parent_job_id}>{competitor.title()} Benchmark Workflow (parent)</a></h2>")
