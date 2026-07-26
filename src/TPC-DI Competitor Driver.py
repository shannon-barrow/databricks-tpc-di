# Databricks notebook source

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
    dbutils.widgets.text("sf_credential_secret", "main.tpcdi_snowflake.password",
                         "SF: Credential secret path (catalog.schema.key)")
    dbutils.widgets.text("sf_dbx_pat_secret", "main.tpcdi_snowflake.dbx_pat",
                         "SF: Databricks PAT secret path (for federation)")
elif competitor == "redshift":
    dbutils.widgets.text("rs_host", "", "RS: Workgroup endpoint")
    dbutils.widgets.text("rs_user", "", "RS: User")
    dbutils.widgets.text("rs_iam_role", "", "RS: IAM role ARN (for COPY)")
    dbutils.widgets.text("rs_password_secret", "main.tpcdi_redshift.password",
                         "RS: Password secret path (catalog.schema.key)")
    dbutils.widgets.text("rs_s3_volume_prefix", "", "RS: S3 volume prefix (s3://.../tpcdi/)")
    dbutils.widgets.text("rs_database", "dev", "RS: Database")
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
    engine_params = dict(
        account=dbutils.widgets.get("sf_account"),
        sf_user=dbutils.widgets.get("sf_user"),
        snowflake_warehouse=dbutils.widgets.get("sf_warehouse"),
        snowflake_stage=dbutils.widgets.get("sf_stage"),
        sf_credential_secret=dbutils.widgets.get("sf_credential_secret"),
        dbx_pat_secret=dbutils.widgets.get("sf_dbx_pat_secret"),
    )
elif competitor == "redshift":
    engine_params = dict(
        rs_host=dbutils.widgets.get("rs_host"),
        rs_user=dbutils.widgets.get("rs_user"),
        rs_iam_role=dbutils.widgets.get("rs_iam_role"),
        rs_password_secret=dbutils.widgets.get("rs_password_secret"),
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
