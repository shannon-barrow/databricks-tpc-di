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
# MAGIC same Augmented Incremental workload via dbt, reading the same source data
# MAGIC Databricks generated. This is the counterpart to the Databricks
# MAGIC `TPC-DI Driver`; run the two side by side to compare Databricks vs a
# MAGIC competitor CDW on identical data + business logic.
# MAGIC
# MAGIC ## What this notebook does (and doesn't)
# MAGIC It **creates two Databricks Jobs** (it does not run them):
# MAGIC - a **child** job — one batch's work: `simulate_filedrops_<engine>` drops
# MAGIC   that day's pre-staged files into the shared UC volume, then `dbt_run`
# MAGIC   executes the dbt models against the competitor engine for that batch date.
# MAGIC - a **parent** job — `setup_<engine>` (self-bootstraps the competitor's
# MAGIC   staging + per-run schema) → a `for_each_task` loop that triggers the
# MAGIC   child once per simulated business day → a gated `cleanup`/teardown.
# MAGIC
# MAGIC It does **not** generate data and does **not** create the Databricks
# MAGIC benchmark. **Prerequisite:** the Databricks Augmented Incremental Stage 0
# MAGIC (`augmented_staging`) must have already run for this scale factor, so the
# MAGIC per-day staged files + `tpcdi_incremental_staging_{sf}` schema exist. The
# MAGIC competitor's `setup_<engine>` reads that staging to seed its own copy.
# MAGIC
# MAGIC To actually run the benchmark after creation, trigger the **parent** job
# MAGIC (a smoke test: `scale_factor=10`, `incremental_batches_to_run=2`,
# MAGIC `delete_tables_when_finished=FALSE`).
# MAGIC
# MAGIC ## How to run
# MAGIC 1. Run the **Setup** cell (bootstraps `tpcdi_config`, imports the generators).
# MAGIC 2. Run the **Widgets** cell; pick a **Competitor** first, then run it again
# MAGIC    to reveal that competitor's inputs (the per-engine widgets, and the
# MAGIC    secret-name defaults, only appear once an engine is selected — and the
# MAGIC    secret-name defaults key off the account/user/project fields, so set
# MAGIC    those first and re-run to see the names update).
# MAGIC 3. Fill the inputs, then run the **Create** cell — it validates each UC
# MAGIC    secret, prints a link to the created parent workflow, and reports what
# MAGIC    each secret still needs (see "Credentials" below).
# MAGIC
# MAGIC ## Inputs
# MAGIC **Shared (all engines):** `scale_factor`, `catalog` (the Databricks UC
# MAGIC catalog that hosts the staged data + external volume — almost always
# MAGIC `main`), `wh_db` (target schema prefix; final schema is `{wh_db}_{sf}`),
# MAGIC `job_name_prefix`, `interactive_cluster_id` (blank = serverless, the
# MAGIC zero-config default; pass a cluster id to pin the child tasks to classic
# MAGIC compute instead).
# MAGIC
# MAGIC **Per engine** (plain, non-secret values — usernames/hosts/accounts are
# MAGIC NOT secrets):
# MAGIC | Engine | Plain inputs | Secrets (see below) |
# MAGIC |---|---|---|
# MAGIC | **Redshift** | `rs_host`, `rs_user`, `rs_iam_role`, `rs_s3_volume_prefix`, `rs_database` | password |
# MAGIC | **Snowflake** | `sf_account`, `sf_user`, `sf_warehouse`, `sf_stage`, `sf_database` | login credential (password OR PEM key) + Databricks PAT (federation) |
# MAGIC | **BigQuery** | `bq_project`, `bq_gcs_volume_prefix` | service-account key JSON |
# MAGIC
# MAGIC Note `sf_database` / `bq_project` are the *engine's* namespace and are
# MAGIC distinct from the Databricks UC `catalog` (which stays `main` and drives
# MAGIC the volume path). Redshift's engine catalog is the same as the UC catalog.
# MAGIC
# MAGIC ## Credentials use Unity Catalog secrets
# MAGIC Only genuine secrets (passwords / keys / tokens) are UC secrets; everything
# MAGIC else above is a plain job parameter. You supply a **secret catalog + schema**
# MAGIC (default `main.tpcdi_raw_data`) plus a **secret name**; the Create cell
# MAGIC assembles the full `catalog.schema.name` path and the job reads it at run
# MAGIC time via `dbutils.secrets.get`.
# MAGIC
# MAGIC - **Named for what they unlock.** Secret-name defaults are derived from the
# MAGIC   target login/account/project (e.g. `redshift_admin_pw_secret`,
# MAGIC   `snowflake_<user>_cred_secret`, `snowflake_<account>_dbx_pat_secret`,
# MAGIC   `bigquery_<project>_sa_json_secret`). This is intentional — the secret is
# MAGIC   **created once per deployment and reused** by anyone on the team, exactly
# MAGIC   like the shared staged data. Name collisions across users are the point.
# MAGIC - **This notebook never creates secrets** — it only consumes them. Create
# MAGIC   them once via Catalog Explorer (the schema → Create secret) or the
# MAGIC   `POST /api/2.1/unity-catalog/secrets` API.
# MAGIC - **Validation, not blocking.** The Create cell checks each referenced
# MAGIC   secret and prints one of: ✅ exists + readable; ⚠️ exists but you lack
# MAGIC   READ (it names the owner to request access from); ⚠️ not created yet. The
# MAGIC   job is still created either way — you just can't run it until the secret
# MAGIC   resolves.
# MAGIC - **Runtime requirement:** UC secret reads need **serverless env v5+** (or
# MAGIC   DBR 17.3 LTS+). Below v5, `dbutils.secrets.get` returns an empty value.
# MAGIC   The workflow builders pin the serverless env to v5. See
# MAGIC   https://docs.databricks.com/aws/en/security/secrets/unity-catalog-secrets
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
# MAGIC ## Compute
# MAGIC All tasks default to **serverless** (env v5) — no cluster config needed.
# MAGIC The child tasks accept an `interactive_cluster_id` to run on classic
# MAGIC compute instead; both paths are supported. Notebooks that need engine
# MAGIC client libraries declare them in their serverless env and still
# MAGIC defensively `pip install` when run on a classic cluster.
# MAGIC
# MAGIC ## Per-engine specifics
# MAGIC - **Redshift** — password-only auth over the PG wire protocol; COPY from S3
# MAGIC   via the IAM role. The connection retries with backoff to absorb Redshift
# MAGIC   Serverless workgroup cold-start (a paused workgroup can exceed a single
# MAGIC   connect timeout).
# MAGIC - **Snowflake** — **two** secrets: the login credential (a PEM private key
# MAGIC   → keypair auth, or a password → MFA, sniffed by content) and a Databricks
# MAGIC   PAT used to refresh the catalog integration for UC federation. Reads the
# MAGIC   staged data through an external stage.
# MAGIC - **BigQuery** — one service-account-key secret; reads staged files from the
# MAGIC   GCS-backed external volume.
# MAGIC
# MAGIC See `dbt/competitors/README.md` for the cross-engine overview and the
# MAGIC per-engine `PORT_NOTES.md` for design detail.

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
    # Snowflake DATABASE the benchmark builds into. This is a Snowflake
    # namespace, NOT the Databricks UC catalog above — the UC catalog (03)
    # still drives the UC volume the staged files land in (same split as
    # BigQuery's project vs databricks_catalog).
    dbutils.widgets.text("sf_database", "TPCDI_TEST", "SF: Snowflake database")
    # Snowflake needs TWO UC secrets, both named for WHAT THEY UNLOCK so each is
    # created once per deployment and reused (collisions intended):
    #   - the login credential (password OR PEM key), named from the SF user
    #   - the Databricks PAT used for federation, named from the SF account
    # catalog/schema default to main.tpcdi_raw_data; set sf_user / sf_account and
    # re-run to see the defaults update.
    dbutils.widgets.text("secret_catalog", "main", "SF: Secret catalog")
    dbutils.widgets.text("secret_schema", "tpcdi_raw_data", "SF: Secret schema")
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
    # re-run to see the default update. catalog/schema default to main.tpcdi_raw_data.
    dbutils.widgets.text("secret_catalog", "main", "RS: Secret catalog")
    dbutils.widgets.text("secret_schema", "tpcdi_raw_data", "RS: Secret schema")
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
    # UC secret holding the service-account key JSON. Named for WHAT IT UNLOCKS
    # (the BQ project), so it's created once per deployment and reused —
    # collisions intended. Defaults from bq_project; set it and re-run to see
    # the default update. catalog/schema default to main.tpcdi_raw_data.
    dbutils.widgets.text("secret_catalog", "main", "BQ: Secret catalog")
    dbutils.widgets.text("secret_schema", "tpcdi_raw_data", "BQ: Secret schema")
    try:
        _bq_project_now = dbutils.widgets.get("bq_project")
    except Exception:
        _bq_project_now = ""
    dbutils.widgets.text("bq_sa_json_secret_name",
                         default_secret_name("bigquery", _bq_project_now, kind="sa_json"),
                         "BQ: Service-account JSON secret name")

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
        sf_database=dbutils.widgets.get("sf_database"),
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
    _sec_cat = dbutils.widgets.get("secret_catalog")
    _sec_sch = dbutils.widgets.get("secret_schema")
    sa_json_secret = (f"{_sec_cat}.{_sec_sch}."
                      f"{dbutils.widgets.get('bq_sa_json_secret_name')}")
    engine_params = dict(
        catalog_project=dbutils.widgets.get("bq_project"),
        gcs_volume_prefix=dbutils.widgets.get("bq_gcs_volume_prefix"),
        sa_json_secret=sa_json_secret,
        bq_location="us-central1",
        databricks_catalog=catalog,
    )

# The engine `catalog` (the namespace the benchmark builds into) is NOT always
# the Databricks UC catalog. UC catalog (`catalog`) always drives tpcdi_directory
# above; the engine catalog can differ:
#   - BigQuery:  the BQ project        (databricks_catalog carries the UC catalog)
#   - Snowflake: the Snowflake database (TPCDI_TEST) — separate from UC catalog
#   - Redshift:  same as the UC catalog
if competitor == "bigquery":
    _effective_catalog = engine_params.pop("catalog_project")
elif competitor == "snowflake":
    _effective_catalog = engine_params.pop("sf_database")
else:
    _effective_catalog = catalog

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
