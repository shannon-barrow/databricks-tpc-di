# Databricks notebook source
# Shared BigQuery client helper for the TPC-DI augmented incremental
# BigQuery workflow notebooks. Reads a service-account key from a
# Unity Catalog secret and returns a live google.cloud.bigquery.Client.
#
# Secret layout (default UC location `main.tpcdi_bigquery`):
#   sa_json  — full service-account key JSON (single secret)
#
# The SA needs BigQuery Data Editor + BigQuery Job User on the target
# project (default: `<your-bq-project>`).
#
# Usage from a calling notebook:
#   %run ./_bq_conn
#   client = bq_connect(project="<your-bq-project>",
#                       location="us-central1",
#                       sa_json_secret="main.tpcdi_bigquery.sa_json",
#                       query_label={"task": "setup_bq"})
#   client.query("SELECT 1").result()

import json
import os


def _secret_from_path(path):
    catalog, schema, key = path.split(".", 2)
    return dbutils.secrets.get(catalog=catalog, schema=schema, key=key)  # noqa: F821


def _maybe_install_bigquery():
    """No-op if google.cloud.bigquery already imports; else pip-install."""
    try:
        import google.cloud.bigquery  # noqa: F401
    except ImportError:
        import subprocess, sys
        subprocess.check_call([sys.executable, "-m", "pip", "install",
                               "--quiet", "google-cloud-bigquery"])
        import google.cloud.bigquery  # noqa: F401


def bq_connect(*, project: str, location: str = "us-central1",
               sa_json_secret: str = "main.tpcdi_bigquery.sa_json",
               query_label: dict | None = None):
    """Open a BigQuery client using a service-account key from a Unity
    Catalog secret.

    BigQuery's Application Default Credentials chain doesn't work on
    Databricks worker pods (no metadata server), so we always load the
    SA key explicitly.

    `query_label` (dict, optional) is set as the client's default job
    labels — BigQuery stamps these on every job submitted, so the
    INFORMATION_SCHEMA.JOBS_BY_PROJECT view can attribute jobs back to
    specific tasks/runs without scraping logs. Mirrors the Snowflake
    `query_tag` plumbing. BQ label values must be lowercase alphanumeric
    + dash/underscore + max 63 chars; we sanitize the dict values here.
    """
    _maybe_install_bigquery()
    from google.cloud import bigquery
    from google.oauth2 import service_account

    try:
        sa_json = _secret_from_path(sa_json_secret)
    except Exception as e:
        raise RuntimeError(
            f"BigQuery secret at '{sa_json_secret}' is missing "
            f"({type(e).__name__}: {e})"
        )

    sa_info = json.loads(sa_json)
    credentials = service_account.Credentials.from_service_account_info(sa_info)
    client = bigquery.Client(project=project, credentials=credentials,
                             location=location)

    if query_label:
        # BQ label sanitation: lowercase, [a-z0-9-_], max 63 chars per value.
        def _sanitize(v):
            s = str(v).lower()
            return "".join(c if c.isalnum() or c in "-_" else "-" for c in s)[:63]
        client.default_query_job_config = bigquery.QueryJobConfig(
            labels={k: _sanitize(v) for k, v in query_label.items()}
        )

    return client


def bq_credentials_path(*, sa_json_secret: str = "main.tpcdi_bigquery.sa_json",
                         out_dir: str | None = None) -> str:
    """Materialize the SA key JSON to a tempfile and return its path.

    dbt-bigquery's keyfile auth needs a path, not in-memory creds, so the
    run_dbt notebook calls this once per invocation. The file is written
    to a fresh /tmp dir with 0600 perms.
    """
    import tempfile
    try:
        sa_json = _secret_from_path(sa_json_secret)
    except Exception as e:
        raise RuntimeError(
            f"BigQuery secret at '{sa_json_secret}' is missing "
            f"({type(e).__name__}: {e})"
        )
    if out_dir is None:
        out_dir = tempfile.mkdtemp(prefix="bq_sa_")
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, "sa.json")
    with open(path, "w") as f:
        f.write(sa_json)
    os.chmod(path, 0o600)
    return path
