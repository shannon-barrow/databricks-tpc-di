# Databricks notebook source
# Shared Redshift connection helper for the TPC-DI augmented incremental
# Redshift workflow notebooks. Reads credentials from a Databricks secret
# scope and returns a live psycopg2 connection.
#
# Secret scope layout (default scope name `tpcdi_redshift`):
#   host       — Redshift Serverless workgroup endpoint
#                e.g. <workgroup>.<account-id>.<region>.redshift-serverless.amazonaws.com
#   port       — JDBC/PG wire port, default 5439
#   database   — default database, e.g. "dev"
#   user       — Redshift user (a service account is recommended over admin)
#   password   — password
#   iam_role   — ARN of the IAM role with S3 read access for COPY,
#                e.g. arn:aws:iam::<account-id>:role/<role-name>
#
# Notes on driver choice:
#   psycopg2 talks Redshift's PostgreSQL wire protocol natively. It's the
#   driver dbt-redshift uses under the hood, so connection semantics match
#   what dbt will see. We deliberately don't use jaydebeapi/JDBC (which
#   Srilekha used in the proof-of-life test) — JDBC adds a JVM dependency
#   and indirection we don't need.
#
# Usage from a calling notebook:
#   %run ./_rs_conn
#   ctx = rs_connect(query_group={"task": "setup_rs", "wh_db": wh_db,
#                                 "scale_factor": scale_factor})
#   with ctx.cursor() as cur:
#       cur.execute("...")

import json
import os


def _maybe_install_psycopg2():
    """No-op if psycopg2 already imports; else pip-install psycopg2-binary."""
    try:
        import psycopg2  # noqa: F401
    except ImportError:
        import subprocess, sys
        subprocess.check_call([sys.executable, "-m", "pip", "install",
                               "--quiet", "psycopg2-binary"])
        import psycopg2  # noqa: F401


def rs_connect(*, database: str | None = None,
               schema: str | None = None,
               secret_scope: str = "tpcdi_redshift",
               query_group: str | dict | None = None,
               autocommit: bool = True):
    """Open a Redshift connection using creds from a Databricks secret scope.

    Mirrors `_sf_conn.sf_connect` / `_bq_conn.bq_connect` shape.

    `query_group` (str or dict) is set as the session's QUERY_GROUP after
    connect. Lands in `SYS_QUERY_HISTORY.query_label`, so the cost-attribution
    extract can attribute queries back to specific tasks/runs without
    scraping logs. Dicts are JSON-encoded (matches `query_tag` plumbing on
    the SF side). Redshift caps query_group at 320 chars; we truncate.

    `autocommit=True` by default — most ops in our pipeline are DDL/COPY/CTAS
    that don't need explicit transactions. Set False if calling code needs
    multi-statement atomicity.
    """
    _maybe_install_psycopg2()
    import psycopg2

    def _get(name, default=None):
        try:
            return dbutils.secrets.get(scope=secret_scope, key=name)  # noqa: F821
        except Exception:
            return default

    host     = _get("host")
    port     = int(_get("port", "5439"))
    user     = _get("user")
    password = _get("password")
    dbname   = database or _get("database", "dev")

    missing = [k for k, v in [("host", host), ("user", user),
                              ("password", password)] if not v]
    if missing:
        raise RuntimeError(
            f"Redshift secret scope '{secret_scope}' missing required key(s): {missing}"
        )

    conn = psycopg2.connect(
        host=host, port=port, user=user, password=password,
        dbname=dbname, sslmode="require", connect_timeout=30,
        # TCP keepalives (probe after 30s idle, 3x at 10s) so a long-idle
        # socket isn't silently dropped mid-pipeline by Redshift Serverless.
        keepalives=1, keepalives_idle=30,
        keepalives_interval=10, keepalives_count=3,
    )
    conn.autocommit = autocommit

    with conn.cursor() as cur:
        if schema:
            cur.execute(f'SET SEARCH_PATH TO "{schema}"')
        if query_group is not None:
            tag = query_group if isinstance(query_group, str) \
                  else json.dumps(query_group, separators=(",", ":"))
            # Redshift caps query_group at 320 chars; truncate defensively.
            tag = tag[:320]
            # Escape single quotes for the SET literal.
            tag_esc = tag.replace("'", "''")
            cur.execute(f"SET query_group TO '{tag_esc}'")

    return conn


def rs_iam_role(*, secret_scope: str = "tpcdi_redshift") -> str:
    """Return the IAM role ARN used by Redshift COPY statements.

    Pulled from the secret scope so the workgroup identity stays out of
    source code. Used by `load_bronze_rs` and `setup_rs` when issuing
    `COPY ... IAM_ROLE '<arn>' ...`.
    """
    try:
        return dbutils.secrets.get(scope=secret_scope, key="iam_role")  # noqa: F821
    except Exception as e:
        raise RuntimeError(
            f"Redshift secret scope '{secret_scope}' missing key 'iam_role' "
            f"({type(e).__name__}: {e})"
        )
