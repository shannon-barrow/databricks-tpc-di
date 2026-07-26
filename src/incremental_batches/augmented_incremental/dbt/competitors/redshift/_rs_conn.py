# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "psycopg2-binary",
# ]
# ///
# Shared Redshift connection helper for the TPC-DI augmented incremental
# Redshift workflow notebooks. Returns a live psycopg2 connection.
#
# Secret contract: only the password is a genuine UC secret. It arrives as a
# FULL UC secret path (`catalog.schema.key`, e.g. "main.tpcdi_redshift.password")
# and is resolved via `_secret_from_path`. Everything else is a plain value
# passed in directly by the caller (job params / notebook widgets):
#   host       — Redshift Serverless workgroup endpoint
#                e.g. <workgroup>.<account-id>.<region>.redshift-serverless.amazonaws.com
#   port       — JDBC/PG wire port, default 5439
#   database   — default database, e.g. "dev"
#   user       — Redshift user (a service account is recommended over admin)
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


def _secret_from_path(path):
    """Resolve a full UC secret path "catalog.schema.key" to its value.

    maxsplit=2 so a key containing dots still resolves — the first two dots
    delimit catalog + schema, everything after is the key.
    """
    catalog, schema, key = path.split(".", 2)
    return dbutils.secrets.get(catalog=catalog, schema=schema, key=key)  # noqa: F821


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
               host: str | None = None,
               port: str = "5439",
               user: str | None = None,
               rs_password_secret: str | None = None,
               query_group: str | dict | None = None,
               autocommit: bool = True):
    """Open a Redshift connection.

    host / user / port / database are plain values passed in directly. The
    password is the only genuine secret — `rs_password_secret` is a full UC
    secret path (e.g. "main.tpcdi_redshift.password") resolved at connect.

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

    password = _secret_from_path(rs_password_secret) if rs_password_secret else None
    dbname   = database or "dev"

    missing = [k for k, v in [("host", host), ("user", user),
                              ("rs_password_secret", rs_password_secret)] if not v]
    if missing:
        raise RuntimeError(
            f"Redshift connection missing required value(s): {missing} "
            f"(host/user are plain params; password comes from the UC secret "
            f"path in rs_password_secret)"
        )

    # Redshift Serverless auto-suspends when idle; the first connection after
    # a suspend has to wait out the workgroup resume, which can take longer
    # than a single connect_timeout window (the socket times out before the
    # workgroup finishes waking). Retry with backoff so a cold workgroup wakes
    # on an early attempt and a later one connects, instead of failing the run.
    import time as _time
    last_err = None
    for _attempt in range(5):
        try:
            conn = psycopg2.connect(
                host=host, port=int(port), user=user, password=password,
                dbname=dbname, sslmode="require", connect_timeout=60,
                # TCP keepalives (probe after 30s idle, 3x at 10s) so a long-idle
                # socket isn't silently dropped mid-pipeline by Redshift Serverless.
                keepalives=1, keepalives_idle=30,
                keepalives_interval=10, keepalives_count=3,
            )
            break
        except psycopg2.OperationalError as e:
            last_err = e
            if _attempt == 4:
                raise
            _time.sleep(15)
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


def rs_iam_role(*, iam_role: str) -> str:
    """Return the IAM role ARN used by Redshift COPY statements.

    The ARN is now a PLAIN value (not a secret) — the workgroup identity ARN
    isn't sensitive. This thin passthrough is retained so call sites read
    identically to the old secret-backed contract. Used by setup_rs and the
    dbt bronze pre_hooks when issuing `COPY ... IAM_ROLE '<arn>' ...`.
    """
    if not iam_role:
        raise RuntimeError("rs_iam_role: iam_role is required (plain param, "
                           "e.g. arn:aws:iam::<account-id>:role/<role-name>)")
    return iam_role
