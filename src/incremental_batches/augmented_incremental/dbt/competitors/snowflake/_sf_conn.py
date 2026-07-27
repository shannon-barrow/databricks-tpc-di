# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "snowflake-connector-python",
# ]
# ///
# Shared Snowflake connection helper for the TPC-DI augmented incremental
# Snowflake workflow notebooks. Returns a live snowflake.connector connection.
#
# Secret contract (rework, no backward compat):
#   account / user / warehouse / role  — plain values (job params / widgets),
#                                         NOT secrets.
#   sf_credential_secret               — full UC secret PATH (e.g.
#                                         "main.tpcdi_raw_data.snowflake_cred_secret")
#                                         resolving to EITHER a PEM private key
#                                         OR a password. Auth mode is decided
#                                         by sniffing the resolved value.
#
# Usage from a calling notebook:
#   %run ./_sf_conn
#   ctx = sf_connect(database=catalog, schema=f"{wh_db}_{scale_factor}",
#                    account=account, user=sf_user, warehouse=warehouse,
#                    sf_credential_secret="main.tpcdi_raw_data.snowflake_cred_secret")
#   with ctx.cursor() as cur:
#       cur.execute("...")

import os


def _secret_from_path(path):
    catalog, schema, key = path.split(".", 2)
    return dbutils.secrets.get(catalog=catalog, schema=schema, key=key)  # noqa: F821


def _maybe_install_connector():
    """No-op if snowflake.connector already imports; else pip-install it."""
    try:
        import snowflake.connector  # noqa: F401
    except ImportError:
        import subprocess, sys
        subprocess.check_call([sys.executable, "-m", "pip", "install",
                               "--quiet", "snowflake-connector-python[secure-local-storage]"])
        import importlib, snowflake.connector  # noqa: F401


def sf_connect(*, database: str | None = None, schema: str | None = None,
               account: str | None = None, user: str | None = None,
               warehouse: str | None = None, role: str | None = None,
               sf_credential_secret: str | None = None,
               query_tag: str | dict | None = None):
    """Open a Snowflake connection.

    account / user / warehouse / role are plain values passed in by the
    caller (job params / widgets). The one real secret is
    `sf_credential_secret` — a full UC secret path. Its resolved value is
    used as a PEM private key when it looks like one (contains "BEGIN" and
    "PRIVATE KEY"); otherwise it is treated as a password (MFA must already
    be cached on the account, or the user must allow password-only auth).

    query_tag (str or dict) is stamped on every query issued through this
    connection. The task-time extract reads it from
    SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY to attribute queries back to
    specific tasks/runs without scraping logs. Pass a dict and we'll
    JSON-encode it (matches the shape run_dbt.py uses)."""
    _maybe_install_connector()
    import snowflake.connector

    if not account or not user:
        raise RuntimeError(
            "Snowflake connection requires plain 'account' and 'user' values "
            "(pass them as job params / widgets — they are no longer secrets)."
        )
    if not sf_credential_secret:
        raise RuntimeError(
            "sf_credential_secret is required — pass the full UC secret path "
            "(e.g. 'main.tpcdi_raw_data.snowflake_cred_secret') to the password "
            "OR PEM private key."
        )

    credential = _secret_from_path(sf_credential_secret)

    if "BEGIN" in credential and "PRIVATE KEY" in credential:
        # PEM private key → keypair auth (preferred for unattended runs).
        from cryptography.hazmat.primitives import serialization
        from cryptography.hazmat.backends import default_backend
        pk = serialization.load_pem_private_key(
            credential.encode("utf-8"), password=None, backend=default_backend()
        )
        pk_der = pk.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
        conn_kwargs = dict(account=account, user=user, private_key=pk_der)
    else:
        conn_kwargs = dict(account=account, user=user, password=credential,
                           authenticator="username_password_mfa",
                           client_request_mfa_token=True)

    if role:      conn_kwargs["role"]      = role
    if warehouse: conn_kwargs["warehouse"] = warehouse
    if database:  conn_kwargs["database"]  = database
    if schema:    conn_kwargs["schema"]    = schema

    conn = snowflake.connector.connect(**conn_kwargs)

    if query_tag is not None:
        import json
        tag = query_tag if isinstance(query_tag, str) else json.dumps(query_tag, separators=(",", ":"))
        with conn.cursor() as _cur:
            _cur.execute(f"ALTER SESSION SET QUERY_TAG = $${tag}$$")

    return conn
