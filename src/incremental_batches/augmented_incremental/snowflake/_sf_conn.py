# Databricks notebook source
# Shared Snowflake connection helper for the TPC-DI augmented incremental
# Snowflake workflow notebooks. Reads credentials from a Databricks
# secret scope and returns a live snowflake.connector connection.
#
# Secret scope layout (default scope name `tpcdi_snowflake`):
#   account      — Snowflake account identifier (e.g. tla33105 or kponzso-bwa95870)
#   user         — Snowflake user
#   private_key  — RSA private key PEM (preferred) OR
#   password     — Snowflake password (fallback; will require MFA if enabled)
#   role         — Snowflake role to assume
#   warehouse    — default warehouse for the session
#
# Usage from a calling notebook:
#   %run ./_sf_conn
#   ctx = sf_connect(database=catalog, schema=f"{wh_db}_{scale_factor}")
#   with ctx.cursor() as cur:
#       cur.execute("...")

import os

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
               warehouse: str | None = None, role: str | None = None,
               secret_scope: str = "tpcdi_snowflake"):
    """Open a Snowflake connection using creds from a Databricks secret scope.

    Prefers private-key auth when the `private_key` secret is set; otherwise
    falls back to password (which means MFA needs to already be cached on
    the account, or the user must allow password-only auth)."""
    _maybe_install_connector()
    import snowflake.connector

    def _get(name, default=None):
        try:
            return dbutils.secrets.get(scope=secret_scope, key=name)  # noqa: F821
        except Exception:
            return default

    account   = _get("account")
    user      = _get("user")
    role      = role or _get("role")
    warehouse = warehouse or _get("warehouse")
    if not account or not user:
        raise RuntimeError(
            f"Snowflake secret scope '{secret_scope}' is missing 'account' or 'user'."
        )

    pk_pem = _get("private_key")
    if pk_pem:
        # Keypair auth — preferred for unattended runs.
        from cryptography.hazmat.primitives import serialization
        from cryptography.hazmat.backends import default_backend
        pk = serialization.load_pem_private_key(
            pk_pem.encode("utf-8"), password=None, backend=default_backend()
        )
        pk_der = pk.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
        conn_kwargs = dict(account=account, user=user, private_key=pk_der)
    else:
        password = _get("password")
        if not password:
            raise RuntimeError(
                f"Neither 'private_key' nor 'password' is set under "
                f"secret scope '{secret_scope}'."
            )
        conn_kwargs = dict(account=account, user=user, password=password,
                           authenticator="username_password_mfa",
                           client_request_mfa_token=True)

    if role:      conn_kwargs["role"]      = role
    if warehouse: conn_kwargs["warehouse"] = warehouse
    if database:  conn_kwargs["database"]  = database
    if schema:    conn_kwargs["schema"]    = schema

    conn = snowflake.connector.connect(**conn_kwargs)
    return conn
