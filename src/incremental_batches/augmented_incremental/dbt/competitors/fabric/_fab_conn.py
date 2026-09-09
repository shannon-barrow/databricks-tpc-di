# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "pyodbc",
#   "msal",
# ]
# ///
# Shared Fabric Data Warehouse connection helper for the TPC-DI augmented
# incremental Fabric-DW workflow notebooks. Returns a live pyodbc connection to
# the warehouse's SQL analytics endpoint over TDS, authenticated as an Entra
# service principal.
#
# WHY this differs from the Redshift/_rs_conn.py (psycopg2) helper:
#   Fabric DW speaks the SQL Server TDS protocol, not Postgres wire. The client
#   is pyodbc + the Microsoft ODBC Driver 18 (msodbcsql18). Fabric DW supports
#   ONLY Microsoft Entra ID auth (no SQL logins), so we authenticate with the
#   service principal client-credentials flow.
#
# Auth mechanism (token-based, most reliable for SP -> Fabric DW):
#   MSAL ConfidentialClientApplication acquires an access token for the SQL
#   resource, and we hand it to the ODBC driver via the SQL_COPT_SS_ACCESS_TOKEN
#   (1256) pre-connect attribute. This avoids relying on the driver's own
#   ActiveDirectoryServicePrincipal parsing (which wants UID=appId@tenant and is
#   fiddlier across driver builds).
#
# ⚠ VERIFY AT SMOKE TIME (couldn't test headless without the driver installed):
#   1. The SP (tpcdi-fabric-sp) must have a login/user IN the warehouse. Being
#      workspace Contributor usually maps to a DW role, but if connects are
#      rejected, run once as an admin against the WH:
#        CREATE USER [tpcdi-fabric-sp] FROM EXTERNAL PROVIDER;
#        ALTER ROLE db_owner ADD MEMBER [tpcdi-fabric-sp];
#      (Fabric DW resolves EXTERNAL PROVIDER users by the SP display name.)
#   2. The token resource: Fabric DW accepts tokens minted for the Azure SQL
#      audience "https://database.windows.net/.default". If that is rejected,
#      try "https://analysis.windows.net/powerbi/api/.default".
#
# Secret contract (mirrors _rs_conn): the SP client_id + client_secret are UC
# secrets in scope `tpcdi_fabric`. Host / database / tenant are plain values.
#
# Usage from a calling notebook:
#   %run ./_fab_conn
#   conn = fab_connect(host=..., database=..., tenant_id=..., label={...})
#   with conn.cursor() as cur: cur.execute("...")

import struct


def _install(pkgs):
    import importlib, subprocess, sys
    for mod, pip_name in pkgs:
        try:
            importlib.import_module(mod)
        except ImportError:
            subprocess.check_call([sys.executable, "-m", "pip", "install", "--quiet", pip_name])


def _sp_access_token(*, tenant_id: str, client_id: str, client_secret: str,
                     resource: str = "https://database.windows.net/.default") -> bytes:
    """Client-credentials token for the SP, packed as the ODBC token struct."""
    _install([("msal", "msal")])
    import msal
    app = msal.ConfidentialClientApplication(
        client_id,
        authority=f"https://login.microsoftonline.com/{tenant_id}",
        client_credential=client_secret,
    )
    res = app.acquire_token_for_client(scopes=[resource])
    if "access_token" not in res:
        raise RuntimeError(f"MSAL token acquisition failed: {res.get('error')}: "
                           f"{res.get('error_description')}")
    tok = res["access_token"].encode("utf-16-le")
    # SQL_COPT_SS_ACCESS_TOKEN expects a 4-byte length prefix + the UTF-16 token.
    return struct.pack("<i", len(tok)) + tok


def fab_connect(*, host: str, database: str,
                tenant_id: str,
                client_id_secret: str = "tpcdi_fabric.client_id",
                client_secret_secret: str = "tpcdi_fabric.client_secret",
                secret_scope: str = "tpcdi_fabric",
                token_resource: str = "https://database.windows.net/.default",
                label: str | dict | None = None,
                autocommit: bool = True):
    """Open a Fabric DW connection as the service principal.

    host      — the warehouse SQL analytics endpoint
                (e.g. <...>.datawarehouse.fabric.microsoft.com)
    database  — the warehouse item name (e.g. tpcdi_fabric_dw)
    tenant_id — Entra tenant GUID
    The SP client_id/secret are read from the `tpcdi_fabric` UC secret scope.

    `label` is attached to statements via `OPTION (LABEL='...')` by callers that
    want per-task attribution in queryinsights.exec_requests_history (Fabric DW
    has no Redshift-style session query_group). Returned here for the caller to
    stamp onto its SQL; the connection itself doesn't set a session tag.
    """
    _install([("pyodbc", "pyodbc")])
    import pyodbc

    cid = dbutils.secrets.get(scope=secret_scope, key="client_id")      # noqa: F821
    csec = dbutils.secrets.get(scope=secret_scope, key="client_secret")  # noqa: F821
    if not (host and database and tenant_id and cid and csec):
        raise RuntimeError("fab_connect missing host/database/tenant_id or SP creds "
                           f"(scope={secret_scope})")

    token = _sp_access_token(tenant_id=tenant_id, client_id=cid,
                             client_secret=csec, resource=token_resource)
    conn_str = (
        "Driver={ODBC Driver 18 for SQL Server};"
        f"Server={host},1433;"
        f"Database={database};"
        "Encrypt=yes;TrustServerCertificate=no;"
        # Fabric DW statements can legitimately run 10-20 min at SF=20k.
        "Connection Timeout=60;"
    )
    SQL_COPT_SS_ACCESS_TOKEN = 1256
    conn = pyodbc.connect(conn_str, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token},
                          autocommit=autocommit, timeout=3600)
    return conn


def fab_label(d) -> str:
    """Render a dict/str as an OPTION(LABEL=...) fragment for query attribution.

    Fabric DW surfaces the label in queryinsights.exec_requests_history.label,
    so fabric_metrics can attribute a statement back to a task/batch. Caller
    appends the returned string to the statement, e.g.
        cur.execute(sql + fab_label({"task":"setup_fabric","batch_date":bd}))
    """
    import json
    tag = d if isinstance(d, str) else json.dumps(d, separators=(",", ":"))
    tag = tag.replace("'", "''")[:200]   # LABEL is a string literal; keep it short
    return f" OPTION (LABEL = '{tag}')"
