# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
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
# Prerequisites in Fabric (one-time, per warehouse):
#   - The SP is a member (Contributor or higher) of the Fabric workspace.
#     Workspace roles map onto warehouse roles, so no CREATE USER is needed;
#     if connects are still rejected, run once as a warehouse admin:
#       CREATE USER [<sp display name>] FROM EXTERNAL PROVIDER;
#       ALTER ROLE db_owner ADD MEMBER [<sp display name>];
#   - Tokens are minted for the Azure SQL audience
#     "https://database.windows.net/.default", which Fabric DW accepts.
#
# Secret contract (mirrors _rs_conn): only the SP client secret is a genuine
# secret. It arrives as a FULL UC secret path (`catalog.schema.key`, e.g.
# "main.tpcdi_raw_data.fabric_<client_id>_sp_secret"). Tenant id, client id,
# host and database are plain values passed in by the caller.
#
# Usage from a calling notebook:
#   %run ./_fab_conn
#   conn = fab_connect(host=..., database=..., tenant_id=..., client_id=...,
#                      client_secret_secret=..., label={...})
#   with conn.cursor() as cur: cur.execute("...")
#   fab_onelake_conf(spark, tenant_id=..., client_id=...,
#                    client_secret_secret=...)   # before abfss:// OneLake I/O

import struct


def _install(pkgs):
    import importlib, subprocess, sys
    for mod, pip_name in pkgs:
        try:
            importlib.import_module(mod)
        except ImportError:
            subprocess.check_call([sys.executable, "-m", "pip", "install", "--quiet", pip_name])


def _secret_from_path(path):
    """Resolve a full UC secret path "catalog.schema.key" to its value.

    maxsplit=2 so a key containing dots still resolves — the first two dots
    delimit catalog + schema, everything after is the key.
    """
    catalog, schema, key = path.split(".", 2)
    return dbutils.secrets.get(catalog=catalog, schema=schema, key=key)  # noqa: F821


_ONELAKE = "onelake.dfs.fabric.microsoft.com"


def fab_onelake_conf(spark, *, tenant_id: str, client_id: str,
                     client_secret_secret: str) -> None:
    """Point the session's ABFS driver at OneLake as the SP.

    Sets both spark.conf (Spark reads/writes) and the JVM Hadoop conf
    (dbutils.fs.* uses that one). Needs classic compute: serverless doesn't
    allow setting fs.azure.* confs.
    """
    conf = {
        f"fs.azure.account.auth.type.{_ONELAKE}": "OAuth",
        f"fs.azure.account.oauth.provider.type.{_ONELAKE}":
            "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        f"fs.azure.account.oauth2.client.id.{_ONELAKE}": client_id,
        f"fs.azure.account.oauth2.client.secret.{_ONELAKE}":
            _secret_from_path(client_secret_secret),
        f"fs.azure.account.oauth2.client.endpoint.{_ONELAKE}":
            f"https://login.microsoftonline.com/{tenant_id}/oauth2/token",
    }
    hconf = spark._jsc.hadoopConfiguration()
    for k, v in conf.items():
        spark.conf.set(k, v)
        hconf.set(k, v)


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


def fab_connect(*, host: str, database: str, tenant_id: str, client_id: str,
                client_secret_secret: str,
                token_resource: str = "https://database.windows.net/.default",
                label: str | dict | None = None,
                autocommit: bool = True):
    """Open a Fabric DW connection as the service principal.

    host                 — the warehouse SQL analytics endpoint
                           (e.g. <...>.datawarehouse.fabric.microsoft.com)
    database             — the warehouse item name (e.g. tpcdi_fabric_dw)
    tenant_id, client_id — Entra tenant + SP application (client) id
    client_secret_secret — full UC secret path of the SP client secret

    `label` is attached to statements via `OPTION (LABEL='...')` by callers that
    want per-task attribution in queryinsights.exec_requests_history (Fabric DW
    has no Redshift-style session query_group). Returned here for the caller to
    stamp onto its SQL; the connection itself doesn't set a session tag.
    """
    _install([("pyodbc", "pyodbc")])
    import pyodbc

    csec = _secret_from_path(client_secret_secret) if client_secret_secret else ""
    if not (host and database and tenant_id and client_id and csec):
        raise RuntimeError("fab_connect missing host/database/tenant_id/client_id "
                           f"or the SP client secret ({client_secret_secret!r})")

    token = _sp_access_token(tenant_id=tenant_id, client_id=client_id,
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
