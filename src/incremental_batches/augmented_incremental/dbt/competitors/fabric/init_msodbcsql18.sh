#!/bin/bash
# Cluster init script — install the Microsoft ODBC Driver 18 (msodbcsql18) so
# pyodbc / dbt-fabric can talk TDS to the Fabric DW SQL endpoint. Required
# because Databricks classic DBR images don't reliably bundle msodbcsql18, and
# serverless can't apt-install a system driver at all (hence dbt-fabric must run
# on a CLASSIC cluster with this init script — see augmented_fabric.py / PORT_NOTES).
#
# Idempotent: apt is a no-op if the driver is already present.
# Ubuntu-based DBR. If the DBR's Ubuntu release changes, update the repo URL
# codename (22.04 shown). ODBC 18 covers SQL Server / Azure SQL / Fabric DW.
set -euo pipefail

export DEBIAN_FRONTEND=noninteractive
export ACCEPT_EULA=Y

# Microsoft apt repo (Ubuntu 22.04). Key + list.
curl -sSL https://packages.microsoft.com/keys/microsoft.asc \
  | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg
echo "deb [arch=amd64,arm64 signed-by=/usr/share/keyrings/microsoft-prod.gpg] \
https://packages.microsoft.com/ubuntu/22.04/prod jammy main" \
  > /etc/apt/sources.list.d/mssql-release.list

apt-get update -y
apt-get install -y --no-install-recommends msodbcsql18 unixodbc-dev

echo "[init] msodbcsql18 installed:"
odbcinst -q -d -n "ODBC Driver 18 for SQL Server" || true
