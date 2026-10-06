#!/bin/bash
# Cluster init script — install the Microsoft ODBC Driver 18 (msodbcsql18) so
# pyodbc / dbt-fabric can talk TDS to the Fabric DW SQL endpoint.
# Classic DBR images don't bundle msodbcsql18, and serverless can't apt-install
# a system driver at all, which is why the Fabric DW tasks run on a classic job
# cluster (see workflow_builders/augmented_fabric.py).
#
# The Microsoft apt repo is per Ubuntu release, so the release + codename come
# from /etc/os-release instead of being pinned (DBR 15.x = 22.04 jammy,
# 17.x+ = 24.04 noble).
# Idempotent: apt is a no-op if the driver is already present.
set -euo pipefail

export DEBIAN_FRONTEND=noninteractive
export ACCEPT_EULA=Y

. /etc/os-release   # VERSION_ID (e.g. 24.04), VERSION_CODENAME (e.g. noble)

curl -sSL https://packages.microsoft.com/keys/microsoft.asc \
  | gpg --dearmor --yes -o /usr/share/keyrings/microsoft-prod.gpg
echo "deb [arch=amd64,arm64 signed-by=/usr/share/keyrings/microsoft-prod.gpg] \
https://packages.microsoft.com/ubuntu/${VERSION_ID}/prod ${VERSION_CODENAME} main" \
  > /etc/apt/sources.list.d/mssql-release.list

apt-get update -y
apt-get install -y --no-install-recommends msodbcsql18 unixodbc-dev

echo "[init] msodbcsql18 installed on Ubuntu ${VERSION_ID} (${VERSION_CODENAME}):"
odbcinst -q -d -n "ODBC Driver 18 for SQL Server" || true
