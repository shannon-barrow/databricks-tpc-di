"""Builder for the SQL Warehouse JSON.

Replaces `jinja_templates/warehouse_jinja_template.json`. Submitted to
`/api/2.0/sql/warehouses` (POST creates a serverless PRO warehouse).
"""
from __future__ import annotations


def build(*, wh_name: str, wh_size: str, **_unused) -> dict:
    return {
        "name": wh_name,
        "cluster_size": wh_size,
        "min_num_clusters": 1,
        "max_num_clusters": 1,
        "auto_stop_mins": 1,
        "enable_serverless_compute": True,
        "warehouse_type": "PRO",
        "channel": {"name": "CHANNEL_NAME_PREVIEW"},
    }
