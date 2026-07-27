"""Unity Catalog secret helpers — naming + validation.

Competitor benchmarks authenticate to an external engine with credentials the
operator stores as **Unity Catalog secrets**. Secrets are named for *what they
unlock* (the target login/account), not for the person running — so a secret
is created once per competitor deployment and reused by anyone on the team,
just like the generated benchmark data. The app/notebook never creates a
secret; it only validates it and tells the user what to do.

A UC secret is a three-part securable ``catalog.schema.name``, read at run time
via ``dbutils.secrets.get(catalog=, schema=, key=name)`` (needs DBR 17.3 LTS+
or serverless env v4+).
"""
from __future__ import annotations

import json
import re
from typing import Callable, Optional


def default_secret_name(engine: str, target: str, kind: str = "pw") -> str:
    """Default secret name, encoding what it unlocks. UC lowercases secret
    names, so we normalize here for a stable, matchable default.

        redshift  + user 'admin'      -> redshift_admin_pw_secret
        snowflake + user 'TPCDI_SVC'  -> snowflake_tpcdi_svc_pw_secret
        snowflake + user 'TPCDI_SVC', kind='dbx_pat' -> snowflake_tpcdi_svc_dbx_pat_secret
        bigquery  + project 'my-proj' -> bigquery_my_proj_sa_json_secret

    `target` is the login/account/project the credential authenticates to.
    """
    t = re.sub(r"\W+", "_", (target or "").strip().lower()).strip("_") or "default"
    return f"{engine.lower()}_{t}_{kind}_secret"


def check_uc_secret(path: str, api_call: Callable) -> dict:
    """Validate a UC secret at ``catalog.schema.name`` for the current user.

    ``api_call(body, method, endpoint)`` must be a caller that returns the
    response object for ANY status code — pass ``tpcdi_config.api_call_raw``,
    NOT ``tpcdi_config.api_call`` (the latter calls ``dbutils.notebook.exit`` on
    non-200, which would abort the notebook on the 404/403 this function is
    specifically designed to detect and report).

    Returns a dict describing one of four states — the caller decides whether
    to warn or block:
      {"state": "ok",        "detail": ...}                  exists + readable
      {"state": "no_access", "owner": ..., "detail": ...}    exists, no READ
      {"state": "missing",   "detail": ...}                  not created yet
      {"state": "malformed", "detail": ...}                  not catalog.schema.name
    """
    parts = (path or "").split(".")
    if len(parts) < 3:
        return {"state": "malformed",
                "detail": f"'{path}' is not a catalog.schema.name secret path"}

    # Metadata (existence + owner) — no value.
    meta_resp = api_call(None, "GET", f"/api/2.1/unity-catalog/secrets/{path}")
    if getattr(meta_resp, "status_code", None) != 200:
        return {"state": "missing",
                "detail": f"secret '{path}' does not exist in Unity Catalog yet"}
    meta = json.loads(meta_resp.text)
    owner = meta.get("effective_owner") or meta.get("created_by") or "(unknown)"

    # Access test: reading the value requires READ SECRET. If we can't, the
    # secret exists but this user lacks access.
    val_resp = api_call(None, "GET",
                        f"/api/2.1/unity-catalog/secrets/{path}?include_value=true")
    if getattr(val_resp, "status_code", None) == 200 and \
            "effective_value" in json.loads(val_resp.text):
        return {"state": "ok", "owner": owner,
                "detail": f"secret '{path}' exists and is readable"}
    return {"state": "no_access", "owner": owner,
            "detail": (f"secret '{path}' exists but you don't have READ access — "
                       f"request access from its owner: {owner}")}
