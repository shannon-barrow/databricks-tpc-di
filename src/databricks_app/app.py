"""TPC-DI Guided Benchmark Builder (Streamlit).

A progressive, guided wizard for setting up TPC-DI benchmark runs. Each answer
reveals the next question; the valid choices at every step come from the same
decision tree the driver notebook uses (models.py, mirroring _WORKFLOWS).

The flagship use case is competitive (non-Databricks) runs, which need many
inputs a native run can just default. The wizard collects those, shows the
defaults for review, writes any secrets to the target scope, and emits the
workflow via each port's create_jobs.create().
"""
import base64
from pathlib import Path

import os
import re

import streamlit as st

from models import (
    Engine, SPECS, FieldKind, derived_from_email,
    competitors_for_cloud, REGION_DEFAULT,
    dbt_wh_size, WH_SIZES, cluster_plan, cluster_plan_summary,
    SNOWFLAKE_SUMMARY,
    BATCH_TYPES, DBX_SKUS_BY_BATCH, SDP_EDITIONS,
)
from backend import backend, USE_MOCK

st.set_page_config(page_title="TPC-DI Guided Builder", layout="centered",
                   page_icon="📊")

# Databricks logomark, base64-embedded from the bundled PNG asset. Inlining it
# as a data URI means it ships in the HTML and renders in the deployed app with
# no network fetch (Databricks Apps can't reliably hotlink external images).
_LOGO_PATH = Path(__file__).with_name("assets") / "databricks_logomark.png"
_LOGO_B64 = base64.b64encode(_LOGO_PATH.read_bytes()).decode()
_DB_LOGO = (
    f'<img src="data:image/png;base64,{_LOGO_B64}" alt="Databricks" '
    f'height="40" />'
)

# Databricks-flavored styling: lava-red brand accents, a header band, and
# tighter step framing. Kept in one CSS block so the theme config
# (.streamlit/config.toml) owns colors and this owns layout polish.
st.markdown(
    """
    <style>
      /* brand header band */
      .db-header {
        display: flex; align-items: center; gap: 1rem;
        background: linear-gradient(90deg, #1B3139 0%, #2D4550 100%);
        border-left: 6px solid #FF3621;
        padding: 1.1rem 1.4rem; border-radius: 8px; margin-bottom: 1.25rem;
      }
      .db-header img { flex: 0 0 auto; }
      .db-header .db-title { display: flex; flex-direction: column; }
      .db-header h1 { color: #FFFFFF; font-size: 1.6rem; margin: 0; }
      .db-header p  { color: #C7D0D4; margin: 0.25rem 0 0; font-size: 0.9rem; }
      /* radio selected dot + primary buttons already use primaryColor */
      div[data-testid="stForm"] {
        border: 1px solid #E3E3E3; border-radius: 8px; padding: 1rem 1.2rem;
      }
      .stButton>button[kind="primary"] { font-weight: 600; }
    </style>
    """,
    unsafe_allow_html=True,
)

st.markdown(
    f"""
    <div class="db-header">
      {_DB_LOGO}
      <div class="db-title">
        <h1>TPC-DI Benchmark Builder</h1>
        <p>Guided setup for Databricks &amp; competitive TPC-DI benchmarks.</p>
      </div>
    </div>
    """,
    unsafe_allow_html=True,
)

if USE_MOCK:
    st.info("Mock mode — nothing is written or created. "
            "Set USE_MOCK_BACKEND=false to emit real workflows.", icon="🧪")


def _infer_cloud_region() -> tuple[str, str]:
    """Infer (cloud, region) from the environment — never ask the user. A
    Databricks App runs in the same cloud/region as its workspace, and the
    workspace host encodes both:
      - AWS:   dbc-….cloud.databricks.com  / benchmarking-prod-aws-us-west-2.…
      - Azure: adb-….azuredatabricks.net
      - GCP:   ….gcp.databricks.com
    Region is parsed from the host where present (AWS names often embed it),
    else falls back to a per-cloud default.
    """
    host = (os.getenv("DATABRICKS_HOST")
            or os.getenv("DATABRICKS_WORKSPACE_URL") or "").lower()
    if ".azuredatabricks.net" in host or "azure" in host:
        cloud = "Azure"
    elif ".gcp.databricks.com" in host or "gcp" in host:
        cloud = "GCP"
    else:
        cloud = "AWS"   # default cloud for *.cloud.databricks.com
    # Try to pull a region token out of the host (e.g. "...-aws-us-west-2...").
    region = ""
    m = re.search(r"(us|eu|ap|ca|sa|af|me)-[a-z]+-\d", host)
    if m:
        region = m.group(0)
    return cloud, (region or REGION_DEFAULT.get(cloud, ""))


APP_CLOUD, APP_REGION = _infer_cloud_region()

SF_OPTIONS = ["10", "100", "1000", "5000", "10000", "20000"]

# Data footprint per scale factor. TPC-DI data volume is linear in scale
# factor, so both maps are anchored on one measured point and scaled linearly.
#
# Augmented incremental: measured SF=20000 = ~500 GB initial incremental tables
# + ~1.75 GB new raw data per daily batch.
_SF_CAPTIONS_AUGMENTED = {
    "10":    "Incremental tables start at ~250 MB total; each daily batch adds ~0.9 MB of new raw data.",
    "100":   "Incremental tables start at ~2.5 GB total; each daily batch adds ~9 MB of new raw data.",
    "1000":  "Incremental tables start at ~25 GB total; each daily batch adds ~90 MB of new raw data.",
    "5000":  "Incremental tables start at ~125 GB total; each daily batch adds ~440 MB of new raw data.",
    "10000": "Incremental tables start at ~250 GB total; each daily batch adds ~875 MB of new raw data.",
    "20000": '"Recommended" — Incremental tables start at ~500 GB total; each daily batch adds ~1.75 GB of new raw data.',
}
# Single-batch / incremental: the full raw dataset is processed. Anchored on
# SF=10000 = ~1 TB of raw data.
_SF_CAPTIONS_RAW = {
    "10":    "~1 GB of raw data to process.",
    "100":   "~10 GB of raw data to process.",
    "1000":  "~100 GB of raw data to process.",
    "5000":  "~500 GB of raw data to process.",
    "10000": "~1 TB of raw data to process.",
    "20000": '"Recommended" — ~2 TB of raw data to process.',
}

# Context lines shown under each choice (st.radio captions).
_BATCH_CAPTIONS = {
    "Single Batch": "Load all history in one pass.",
    "Incremental": "Per-day CDC batches after an initial load.",
    "Augmented Incremental": "The daily-streaming benchmark. Required for competitor comparisons.",
}
_SKU_CAPTIONS = {
    "Cluster": "Jobs on a classic/serverless cluster.",
    "DBSQL": "SQL warehouse (dbt).",
    "SDP": "Spark Declarative Pipelines (Lakeflow).",
    "dbt": "dbt-databricks against a SQL warehouse.",
}


def _radio(label, options, captions_map, help=None):
    """Radio with a per-option context caption, no default selection."""
    return st.radio(
        label, options, index=None, help=help,
        captions=[captions_map.get(o, "") for o in options],
    )


# --- Step 1: scope — Databricks only, or Databricks + competitors ------------
# Asked first because it determines the rest: a competitive comparison fixes
# the run to Augmented Incremental + dbt (apples-to-apples), so those questions
# are skipped. A Databricks-only run gets the full batch-type / SKU choice.
scope = _radio(
    "**1. What do you want to run?**",
    ["Databricks only", "Databricks and Competitors"],
    {
        "Databricks only": "Just the Databricks benchmark — pick any run type and SKU.",
        "Databricks and Competitors":
            "Databricks vs one or more competitors, executed via dbt to "
            "standardize execution (Augmented Incremental only).",
    },
)
if not scope:
    st.stop()

competitor_engines: list[Engine] = []
edition = "CORE"

if scope == "Databricks and Competitors":
    # Competitive path: batch type + SKU are fixed; just pick competitors.
    batch_type = "Augmented Incremental"
    sku = "dbt"
    st.markdown("**2. Which competitors?** _(pick one or more)_")
    # Only competitors valid in the cloud this app runs in — the data is
    # generated in Databricks and must be read in the same cloud/region.
    # This also means BigQuery (GCP) and Redshift (AWS) are never both offered.
    _cloud_competitors = competitors_for_cloud(APP_CLOUD)
    st.caption(f"Running on **{APP_CLOUD}** ({APP_REGION}). Run type is "
               "Augmented Incremental and every engine runs dbt, so the "
               "comparison is apples-to-apples. Only same-cloud competitors "
               "are shown — the data can't be read cross-cloud without egress.")
    competitor_engines = [
        e for e in _cloud_competitors
        if st.checkbox(SPECS[e].label, key=f"comp_{e.value}")
    ]
    if not competitor_engines:
        st.info("Select at least one competitor to continue.")
        st.stop()
else:
    # Databricks-only path: full run-type + SKU choice.
    batch_type = _radio("**2. What kind of run?**", BATCH_TYPES, _BATCH_CAPTIONS)
    if not batch_type:
        st.stop()
    st.divider()
    sku = _radio("**3. Which Databricks SKU?**", DBX_SKUS_BY_BATCH[batch_type],
                 _SKU_CAPTIONS, help="Filtered to what's valid for this batch type.")
    if not sku:
        st.stop()
    if sku == "SDP" and batch_type == "Single Batch":
        edition = _radio("**3a. SDP edition?**", SDP_EDITIONS,
                         {e: "" for e in SDP_EDITIONS})
        if not edition:
            st.stop()

# --- Step 4: scale factor ----------------------------------------------------
# Vertical (captioned) so each SF can show its data-footprint sentence. The
# footprint differs by run type: augmented incremental starts from a historical
# table set + streams small daily batches, whereas single-batch / incremental
# processes the full raw dataset.
_is_augmented = batch_type == "Augmented Incremental"
_sf_captions = _SF_CAPTIONS_AUGMENTED if _is_augmented else _SF_CAPTIONS_RAW

if _is_augmented:
    # Push SF=20000 (the published benchmark anchor): surface only it + "Other"
    # first, and reveal the smaller factors only if the user opts out.
    _RECOMMENDED_SF = "20000"
    choice = _radio(
        "**4. Scale factor?**",
        [_RECOMMENDED_SF, "Other…"],
        {_RECOMMENDED_SF: _SF_CAPTIONS_AUGMENTED[_RECOMMENDED_SF],
         "Other…": "Pick a smaller scale factor (quicker/cheaper, but not the "
                   "published benchmark size)."},
    )
    if not choice:
        st.stop()
    if choice == _RECOMMENDED_SF:
        scale_factor = _RECOMMENDED_SF
    else:
        _others = [sf for sf in SF_OPTIONS if sf != _RECOMMENDED_SF]
        scale_factor = _radio("**4a. Which scale factor?**", _others,
                              _SF_CAPTIONS_AUGMENTED)
        if not scale_factor:
            st.stop()
    st.caption("Footprint is for the augmented-incremental workload (initial "
               "tables + per-batch new data). TPC-DI data volume scales "
               "linearly with the scale factor.")
else:
    scale_factor = _radio("**4. Scale factor?**", SF_OPTIONS, _sf_captions)
    st.caption("Footprint is the full raw dataset processed by a single-batch / "
               "incremental run. TPC-DI data volume scales linearly with the "
               "scale factor.")
    if not scale_factor:
        st.stop()

# --- Step 5: details (Databricks defaults + per-competitor prerequisites) ----
# --- Step 5: details -----------------------------------------------------
# A run is always the Databricks baseline plus any selected competitors.
# Native inputs default/derive; each competitor contributes its own
# prerequisite fields (secrets written to that engine's scope).
st.divider()


# Workspace-derived ghost-fill values (username → wh_db prefix, etc.), computed
# the same way the driver notebook does. Shown as placeholders on DERIVED
# fields; leaving the field blank lets the job derive the same value at run
# time, so the placeholder is a preview, not a committed value.
#
# Source the *viewing* user, not the app: a deployed Databricks App runs as its
# service principal, so current_user() would be the app identity. Databricks
# forwards the end user's email in request headers (X-Forwarded-Email), which
# Streamlit exposes via st.context.headers. Fall back to the backend only when
# no header is present (local / run-local).
def _viewer_email() -> str:
    try:
        h = st.context.headers or {}
    except Exception:
        return ""
    for k in ("X-Forwarded-Email", "x-forwarded-email",
              "X-Forwarded-Preferred-Username", "x-forwarded-preferred-username"):
        v = h.get(k)
        if v:
            return v
    return ""


_email = _viewer_email()
_derived = derived_from_email(_email) if _email else backend.derived_defaults()


def _show_check(result: dict) -> None:
    """Render an existence-check result as a status line."""
    exists, detail = result.get("exists"), result.get("detail", "")
    if exists is True:
        st.success(detail, icon="✅")
    elif exists is False:
        st.warning(detail, icon="⚠️")
    else:
        st.caption(detail)   # None → unknown (mock / not checked)


def _render_field(eng_value: str, f) -> str:
    """Render one competitor InputField as a form control and return its value.

    Competitors only carry PARAM, SECRET_PATH, and REGION fields now — the UC
    catalog and target-schema prefix are shared top-level fields.
    """
    wkey = f"{eng_value}.{f.key}"
    if f.kind is FieldKind.SECRET_PATH:
        # A UC secret reference — the operator pastes catalog.schema.key. Not a
        # password box: the value is a path, not the secret itself. The UC
        # secret lives in Databricks, so we can check existence inline via the
        # SDK (no egress to the competitor engine).
        val = st.text_input(f"{f.label} 🔑", value=f.default,
                            placeholder=f.placeholder or None,
                            help=f.help or None, key=wkey)
        if val and len(val.split(".")) >= 3:   # looks like catalog.schema.key
            _show_check(backend.check_secret(val))
        return val
    if f.kind is FieldKind.REGION:
        # Inferred from where the app runs; not editable (same-cloud/region
        # constraint). disabled so the value is visible but locked.
        st.text_input(f"{f.label} (fixed — same region as the Databricks run)",
                      value=APP_REGION, disabled=True,
                      help=f.help or None, key=wkey)
        return APP_REGION
    val = st.text_input(f.label, value=f.default,
                        placeholder=f.placeholder or None,
                        help=f.help or None, key=wkey)
    # S3/GCS staging prefix must be covered by a UC external location — a
    # Databricks-side check, so do it inline too.
    if f.key in ("s3_volume_prefix", "gcs_volume_prefix") and val.startswith(("s3://", "gs://")):
        _show_check(backend.check_external_location_for(val))
    return val


_WH_DB_HINT = _derived.get("wh_db", "{fname}_{lname}_TPCDI")
_JOB_HINT = _derived.get("job_name_prefix", "{fname}-{lname}-TPCDI")
_sf_int = int(scale_factor)


# --- Live validation section (outside st.form so buttons + inline checks work).
# Databricks-side objects the app can reach via the SDK: check existence as the
# user types, and offer to create the 3 we're allowed to (catalog, raw schema,
# dbt warehouse). Secrets + external locations are check-and-tell only.
st.divider()
st.markdown("**5. Confirm run defaults** _(edit if needed)_")

catalog = st.text_input("UC catalog", value="main",
                        help="Unity Catalog catalog for the run's tables + the "
                             "external volume.")
_cat = backend.check_catalog(catalog)
_show_check(_cat)
if _cat.get("exists") is False and catalog:
    if st.button(f"Create catalog `{catalog}`", key="mk_cat"):
        _show_check_res = backend.create_catalog(catalog)
        st.info(_show_check_res["detail"])
        st.rerun()

wh_db = st.text_input(
    f"Target schema prefix (blank will derive as {_WH_DB_HINT})",
    placeholder=_derived.get("wh_db") or None,
    help="The run appends the scale factor → {prefix}_{sf}.")
_wh_db_eff = wh_db or _derived.get("wh_db", "")
if _wh_db_eff:
    st.caption(f"→ schema: `{_wh_db_eff}_{scale_factor}`")

raw_schema = st.text_input("Raw data schema", value="tpcdi_raw_data",
                           help="Schema holding the generated raw data + UC "
                                "volume, as in the driver notebook.")
_rs = backend.check_schema(catalog, raw_schema)
_show_check(_rs)
if _rs.get("exists") is False and catalog and raw_schema:
    if st.button(f"Create schema `{catalog}.{raw_schema}`", key="mk_raw"):
        r = backend.create_schema(catalog, raw_schema)
        st.info(r["detail"])
        st.rerun()

# --- Databricks details (SKU-specific compute) — outside the form so the dbt
# warehouse can be checked + created inline.
st.markdown("**Databricks details**")
dbx_wh_name = dbx_wh_size = None
if sku == "dbt":
    # dbt runs on a DBSQL warehouse — size + name. Size is pre-set by the scale
    # factor (driver mapping) but editable; the name defaults to the driver's
    # non-augmented DBSQL convention: TPCDI_{size} (no username).
    _size_default = dbt_wh_size(_sf_int)
    dbx_wh_size = st.selectbox(
        "Warehouse size", WH_SIZES, index=WH_SIZES.index(_size_default),
        help=f"Pre-set from the scale factor ({_size_default} for "
             f"SF={scale_factor}); change if you need to.")
    dbx_wh_name = st.text_input(
        "Databricks SQL warehouse name", value=f"TPCDI_{dbx_wh_size}",
        help="Defaults to the driver's TPCDI_{size} naming.")
    _wh = backend.check_warehouse(dbx_wh_name)
    _show_check(_wh)
    if _wh.get("exists") is False and dbx_wh_name:
        if st.button(f"Create warehouse `{dbx_wh_name}` ({dbx_wh_size})",
                     key="mk_wh"):
            r = backend.create_warehouse(dbx_wh_name, dbx_wh_size)
            st.info(r["detail"])
            st.rerun()
elif sku in ("Cluster", "SDP"):
    # Cluster / SDP: we pick the compute (not editable here — tune the generated
    # job if needed). Sized from measured tuning: worker cores scale linearly
    # with SF, single-node at <=32 cores, else 8-core workers + a 4-core driver,
    # on the ARM node family for this cloud.
    _plan = cluster_plan(APP_CLOUD, _sf_int, _is_augmented)
    st.text_input("Compute (auto-configured)",
                  value=cluster_plan_summary(_plan), disabled=True,
                  help="Latest DBR. Sized from SKU, scale factor, and cloud "
                       f"({APP_CLOUD}).")
    st.caption(
        "We size the cluster for you from our tuning; you can change the "
        "cluster config on the generated job afterward.")

# --- Batches, job name, competitor details — OUTSIDE st.form so the inline
# Databricks-side checks (secret existence, external location) rerun live as the
# user types. (A form batches inputs and won't rerun per-field.)
batches = st.select_slider(
    "Batches to run (applies to every engine)",
    options=["30", "50", "100", "150", "365"], value="150",
    help="Lower it for a quick smoke run.")
job_name_prefix = st.text_input(
    f"Job name prefix (blank will derive as {_JOB_HINT})",
    placeholder=_derived.get("job_name_prefix") or None,
    help="Suffixes (scale factor, SKU, competitor) are appended "
         "automatically, matching the driver's naming.")

# --- Per-competitor blocks ---------------------------------------------------
comp_values: dict[Engine, dict] = {}
for eng in competitor_engines:
    cspec = SPECS[eng]
    st.markdown(f"**{cspec.label} details**")
    if eng is Engine.SNOWFLAKE:
        st.info(SNOWFLAKE_SUMMARY, icon="❄️")
    cv: dict[str, str] = {"scale_factor": scale_factor,
                          "incremental_batches_to_run": batches,
                          "catalog": catalog, "wh_db": _wh_db_eff}
    for f in cspec.fields:
        # scale_factor is chosen above; catalog/wh_db are shared top-level for
        # engines that consume the UC catalog. Snowflake/BigQuery keep their own
        # engine-specific `catalog` field (SF database / BQ project), so don't
        # skip catalog for those.
        if f.key in ("scale_factor", "wh_db"):
            continue
        cv[f.key] = _render_field(eng.value, f)
    comp_values[eng] = cv

# One footnote for the whole competitor section (not per-engine).
if competitor_engines:
    st.caption(
        "🔑 = a Unity Catalog secret path (catalog.schema.key) to a "
        "secret you already created; the app passes the path and the "
        "job reads the value at run time. Only passwords/keys/tokens "
        "are secrets — other fields are plain config. "
        "Requires Unity Catalog Secrets — for more information see "
        "https://docs.databricks.com/aws/en/security/secrets/unity-catalog-secrets")

# Submit lives in a minimal form so all the above is collected on one click.
with st.form("details"):
    submitted = st.form_submit_button("Review & create")

if not submitted:
    st.stop()

# --- Validate all competitor blocks before creating anything -----------------
# Secret + external-location existence are already surfaced inline (above) as
# each field is filled; here we only block on genuinely missing required fields.
errors = []
for eng, cv in comp_values.items():
    cspec = SPECS[eng]
    for f in cspec.fields:
        if (f.required and f.key not in ("scale_factor", "wh_db")
                and not cv.get(f.key)):
            errors.append(f"{cspec.label}: {f.label}")
if errors:
    st.error("Missing required fields — " + "; ".join(errors))
    st.stop()

# Job-name suffixes, matching the driver + a new competitor suffix (the driver
# doesn't generate competitor jobs today). Parent name per engine:
#   {prefix}-SF{sf}-AugmentedIncremental-{variant}-Parent
_eff_job_prefix = job_name_prefix or _derived.get("job_name_prefix") or "TPCDI"
_AUG_VARIANT = {"dbt": "DBT", "SDP": "SDP", "Cluster": "Cluster"}
_dbx_variant = _AUG_VARIANT.get(sku, sku)


def _job_name(variant: str) -> str:
    return f"{_eff_job_prefix}-SF{scale_factor}-AugmentedIncremental-{variant}-Parent"


results = []

# 1. Databricks baseline (native — always runs).
results.append({
    "engine": "databricks", "sku": sku, "edition": edition,
    "scale_factor": scale_factor, "catalog": catalog,
    "wh_db": _wh_db_eff or "(derived)", "raw_schema": raw_schema,
    "job_name": _job_name(_dbx_variant),
    **({"wh_name": dbx_wh_name, "wh_size": dbx_wh_size} if sku == "dbt" else {}),
    "batches": batches, "mock": USE_MOCK,
})

# 2. Each competitor: emit its workflow. No secret values are handled by the
# app — the secret-path fields in cv (catalog.schema.key) point the job at the
# operator's existing UC secrets, which it reads at run time. Competitor jobs
# get a per-engine variant suffix (SF/Redshift/BigQuery/Snowflake).
_COMP_VARIANT = {Engine.REDSHIFT: "Redshift", Engine.BIGQUERY: "BigQuery",
                 Engine.SNOWFLAKE: "Snowflake"}
for eng, cv in comp_values.items():
    cspec = SPECS[eng]
    cv["job_name"] = _job_name(_COMP_VARIANT[eng])
    try:
        results.append(backend.create_workflow(cspec, cv))
    except NotImplementedError as e:
        results.append({"engine": eng.value, "error": f"not wired yet: {e}"})

st.success(f"Created {len(results)} run(s).")
st.json(results)
