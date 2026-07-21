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

import streamlit as st

from models import (
    Engine, SPECS, FieldKind,
    BATCH_TYPES, DBX_SKUS_BY_BATCH, SDP_EDITIONS,
    COMPETITIVE_ENGINES,
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
    "20000": "Incremental tables start at ~500 GB total; each daily batch adds ~1.75 GB of new raw data.",
}
# Single-batch / incremental: the full raw dataset is processed. Anchored on
# SF=10000 = ~1 TB of raw data.
_SF_CAPTIONS_RAW = {
    "10":    "~1 GB of raw data to process.",
    "100":   "~10 GB of raw data to process.",
    "1000":  "~100 GB of raw data to process.",
    "5000":  "~500 GB of raw data to process.",
    "10000": "~1 TB of raw data to process.",
    "20000": "~2 TB of raw data to process.",
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
    st.caption("Run type is Augmented Incremental and every engine runs dbt, "
               "so the comparison is apples-to-apples.")
    competitor_engines = [
        e for e in COMPETITIVE_ENGINES
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
scale_factor = _radio("**4. Scale factor?**", SF_OPTIONS, _sf_captions)
st.caption(
    ("Footprint is for the augmented-incremental workload (initial tables + "
     "per-batch new data)." if _is_augmented else
     "Footprint is the full raw dataset processed by a single-batch / "
     "incremental run.")
    + " TPC-DI data volume scales linearly with the scale factor.")
if not scale_factor:
    st.stop()

# --- Step 5: details (Databricks defaults + per-competitor prerequisites) ----
# --- Step 5: details -----------------------------------------------------
# A run is always the Databricks baseline plus any selected competitors.
# Native inputs default/derive; each competitor contributes its own
# prerequisite fields (secrets written to that engine's scope).
st.divider()
with st.form("details"):
    st.markdown("**5. Confirm run defaults** _(edit if needed)_")
    catalog = st.text_input("Target catalog", value="main")
    wh_db = st.text_input(
        "Target schema (wh_db)", value="",
        help="Blank = derive from your username, as the driver does.")
    # One batch count for the whole run — every engine (Databricks + each
    # competitor) uses the same count so the comparison is fair.
    batches = st.select_slider(
        "Batches to run (applies to every engine)",
        options=["30", "50", "100", "150", "365"], value="365",
        help="Lower it for a quick smoke run.")

    # Per-competitor prerequisite blocks. The batch count is NOT collected
    # here — it comes from the single control above.
    comp_values: dict[Engine, dict] = {}
    for eng in competitor_engines:
        cspec = SPECS[eng]
        st.markdown(f"**{cspec.label} details**")
        st.caption("🔑 fields are written to that engine's secret scope, never stored.")
        cv: dict[str, str] = {"scale_factor": scale_factor,
                              "incremental_batches_to_run": batches}
        for f in cspec.fields:
            if f.key in ("scale_factor", "incremental_batches_to_run"):
                continue
            wkey = f"{eng.value}.{f.key}"
            if f.kind is FieldKind.SECRET:
                cv[f.key] = st.text_input(f"{f.label} 🔑", type="password",
                                          help=f.help or None, key=wkey)
            elif f.kind is FieldKind.DERIVED:
                cv[f.key] = st.text_input(f"{f.label} (blank = derive)",
                                          help=f.help or None, key=wkey)
            else:
                cv[f.key] = st.text_input(f.label, value=f.default,
                                          help=f.help or None, key=wkey)
        comp_values[eng] = cv

    submitted = st.form_submit_button("Review & create")

if not submitted:
    st.stop()

# --- Validate all competitor blocks before creating anything -----------------
errors = []
for eng, cv in comp_values.items():
    cspec = SPECS[eng]
    for f in cspec.fields:
        if f.required and f.key != "scale_factor" and not cv.get(f.key):
            errors.append(f"{cspec.label}: {f.label}")
if errors:
    st.error("Missing required fields — " + "; ".join(errors))
    st.stop()

results = []

# 1. Databricks baseline (native — always runs).
results.append({
    "engine": "databricks", "sku": sku, "edition": edition,
    "scale_factor": scale_factor, "catalog": catalog,
    "wh_db": wh_db or "(derived)", "batches": batches, "mock": USE_MOCK,
})

# 2. Each competitor: write its secrets, then emit its workflow.
for eng, cv in comp_values.items():
    cspec = SPECS[eng]
    for f in cspec.secret_fields():
        if cv.get(f.key):
            backend.write_secret(f.secret_scope, f.secret_key, cv[f.key])
    try:
        results.append(backend.create_workflow(cspec, cv))
    except NotImplementedError as e:
        results.append({"engine": eng.value, "error": f"not wired yet: {e}"})

st.success(f"Created {len(results)} run(s).")
st.json(results)
