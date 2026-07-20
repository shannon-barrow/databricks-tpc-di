"""TPC-DI Guided Benchmark Builder (Streamlit).

A progressive, guided wizard for setting up TPC-DI benchmark runs. Each answer
reveals the next question; the valid choices at every step come from the same
decision tree the driver notebook uses (models.py, mirroring _WORKFLOWS).

The flagship use case is competitive (non-Databricks) runs, which need many
inputs a native run can just default. The wizard collects those, shows the
defaults for review, writes any secrets to the target scope, and emits the
workflow via each port's create_jobs.create().
"""
import streamlit as st

from models import (
    Engine, SPECS, FieldKind,
    BATCH_TYPES, DBX_SKUS_BY_BATCH, SDP_EDITIONS,
    COMPETITIVE_ENGINES,
)
from backend import backend, USE_MOCK

st.set_page_config(page_title="TPC-DI Guided Builder", layout="centered")

st.title("TPC-DI Guided Benchmark Builder")
st.caption("Answer each step; the next one appears based on your choice.")
if USE_MOCK:
    st.info("Mock mode — nothing is written or created. "
            "Set USE_MOCK_BACKEND=false to emit real workflows.", icon="🧪")

SF_OPTIONS = ["10", "100", "1000", "5000", "10000", "20000"]

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
scale_factor = st.radio("**4. Scale factor?**", SF_OPTIONS, index=None,
                        horizontal=True)
if not scale_factor:
    st.stop()

# --- Step 5: details (Databricks defaults + per-competitor prerequisites) ----
# --- Step 5: details -----------------------------------------------------
# A run is always the Databricks baseline plus any selected competitors.
# Native inputs default/derive; each competitor contributes its own
# prerequisite fields (secrets written to that engine's scope).
st.divider()
with st.form("details"):
    st.markdown("**5. Confirm Databricks defaults** _(edit if needed)_")
    catalog = st.text_input("Target catalog", value="main")
    wh_db = st.text_input(
        "Target schema (wh_db)", value="",
        help="Blank = derive from your username, as the driver does.")
    batches = st.text_input(
        "Batches to run", value="365",
        help="Set at trigger time; lower it for a smoke run.")

    # Per-competitor prerequisite blocks.
    comp_values: dict[Engine, dict] = {}
    for eng in competitor_engines:
        cspec = SPECS[eng]
        st.markdown(f"**{cspec.label} details**")
        st.caption("🔑 fields are written to that engine's secret scope, never stored.")
        cv: dict[str, str] = {"scale_factor": scale_factor}
        for f in cspec.fields:
            if f.key == "scale_factor":
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
