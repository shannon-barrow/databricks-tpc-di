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
    COMPETITIVE_ENGINES, COMPETITIVE_BATCH_TYPES,
)
from backend import backend, USE_MOCK

st.set_page_config(page_title="TPC-DI Guided Builder", layout="centered")

st.title("TPC-DI Guided Benchmark Builder")
st.caption("Answer each step; the next one appears based on your choice.")
if USE_MOCK:
    st.info("Mock mode — nothing is written or created. "
            "Set USE_MOCK_BACKEND=false to emit real workflows.", icon="🧪")

SF_OPTIONS = ["10", "100", "1000", "5000", "10000", "20000"]


def _pick(label, options, help=None):
    """Render a selectbox with a blank leading option so a step stays
    'unanswered' until the user actively chooses. Returns None until then."""
    choice = st.selectbox(label, ["— select —", *options], help=help)
    return None if choice == "— select —" else choice


# --- Step 1: batch type ------------------------------------------------------
batch_type = _pick(
    "**1. What kind of run?**", BATCH_TYPES,
    help="Single Batch = all history at once. Incremental = per-day CDC. "
         "Augmented Incremental = the daily-streaming benchmark (also the only "
         "one with competitive engines).",
)
if not batch_type:
    st.stop()

# --- Step 2: Databricks vs competitive (competitive only for Augmented) ------
if batch_type in COMPETITIVE_BATCH_TYPES:
    run_target = _pick(
        "**2. Databricks or a competitor?**",
        ["Databricks", "Competitor (dbt)"],
        help="Competitive runs (Redshift / BigQuery / Snowflake) use the dbt "
             "project and only exist for Augmented Incremental.",
    )
    if not run_target:
        st.stop()
    is_competitive = run_target == "Competitor (dbt)"
else:
    is_competitive = False
    st.markdown("**2.** Databricks run _(competitive engines are Augmented-only)_.")

# --- Step 3: engine / SKU ----------------------------------------------------
engine = Engine.DATABRICKS
sku = None
edition = "CORE"

if is_competitive:
    eng_label = _pick(
        "**3. Which competitor?**",
        [SPECS[e].label for e in COMPETITIVE_ENGINES],
    )
    if not eng_label:
        st.stop()
    engine = next(e for e in COMPETITIVE_ENGINES if SPECS[e].label == eng_label)
else:
    sku = _pick(
        "**3. Which Databricks SKU?**", DBX_SKUS_BY_BATCH[batch_type],
        help="Filtered to what's valid for this batch type.",
    )
    if not sku:
        st.stop()
    if sku == "SDP" and batch_type == "Single Batch":
        edition = _pick("**3a. SDP edition?**", SDP_EDITIONS) or ""
        if not edition:
            st.stop()

# --- Step 4: scale factor ----------------------------------------------------
scale_factor = _pick("**4. Scale factor?**", SF_OPTIONS)
if not scale_factor:
    st.stop()

# --- Step 5: engine-specific inputs (competitor prerequisites) ---------------
# For competitive runs, collect the non-derivable prerequisites from the
# engine's spec. For native Databricks, these all derive — skip to defaults.
spec = SPECS[engine]
values: dict[str, str] = {"scale_factor": scale_factor}

st.divider()
with st.form("details"):
    if is_competitive:
        st.markdown(f"**5. {spec.label} details**")
        st.caption("🔑 fields are written to the secret scope, never stored.")
        for f in spec.fields:
            if f.key == "scale_factor":
                continue
            if f.kind is FieldKind.SECRET:
                values[f.key] = st.text_input(f"{f.label} 🔑", type="password",
                                              help=f.help or None)
            elif f.kind is FieldKind.DERIVED:
                values[f.key] = st.text_input(f"{f.label} (blank = derive)",
                                              help=f.help or None)
            else:
                values[f.key] = st.text_input(f.label, value=f.default,
                                              help=f.help or None)
    else:
        # Native Databricks: show the defaults we'd use, editable in one place.
        st.markdown("**5. Confirm defaults** _(edit if needed)_")
        values["variant"] = sku or "Cluster"
        values["edition"] = edition
        values["catalog"] = st.text_input("Target catalog", value="main")
        values["wh_db"] = st.text_input(
            "Target schema (wh_db)", value="",
            help="Blank = derive from your username, as the driver does.")
        values["incremental_batches_to_run"] = st.text_input(
            "Batches to run", value="365",
            help="Set at trigger time; lower it for a smoke run.")

    submitted = st.form_submit_button("Review & create")

if not submitted:
    st.stop()

# --- Validate + emit ---------------------------------------------------------
missing = [f.label for f in spec.fields
           if f.required and f.key != "scale_factor" and not values.get(f.key)]
if missing:
    st.error("Missing required fields: " + ", ".join(missing))
    st.stop()

if is_competitive:
    for f in spec.secret_fields():
        if values.get(f.key):
            backend.write_secret(f.secret_scope, f.secret_key, values[f.key])

try:
    result = backend.create_workflow(spec, values)
except NotImplementedError as e:
    st.warning(f"Not wired yet: {e}")
    st.stop()

st.success("Workflow created.")
st.json(result)
if not result.get("mock"):
    batches = values.get("incremental_batches_to_run") or "365"
    st.caption("Trigger the parent job to run:")
    st.code(
        f'databricks jobs run-now {result["parent_id"]} --json '
        f'\'{{"job_parameters": {{"incremental_batches_to_run": "{batches}"}}}}\'',
        language="bash",
    )
