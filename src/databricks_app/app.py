"""TPC-DI benchmark app (Streamlit).

Primary use case: set up and emit the competitive (non-Databricks) TPC-DI
benchmark workflows, which otherwise need many non-derivable inputs
(workspace, bucket, account, warehouse, credentials, catalog federation).

v1 = Create mode: pick an engine, fill its prerequisites, and the app writes
any secrets to the target secret scope and emits the parent+child workflow.
Adjust + Diagnose modes come later.
"""
import streamlit as st

from models import SPECS, Engine, FieldKind
from backend import backend, USE_MOCK

st.set_page_config(page_title="TPC-DI Benchmark Builder", layout="centered")

st.title("TPC-DI Benchmark Builder")
if USE_MOCK:
    st.info("Mock backend — nothing is written or created. "
            "Set USE_MOCK_BACKEND=false to emit real workflows.", icon="🧪")

engine = Engine(st.selectbox(
    "Run type",
    options=[e.value for e in Engine],
    format_func=lambda v: SPECS[Engine(v)].label,
))
spec = SPECS[engine]

st.caption(spec.label)
values: dict[str, str] = {}

with st.form("benchmark_form"):
    for f in spec.fields:
        kwargs = {"help": f.help or None, "value": f.default}
        if f.kind is FieldKind.SECRET:
            values[f.key] = st.text_input(f"{f.label} 🔑", type="password",
                                          help=f.help or None)
        elif f.kind is FieldKind.DERIVED:
            values[f.key] = st.text_input(f"{f.label} (blank = derive)",
                                          help=f.help or None)
        else:
            values[f.key] = st.text_input(f.label, **kwargs)
    submitted = st.form_submit_button("Create workflow")

if submitted:
    # Validate required fields.
    missing = [f.label for f in spec.fields
               if f.required and not values.get(f.key)]
    if missing:
        st.error("Missing required fields: " + ", ".join(missing))
        st.stop()

    # 1. Write secrets to the target scope (value never retained by the app).
    for f in spec.secret_fields():
        if values.get(f.key):
            backend.write_secret(f.secret_scope, f.secret_key, values[f.key])

    # 2. Emit the workflow.
    try:
        result = backend.create_workflow(spec, values)
    except NotImplementedError as e:
        st.warning(f"Not wired yet: {e}")
        st.stop()

    st.success("Workflow created.")
    st.json(result)
    if not result.get("mock"):
        # incremental_batches_to_run is a run-now param, set at trigger time.
        batches = values.get("incremental_batches_to_run") or "365"
        st.caption("Trigger the parent job to run:")
        st.code(
            f'databricks jobs run-now {result["parent_id"]} --json '
            f'\'{{"job_parameters": {{"incremental_batches_to_run": "{batches}"}}}}\'',
            language="bash",
        )
