# Databricks notebook source
# DBTITLE 1,Install Jinja
pip install jinja2

# COMMAND ----------

# DBTITLE 1,Bootstrap SetupContext and re-export attributes for backward compat
# The real bootstrap logic lives in src/tools/setup_context.py (importable,
# testable). This notebook stays as a %run shim: it creates the context and
# re-exports its attributes into the caller's namespace so existing callers
# that reference bare names (default_workflow, node_types, api_call, ...)
# keep working during the migration to `ctx.X`.
import sys as _sys

_nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_tools_dir = f"/Workspace{_nb_path.split('/src')[0]}/src/tools"
if _tools_dir not in _sys.path:
    _sys.path.insert(0, _tools_dir)

# Force-reload so local edits to setup_context.py pick up without a cluster restart.
_sys.modules.pop("setup_context", None)
from setup_context import SetupContext

ctx = SetupContext(spark, dbutils)

# ---- Backward-compat name bindings (migrate callers to ctx.X over time) ----
api_call            = ctx.api_call
get_node_types      = ctx.get_node_types
get_dbr_versions    = ctx.get_dbr_versions
API_URL             = ctx.api_url
TOKEN               = ctx.token
repo_src_path       = ctx.repo_src_path
workspace_src_path  = ctx.workspace_src_path
user_name           = ctx.user_name
workflows_dict      = ctx.workflows_dict
default_workflow    = ctx.default_workflow
workflow_vals       = ctx.workflow_vals
default_sf          = ctx.default_sf
default_sf_options  = ctx.default_sf_options
default_job_name    = ctx.default_job_name
default_wh          = ctx.default_wh
min_dbr_version     = ctx.min_dbr_version
invalid_dbr_list    = ctx.invalid_dbr_list
features_or_perf    = ctx.features_or_perf
UC_enabled          = ctx.uc_enabled
cloud_provider      = ctx.cloud_provider
node_types          = ctx.node_types
dbrs                = ctx.dbrs
default_dbr_version = ctx.default_dbr_version
default_dbr         = ctx.default_dbr
default_serverless  = ctx.default_serverless
worker_cores_mult   = ctx.worker_cores_mult
default_worker_type = ctx.default_worker_type
default_driver_type = ctx.default_driver_type
cust_mgmt_type      = ctx.cust_mgmt_type
default_catalog     = ctx.default_catalog
