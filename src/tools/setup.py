# Databricks notebook source
# DBTITLE 1,Put tools/ on sys.path, import the TPC-DI entry points, bootstrap tpcdi_config
# This cell does the pieces that can't cleanly live inside an importable Python module — sys.path manipulation, %pip install, and module-cache reset so that edits to tools/*.py pick up without a cluster restart. All actual logic and data lives in setup_context.py / generate_*_workflow.py and is callable standalone (importable, testable).
import sys as _sys

_nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_tools_dir = f"/Workspace{_nb_path.split('/src')[0]}/src/tools"
if _tools_dir not in _sys.path:
    _sys.path.insert(0, _tools_dir)

for _m in ("setup_context", "_workflow_utils",
           "generate_datagen_workflow", "generate_benchmark_workflow"):
    _sys.modules.pop(_m, None)

from setup_context import SetupContext
from generate_datagen_workflow import generate_datagen_workflow
from generate_benchmark_workflow import generate_benchmark_workflow

tpcdi_config = SetupContext(spark, dbutils)
