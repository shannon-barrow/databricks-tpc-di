-- Databricks notebook source
-- MAGIC %md
-- MAGIC # TPC-DI Cleanup
-- MAGIC
-- MAGIC Final task in the benchmark workflow. Drops the schemas produced by
-- MAGIC the run. The decision *whether* to clean up is made by an upstream
-- MAGIC `delete_when_finished_TRUE_FALSE` condition_task in the workflow,
-- MAGIC which compares the `delete_tables_when_finished` job parameter to
-- MAGIC `TRUE`; this notebook only fires when that gate evaluates true. Set
-- MAGIC the parameter to `FALSE` per-run to retain the schemas — useful when
-- MAGIC inspecting audit results, debugging mismatches, or comparing two
-- MAGIC variants side-by-side.
-- MAGIC
-- MAGIC Schemas dropped:
-- MAGIC - `{catalog}.{wh_db}_{scale_factor}` — final dimensional model.
-- MAGIC - `{catalog}.{wh_db}_{scale_factor}_stage` — staging tables (may not
-- MAGIC   exist for SDP runs at low SF; DROP IF EXISTS makes it a no-op).
-- MAGIC
-- MAGIC SQL notebook (not Python) so it runs uniformly on serverless / job
-- MAGIC clusters / DBSQL warehouses. Uses `:param + IDENTIFIER()` so all three
-- MAGIC compute backends parse the parameter substitution identically.

-- COMMAND ----------

-- :catalog / :wh_db / :scale_factor come from the task's base_parameters and
-- bind automatically on both clusters and SQL warehouses. CREATE WIDGET is
-- left commented out so it doesn't error on warehouses (which reject that
-- syntax) — uncomment when running interactively from a notebook.
-- CREATE WIDGET TEXT catalog DEFAULT 'main';
-- CREATE WIDGET TEXT wh_db DEFAULT '';
-- CREATE WIDGET TEXT scale_factor DEFAULT '10';

-- COMMAND ----------

-- Show what's about to be dropped — visible in the run output for audit.
SELECT
  :catalog || '.' || :wh_db || '_' || :scale_factor             AS target_schema,
  :catalog || '.' || :wh_db || '_' || :scale_factor || '_stage' AS stage_schema;

-- COMMAND ----------

DROP SCHEMA IF EXISTS IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor) CASCADE;
DROP SCHEMA IF EXISTS IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '_stage') CASCADE;
