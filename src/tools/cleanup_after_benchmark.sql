-- Databricks notebook source
-- MAGIC %md
-- MAGIC # TPC-DI Cleanup
-- MAGIC
-- MAGIC Final task in the benchmark workflow. Drops the schemas produced by
-- MAGIC the run **when `delete_tables_when_finished=TRUE`** (the default the
-- MAGIC Driver bakes into each created job — users override per-run via the
-- MAGIC job parameter).
-- MAGIC
-- MAGIC Schemas dropped:
-- MAGIC - `{catalog}.{wh_db}_{scale_factor}` — final dimensional model.
-- MAGIC - `{catalog}.{wh_db}_{scale_factor}_stage` — staging tables (may not
-- MAGIC   exist for SDP runs at low SF; DROP IF EXISTS makes it a no-op).
-- MAGIC
-- MAGIC Runs `run_if=ALL_DONE` in the workflow, so a partial-failure run still
-- MAGIC has its debris cleaned up (default behavior). Set
-- MAGIC `delete_tables_when_finished=FALSE` to retain the schemas — useful when
-- MAGIC inspecting audit results, debugging mismatches, or comparing two
-- MAGIC variants side-by-side.
-- MAGIC
-- MAGIC SQL notebook (not Python) so it runs uniformly on serverless / job
-- MAGIC clusters / DBSQL warehouses. Uses `:param + IDENTIFIER()` so all three
-- MAGIC compute backends parse the parameter substitution identically.

-- COMMAND ----------

CREATE WIDGET TEXT catalog DEFAULT 'main';
CREATE WIDGET TEXT wh_db DEFAULT '';
CREATE WIDGET TEXT scale_factor DEFAULT '10';
CREATE WIDGET TEXT delete_tables_when_finished DEFAULT 'TRUE';

-- COMMAND ----------

-- Show what the cleanup decision will be — visible in the run output for audit.
SELECT
  :catalog || '.' || :wh_db || '_' || :scale_factor             AS target_schema,
  :catalog || '.' || :wh_db || '_' || :scale_factor || '_stage' AS stage_schema,
  upper(:delete_tables_when_finished) IN ('TRUE','YES','T','Y','1') AS will_delete;

-- COMMAND ----------

-- Conditional drop. Compound BEGIN/IF/END so we can skip the DROPs entirely
-- when delete_tables_when_finished=FALSE (vs DROP IF EXISTS, which would
-- silently no-op only when the schema is already gone).
BEGIN
  IF upper(:delete_tables_when_finished) IN ('TRUE','YES','T','Y','1') THEN
    DROP SCHEMA IF EXISTS IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor) CASCADE;
    DROP SCHEMA IF EXISTS IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '_stage') CASCADE;
  END IF;
END;
