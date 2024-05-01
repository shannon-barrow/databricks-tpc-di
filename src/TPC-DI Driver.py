# Databricks notebook source
# MAGIC %md
# MAGIC # Welcome to the TPC-DI Spec Implementation on Databricks!

# COMMAND ----------

# MAGIC %md
# MAGIC ## The README doc is outdated but will be updated soon
# MAGIC Meanwhile please choose from the widgets above, starting with the WORKFLOW TYPE:
# MAGIC 1. CLUSTER-based Workflow with Structured Streaming Notebooks  
# MAGIC         * Leverage traditional cluster and do an incremental (benchmarked) run OR execute all batches in one pass
# MAGIC 2. DBSQL WAREHOUSE-based Workflow with Structured Streaming Notebooks  
# MAGIC         * Same as cluster approached above BUT against a DBSQL Warehouse. Do an incremental (benchmarked) run OR execute all batches in one pass
# MAGIC 3. Delta Live Tables Pipeline: CORE Sku  
# MAGIC         * Best TCO Option
# MAGIC 4. Delta Live Tables Pipeline: PRO Sku  
# MAGIC         * Leverage APPLY CHANGES INTO to Simplify Slowly Changing Dimensions Ingestion, with both Type 1 and Type 2 SCD
# MAGIC 5. Delta Live Tables Pipeline: ADVANCED Sku  
# MAGIC         * Easily add in Data Quality to your pipeline to either limit ingestion of poor data or gain insights into your DQ - or both!
# MAGIC
# MAGIC ***The following are under modification to update with latest code the above patterns recently receeived, but will be available again soon!***
# MAGIC 1. <a href="https://docs.getdbt.com/" target="_blank">dbt Core</a>  
# MAGIC         * Built using dbt Core, this version allows the code to be run consistently across other DWs for comparing TCO!  
# MAGIC         * The <a href="https://github.com/rlsalcido24/dbtpcdi" target="_blank">dbt TPC-DI repo</a> exists externally but is called by the Workflow created here with all dependencies met
# MAGIC 2. <a href="https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-streaming-table.html" target="_blank">Streaming Tables</a> and <a href="https://docs.databricks.com/en/sql/user/materialized-views.html" target="_blank">Materialized Views</a>  
# MAGIC         * Leverage the latest in SQL-based streaming tables and materialized views. Built using DBSQL, executed as a DLT Pipeline!
# MAGIC

# COMMAND ----------

# DBTITLE 1,Setup: Declare defaults and find basic details about the cloud, DBR versions, and available node types
# MAGIC %run ./tools/setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Many options are configurable from the workflow_builder EXCEPT the cluster or warehouse size.
# MAGIC - Serverless clusters/DLT pipelines are dynamically autoscaled
# MAGIC - Clusters and warehouses are sized to achieve the best TCO based on the scale factor chosen
# MAGIC - If you prefer changing the cluster/WH size for your own testing then it can be manually modified from the Workflow created by this notebook

# COMMAND ----------

# DBTITLE 1,Declare Widgets and Assign to Variables EXCEPT Worker Count
dbutils.widgets.dropdown("scale_factor", default_sf, default_sf_options, "Scale factor")
dbutils.widgets.dropdown("workflow_type", default_workflow, workflow_vals, "Workflow Type")
dbutils.widgets.dropdown("serverless", default_serverless, ['YES', 'NO'], "Enable Serverless")
dbutils.widgets.dropdown("batched", 'Single Collective Batch', ['Single Collective Batch', 'Incremental Batches'], "Collective batch or incremental batches")
dbutils.widgets.dropdown("worker_type", default_worker_type, list(node_types.keys()), "Worker Type")
dbutils.widgets.dropdown("driver_type", default_driver_type, list(node_types.keys()), "Driver Type")
dbutils.widgets.dropdown("dbr", default_dbr, list(dbrs.values()), "Databricks Runtime")
dbutils.widgets.text("job_name", default_job_name, "Job Name")
dbutils.widgets.text("wh_target", default_wh, 'Target Database')
dbutils.widgets.text("catalog", default_catalog, 'Target Catalog')

scale_factor      = int(dbutils.widgets.get("scale_factor"))
workflow_type     = dbutils.widgets.get('workflow_type')
wh_target         = dbutils.widgets.get("wh_target")
catalog           = dbutils.widgets.get("catalog")
wf_key            = list(workflows_dict)[workflow_vals.index(workflow_type)]
sku               = wf_key.split('-')
job_name          = f"{dbutils.widgets.get('job_name')}-SF{scale_factor}-{wf_key}"
serverless        = dbutils.widgets.get('serverless')
worker_node_type  = dbutils.widgets.get("worker_type")
driver_node_type  = dbutils.widgets.get("driver_type")
dbr_version_id    = list(dbrs.keys())[list(dbrs.values()).index(dbutils.widgets.get("dbr"))]
incremental       = True if dbutils.widgets.get("batched") == 'Incremental Batches' else False

if sku[0] not in ['CLUSTER','DBSQL']:
  dbutils.widgets.remove('batched')

if sku[0] not in ['CLUSTER','DLT'] or serverless == 'YES':
  dbutils.widgets.remove('worker_type')
  dbutils.widgets.remove('driver_type')
  dbutils.widgets.remove('dbr')

if sku[0] in ['DBT', 'STMV']:
  displayHTML(f"<h1>Please select a different Workflow Type from the widgets above and rerun. The DBT and Streaming Tables/Materialized Views flows are 'UNDER RENOVATION' </h1>")
  raise Exception("Please select a different Workflow Type from the widgets above and rerun<. The DBT and Streaming Tables/Materialized Views flows are 'UNDER RENOVATION'")

# COMMAND ----------

# MAGIC %md 
# MAGIC **Data Generation can take a few minutes on smaller scale factors, or hours on higher scale factors (i.e. 10,000 scale factor). Review README for more details**. 

# COMMAND ----------

# DBTITLE 1,Copy DIGen jar file and dependencies from repo to driver, then use dbutils to copy from driver to DBFS.  
# MAGIC %run ./tools/data_generator

# COMMAND ----------

# DBTITLE 1,Generate and submit the Databricks Workflow
# MAGIC %run ./tools/generate_workflow

# COMMAND ----------

dbutils.notebook.exit(job_id)
