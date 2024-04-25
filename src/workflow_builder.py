# Databricks notebook source
# DBTITLE 1,Setup: Declare defaults and find basic details about the cloud, DBR versions, and available node types
# MAGIC %run ./tools/setup

# COMMAND ----------

# DBTITLE 1,Declare Widgets and Assign to Variables EXCEPT Worker Count (cluster size is dynamic and dependent on Scale Factor)
# If you prefer changing the cluster size, you can do so (with caution) from the Workflow created

dbutils.widgets.dropdown("scale_factor", default_sf, default_sf_options, "Scale factor")
dbutils.widgets.dropdown("workflow_type", default_workflow, workflow_vals, "Workflow Type")
dbutils.widgets.dropdown("serverless", default_serverless, ['YES', 'NO'], "Enable Serverless")
dbutils.widgets.text("job_name", default_job_name, "Job Name")
dbutils.widgets.text("wh_target", default_wh, 'Target Database')
dbutils.widgets.text("catalog", default_catalog, 'Target Catalog')

scale_factor  = int(dbutils.widgets.get("scale_factor"))
workflow_type = dbutils.widgets.get('workflow_type')
wh_target     = dbutils.widgets.get("wh_target")
catalog       = dbutils.widgets.get("catalog")
wf_key        = list(workflows_dict)[workflow_vals.index(workflow_type)]
sku           = wf_key.split('-')
job_name      = f"{dbutils.widgets.get('job_name')}-SF{scale_factor}-{wf_key}"
serverless    = dbutils.widgets.get('serverless')
batch_folder  = "single_batch"
dbutils.widgets.dropdown("batched", 'Single Batch', ['Single Batch', 'Incremental Batches'], "Collective batch or incremental batches")
if dbutils.widgets.get("batched") == 'Incremental Batches':
  # batch_folder = "incremental_batches"
  print("Incremental batches temporarily removed until 4-29-2024 as a code and orchestration update is being made.")
dbutils.widgets.dropdown("worker_type", default_worker_type, list(node_types.keys()), "Worker Type")
dbutils.widgets.dropdown("driver_type", default_driver_type, list(node_types.keys()), "Driver Type")
dbutils.widgets.dropdown("dbr", default_dbr, list(dbrs.values()), "Databricks Runtime")
worker_node_type  = dbutils.widgets.get("worker_type")
driver_node_type  = dbutils.widgets.get("driver_type")
dbr_version_id    = list(dbrs.keys())[list(dbrs.values()).index(dbutils.widgets.get("dbr"))]

if sku[0] not in ['CLUSTER','DBSQL']:
  dbutils.widgets.remove('batched')

if sku[0] not in ['CLUSTER','DLT'] or serverless == 'YES':
  dbutils.widgets.remove('worker_type')
  dbutils.widgets.remove('driver_type')
  dbutils.widgets.remove('dbr')

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
