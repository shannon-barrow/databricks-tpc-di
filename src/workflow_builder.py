# Databricks notebook source
# DBTITLE 1,Setup: Declare defaults and find basic details about the cloud, DBR versions, and available node types
# MAGIC %run ./tools/setup

# COMMAND ----------

# DBTITLE 1,Declare Widgets and Assign to Variables EXCEPT Worker Count (cluster size is dynamic and dependent on Scale Factor)
# Unavailable Tech Summit Options:
# dbutils.widgets.dropdown("serverless", default_serverless, ['YES', 'NO'], "USE SERVERLESS COMPUTE")
# dbutils.widgets.dropdown("scale_factor", default_sf, default_sf_options, "Scale factor")
# dbutils.widgets.dropdown("driver_type", default_driver_type, list(node_types.keys()), "Driver Type")
# dbutils.widgets.dropdown("worker_type", default_worker_type, list(node_types.keys()), "Worker Type")
# dbutils.widgets.dropdown("datagen_rewrite", 'False', ['True', 'False'], "Force Re-Generation of Raw Files")
# dbutils.widgets.text("catalog", default_catalog, 'Target Catalog')
# dbutils.widgets.dropdown("dbr", list(dbrs.values())[0], list(dbrs.values()), "Databricks Runtime")

# Hard-coded Tech Summit Options
serverless        = 'NO'
scale_factor      = 10
driver_node_type  = 'i3.xlarge'
worker_node_type  = 'i3.xlarge'
FORCE_REWRITE     = False
catalog           = 'tpcdi'
dbr_version_id = list(dbrs.keys())[list(dbrs.values()).index(list(dbrs.values())[0])]

# Dynamic variables through Widgets
dbutils.widgets.dropdown("workflow_type", default_workflow, workflow_vals, "Workflow Type")
dbutils.widgets.text("job_name", default_job_name, "Job Name")
dbutils.widgets.text("wh_target", default_wh, 'Target Database')

# Assign widget results to variables
workflow_type  = dbutils.widgets.get('workflow_type')
wh_target      = dbutils.widgets.get("wh_target")
wf_key         = list(workflows_dict)[workflow_vals.index(workflow_type)]
job_name       = f"{dbutils.widgets.get('job_name')}-SF{scale_factor}-{wf_key}"

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
