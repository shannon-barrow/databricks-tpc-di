# Databricks notebook source
# DBTITLE 1,Setup: Declare defaults and find basic details about the cloud, DBR versions, and available node types
# MAGIC %run ./tools/setup

# COMMAND ----------

# DBTITLE 1,Declare Widgets and Assign to Variables EXCEPT Worker Count (cluster size is dynamic and dependent on Scale Factor)
# If you prefer changing the cluster size, you can do so (with caution) from the Workflow created

dbutils.widgets.text("scale_factor", default_sf, "Scale factor")
dbutils.widgets.dropdown("workflow_type", default_workflow, workflow_vals, "Workflow Type")
dbutils.widgets.dropdown("driver_type", default_driver_type, list(node_types.keys()), "Driver Type")
dbutils.widgets.dropdown("dbr", list(dbrs.values())[0], list(dbrs.values()), "Databricks Runtime")
dbutils.widgets.dropdown("datagen_rewrite", 'False', ['True', 'False'], "Force Re-Generation of Raw Files")
dbutils.widgets.dropdown("debug", 'True', ['True', 'False'], "Debug Mode")
dbutils.widgets.text("job_name", default_job_name, "Job Name")
dbutils.widgets.text("wh_target", f"{user_name}_TPCDI", 'Root name of Target Warehouse')
dbutils.widgets.text("tpcdi_directory", "/tmp/tpcdi/", "Directory where Raw Files are located")
dbutils.widgets.dropdown("worker_type", default_worker_type, list(node_types.keys()), "Worker Type")

# PARAMETERS
scale_factor      = int(dbutils.widgets.get("scale_factor"))
workflow_type     = dbutils.widgets.get('workflow_type')
wh_target         = dbutils.widgets.get("wh_target")
tpcdi_directory   = dbutils.widgets.get("tpcdi_directory")
dbr_version_id    = list(dbrs.keys())[list(dbrs.values()).index(dbutils.widgets.get("dbr"))]
FORCE_REWRITE     = eval(dbutils.widgets.get("datagen_rewrite"))
DEBUG             = eval(dbutils.widgets.get("debug"))
wf_key            = list(workflows_dict)[workflow_vals.index(workflow_type)]
job_name          = f"{dbutils.widgets.get('job_name')}-SF{scale_factor}-{wf_key}"
worker_node_type  = dbutils.widgets.get("worker_type")
worker_node_count = round(scale_factor * worker_cores_mult / node_types[worker_node_type]['num_cores'])
driver_node_type  = dbutils.widgets.get("driver_type") if worker_node_count > 0 else worker_node_type

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
