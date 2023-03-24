# Databricks notebook source
# MAGIC %md
# MAGIC # Setup

# COMMAND ----------

# DBTITLE 1,Declare defaults and find basic details about the cloud, DBR versions, and available node types
# MAGIC %run ./setup

# COMMAND ----------

# MAGIC %md
# MAGIC # Declare Widgets and Assign to Variables
# MAGIC * All widgets are declared here EXCEPT the worker count. The intention is to make this number dynamic and dependent on the SCALE FACTOR chosen.

# COMMAND ----------

# WIDGETS
dbutils.widgets.dropdown("workflow_type", "Native Notebooks Workflow", workflow_vals, "Workflow Type")
dbutils.widgets.dropdown("driver_type", default_node_type, node_labels, "Driver Type")
dbutils.widgets.dropdown("dbr", default_dbr, valid_dbr_labels, "Databricks Runtime")
dbutils.widgets.dropdown("datagen_rewrite", 'False', ['True', 'False'], "Force Re-Generation of Raw Files")
dbutils.widgets.dropdown("debug", 'True', ['True', 'False'], "Debug Mode")
dbutils.widgets.text("scale_factor", default_sf, "Scale factor")
dbutils.widgets.text("job_name", default_job_name, "Job Name")
dbutils.widgets.text("wh_target", f"{user_name}_TPCDI", 'Root name of Target Warehouse')
dbutils.widgets.text("tpcdi_directory", "/tmp/tpcdi/", "Directory where Raw Files are located")
dbutils.widgets.dropdown("worker_type", default_node_type, node_labels, "Worker Type")

# PARAMETERS
workflow_type          = dbutils.widgets.get('workflow_type')
wh_target              = dbutils.widgets.get("wh_target")
scale_factor           = int(dbutils.widgets.get("scale_factor"))
tpcdi_directory        = dbutils.widgets.get("tpcdi_directory")
driver_node_type_label = dbutils.widgets.get("driver_type")
dbr_version_id         = dbutils.widgets.get("dbr")
worker_node_type_label = dbutils.widgets.get("worker_type")
FORCE_REWRITE          = eval(dbutils.widgets.get("datagen_rewrite"))
debug_mode             = eval(dbutils.widgets.get("debug"))
wf_key                 = list(workflows_dict)[workflow_vals.index(workflow_type)]
job_name               = f"{dbutils.widgets.get('job_name')}-SF{scale_factor}-{wf_key}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Size the number of Cores Based on the Scale Factor Selected! 
# MAGIC * This will create a widget to allow you to change this value BUT we caution against changing unless you have knowledge of sizing clusters appropriately.
# MAGIC * The size of raw data is 1 GB per 10 Scale Factor, meaning a single node could do a default 10 Scale Factor.
# MAGIC * We have tested on 10, 100, 1000, and 10000 SCALE FACTORS. Ideally, you will need ~10% of the SCALE FACTOR for the shuffle partitions (otherwise you can face spill as memory fills up - causing the job to run slower).
# MAGIC * Therefore, you can size the cluster to match these shuffle partitions count or opt for a slightly smaller cluster than this, which causes a slightly slower execution time but has shown to lower the TCO. We have noticed the sweetspot in the 50-80% of the shuffle partion count.  
# MAGIC * Our default worker has 8-cores, so select number of cores to land in this range.
# MAGIC * Default settings:
# MAGIC   * SF 100   = 10 shuffle partitions   = at least 8 Worker Cores
# MAGIC   * SF 1000  = 100 shuffle partitions  = at least 64 Worker Cores
# MAGIC   * SF 10000 = 1000 shuffle partitions = at least 576 Worker Cores

# COMMAND ----------

default_worker_count = round(scale_factor * worker_core_count_mult / cores_per_node)
dbutils.widgets.dropdown("worker_count", f"{default_worker_count}", [str(i) for i in range(100)], "Worker Count")
worker_node_count  = int(dbutils.widgets.get("worker_count"))
shuffle_partitions = 8 if scale_factor < 100 else int(scale_factor * shuffle_partitions_mult)

# DAG of args to send to Jinja
dag_args = {"wh_target":wh_target, 
            "tpcdi_directory":tpcdi_directory, 
            "scale_factor":scale_factor, 
            "job_name":job_name, 
            "repo_src_path":repo_src_path,
            "cloud_provider":cloud_provider,
            "worker_node_type_id":worker_node_type_label,
            "driver_node_type_id":driver_node_type_label,
            "worker_node_count":worker_node_count,
            "dbr":dbrs[dbr_version_id],
            "shuffle_partitions":shuffle_partitions
           }

# To turn on debug mode change widget option to True
if debug_mode:
  print(f"""workflow_type: {workflow_type}
  {dag_args}
  """)

# COMMAND ----------

# DBTITLE 1,Warnings around size of cluster or Scale Factor
if default_worker_count > worker_node_count: 
  print(F"WARNING! YOUR CLUSTER SIZE OF {worker_node_count} WORKER NODES IS SMALLER THAN THE SUGGESTED {default_worker_count} WORKER NODES!")
if scale_factor > 1000: 
  print(F"""
  WARNING! YOUR SCALE FACTOR OF {scale_factor} MAY CAUSE A LONGER RAW FILE GENERATION STEP! 
  IF THIS IS INTENDED, BE AWARE THAT THE DATA GENERATION JAR PROVIDED CAN TAKE UP TO AN HOUR OR MORE AT HIGHER SCALE FACTORS!  
  IF BUILDING SCALE FACTORS OVER 100 THE RECOMMENDATION IS TO USE A LARGE SINGLE NODE CLUSTER WITH SSD STORAGE!
  """)

# COMMAND ----------

# MAGIC %md 
# MAGIC # Data Generation
# MAGIC 
# MAGIC - We first need to copy the DIGen jar file and dependencies located inside the tpcdi-sql repo to a /tmp location on DBFS.
# MAGIC - Then, we'll the execute the DIGen wrapper with a given scale factor. This operation can take a few minutes on smaller scale factors, or hours on higher scale factors (i.e. 10,000 scale factor)

# COMMAND ----------

# MAGIC %run ./data_generator

# COMMAND ----------

# MAGIC %md 
# MAGIC # Generate and submit the Databricks Workflow
# MAGIC * This will create a workflow for either the Traditonal native notebooks or your choice of Delta Live Tables options

# COMMAND ----------

job_id = generate_workflow(wf_key, f"/Workspace{repo_src_path}/tools/jinja_templates/", dag_args, TOKEN, API_URL)
url = f"/#job/{job_id}"
displayHTML(f"<h1><a href={url}>Your Workflow can be found here!</a></h1>")

# COMMAND ----------

dbutils.notebook.exit(job_id)
