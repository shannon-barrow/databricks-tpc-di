# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Databricks TPC-DI SQL Benchmark
# MAGIC ## Source code can be found within our [Git Repo](https://github.com/databricks/tpcdi-sql)
# MAGIC 
# MAGIC ## Click Run all to populate Parameters
# MAGIC * Notebook will stop after parameters are populated, to allow you to make your selections
# MAGIC * Then go to Data Generation Bookmark or command 12 and click run all below 
# MAGIC * The notebook will generate the Multi Task Job definition, send it to the jobs API, and run it
# MAGIC * WILL NOT WORK ON A HIGH CONCURRENCY CLUSTER - Scala is required for xml parsing
# MAGIC 
# MAGIC ## Notebook Parameters
# MAGIC 
# MAGIC __1. Name of the environment:__ Two databases will be created using the name you provide as a prefix. <br>
# MAGIC i.e. yourname_tpcdi_warehouse and yourname_tpcdi_staging
# MAGIC 
# MAGIC __2. Scale Factor :__ The higher the scale factor, the more data is used. We have loaded 3 and 10000.
# MAGIC 
# MAGIC __3. Directory where files are loaded :__ /tmp/tpcdi is the default
# MAGIC 
# MAGIC __4. Please enter an existing cluster ID if you are choose to execute using Delta Live Tables.__ This is required for one job which requires a Scala based XML parsing library that is currently unsupported in DLT.
# MAGIC 
# MAGIC __5. Workflow Type:__ Please choose Traditional (Workflow / Jobs) or Delta Live Tables

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install Dependencies

# COMMAND ----------

# MAGIC %pip install jinja2

# COMMAND ----------

import json
from pprint import pprint
from tools.helpers import (copy_directory, 
                           api_call, 
                           get_node_types,
                           mtj_generate, 
                           mtj_submit, 
                           mtj_start, 
                           mtj_get_run, 
                           get_dbr_versions, 
                           dlt_generate, 
                           dlt_new_pipeline, 
                           dlt_mtj_generate
                          )
from jinja2 import Template

# COMMAND ----------

def get_auth_params():
  API_URL = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None)
  TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)
  if API_URL is None:
    return "Unable to capture API from Dbutils, open an issue on github"
  if TOKEN is None:
    return "Unable to capture Token from Dbutils, open an issue on github"
  return (API_URL, TOKEN)

user_name = spark.sql("select current_user()").collect()[0][0].split("@")[0].replace(".","_")

API_URL, TOKEN          = get_auth_params()
node_types, node_labels = get_node_types(TOKEN, API_URL)
dbrs, dbr_labels        = get_dbr_versions(TOKEN, API_URL)
cloud_provider          = spark.conf.get('spark.databricks.cloudProvider') # "Azure" or "AWS"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set up widgets and assign variables suited for the execution in this environment

# COMMAND ----------

valid_dbr_labels = [x for x in dbr_labels if 'aarch64' not in x and 'ML' not in x and 'snapshot' not in x.lower() and 'GPU' not in x and 'Photon' in x]
default_dbr = [x for x in valid_dbr_labels if str(max([float(y.split()[0]) for y in [z for z in dbr_labels if 'LTS' in z]])) in x].pop()

dbutils.widgets.dropdown("Workflow Type", "Traditional", ["Delta Live Tables", "Traditional"])
dbutils.widgets.dropdown("Cluster Type", "Existing Interactive", ["New Jobs Cluster", "Existing Interactive"])
dbutils.widgets.text("Cluster ID", "Cluster ID (If not using Jobs)", "Cluster ID")
dbutils.widgets.dropdown("Driver Type", "NA", ["NA"]+node_labels)
dbutils.widgets.dropdown("Worker Type", "NA", ["NA"]+node_labels)
dbutils.widgets.dropdown("Worker Count", "8", [str(i) for i in range(21)])
dbutils.widgets.dropdown("DBR Type", default_dbr, valid_dbr_labels)
dbutils.widgets.dropdown("Generate Data", "False", ["True", "False"])
dbutils.widgets.text("env", user_name,'Name of the environment')

dbutils.widgets.text("files_directory", f"/tmp/{user_name}/tpc-di", "Directory where Files are located")
dbutils.widgets.text("scale_factor", "3", "Scale factor")
dbutils.widgets.text("Job Name", f"TPC-DI-BENCHMARK-{user_name}", "Job Name")


# PARAMETERS
env                    = dbutils.widgets.get("env")
files_directory        = dbutils.widgets.get("files_directory")
scale_factor           = dbutils.widgets.get("scale_factor")
job_name               = dbutils.widgets.get("Job Name")
generate_data          = dbutils.widgets.get("Generate Data")
worker_node_type_label = dbutils.widgets.get("Worker Type")
driver_node_type_label = dbutils.widgets.get("Driver Type")
worker_node_count      = dbutils.widgets.get("Worker Count")
cluster_type           = dbutils.widgets.get("Cluster Type")
cluster_id             = dbutils.widgets.get("Cluster ID")
dbr_version_id         = dbutils.widgets.get("DBR Type")
workflow_type          = dbutils.widgets.get("Workflow Type")

if worker_node_type_label == "NA" and driver_node_type_label == "NA" and cluster_type == "Existing Interactive":
    cluster_type = "Existing Interactive"
elif worker_node_type_label != "NA" and driver_node_type_label != "NA" and cluster_type == "New Jobs Cluster":
    cluster_type = "New Jobs Cluster"

# PATHS
repo_directory   = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().split("/tpc-di__benchmark_run/TPC-DI")[0]
repo_root        = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().split("/TPC-DI")[0]
notebook_path    = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
datagen_tmp_path = "/dbfs/tmp/datagen"
datagen_jar_path = datagen_tmp_path + "/DIGen.jar"
datagen_out_path = f"/dbfs{files_directory}"
datagen_git_path = "/Workspace"+repo_root

dag_args = {"env":env, 
            "files_directory":files_directory, 
            "scale_factor":scale_factor, 
            "job_name":job_name, 
            "cluster_id":cluster_id, 
            "repo_directory":repo_directory,
            "cloud_provider":cloud_provider,
            "cluster_type":cluster_type,
            "worker_node_type_id":node_types[worker_node_type_label],
            "worker_node_count":worker_node_count,
            "dbr_version_type_id":dbrs[dbr_version_id]
           }
# Print a summary
print_summary=False
if print_summary:
    print(f"""---- Application Parameters
    env: {env}
    generate_date: {generate_data}
    scale_factor: {scale_factor}

    ---- Application Paths
    files_directory: {files_directory}
    repo_directory: {repo_directory} 
    repo_root: {repo_root}
    notebook_path: {notebook_path}

    ---- Data Generation Paths
    datagen_tmp_path: {datagen_tmp_path}
    datagen_jar_path: {datagen_jar_path}
    datagen_out_path: {datagen_out_path}
    datagen_git_path: {datagen_git_path}

    ---- Cluster Configuration
    Driver Type: {driver_node_type_label}
    Worker Type: {worker_node_count} X {worker_node_type_label}
    Cluster Type: {cluster_type}
    DBR Type: {dbr_version_id}
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## The following Cell forces the notebook to stop running.
# MAGIC * This helps to ensure the notebook doesn't fail in the case it is pulled from Git, "Run all" is selected, and the default widget values are not changed!
# MAGIC * Click 'Run All Below' after the following cell once you have set the parameters accordingly

# COMMAND ----------

dbutils.notebook.exit("Choose your parameters wisely! Click 'Run All Below' after this cell once you have set the parameters accordingly")

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Data Generation
# MAGIC 
# MAGIC - We first need to copy the DIGen jar file and dependencies located inside the tpcdi-sql repo to a /tmp location on DBFS.
# MAGIC - Then, we'll the DIGen wrapper with a given scale factor. This operation can take a few minutes to complete depending on the scale factor provided. 

# COMMAND ----------

if generate_data == "True":
    copy_directory(datagen_git_path+"/tools/datagen", datagen_tmp_path, overwrite=False)
    print(f"Data generation for scale factor {scale_factor} is starting...")
    DIGen(datagen_jar_path, scale_factor, datagen_git_path, datagen_tmp_path, datagen_out_path)
else:
    print("Data generation skipped")

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Generate and submit the Databricks Multi-Task Job for Traditonal or Delta Live Table Wor

# COMMAND ----------

def launch_benchmark(workflow_type, dag_args):
    if workflow_type == "Delta Live Tables":
        dlt_dag = dlt_generate(repo_root, **dag_args)
        pipeline_id = dlt_new_pipeline(pat_token=TOKEN, api_base_url=API_URL, dag_dict=dlt_dag)
        dag_args['pipeline_id'] = pipeline_id
        dlt_mtj_dag = dlt_mtj_generate(repo_root, **dag_args)
        job_id = mtj_submit(pat_token=TOKEN, api_base_url=API_URL, dag_dict=dlt_mtj_dag)
        #run_id = mtj_start(pat_token=TOKEN, api_base_url=API_URL, job_id=job_id)
        #run_response = mtj_get_run(pat_token=TOKEN, api_base_url=API_URL, run_id=run_id)
        url = f"/#job/{job_id}"
        displayHTML(f"""
          <h1><a href={url}> Please visit the pipeline at this link.</a></h1>
        """)
    elif workflow_type == "Traditional":
        dag = mtj_generate(repo_root, **dag_args)
        job_id = mtj_submit(pat_token=TOKEN, api_base_url=API_URL, dag_dict=dag)
        #run_id = mtj_start(pat_token=TOKEN, api_base_url=API_URL, job_id=job_id)
        #run_response = mtj_get_run(pat_token=TOKEN, api_base_url=API_URL, run_id=run_id)
        url = f"/#job/{job_id}"
        displayHTML(f"""
          <h1><a href={url}> Please visit the job at this link.</a></h1>
        """)
launch_benchmark(workflow_type, dag_args)   
