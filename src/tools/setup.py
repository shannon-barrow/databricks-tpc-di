# Databricks notebook source
from helpers import *
import string

# COMMAND ----------

workflows_dict = {"NATIVE": "Native Notebooks Workflow", "DLT-CORE": "CORE Delta Live Tables Pipeline", "DLT-PRO": "PRO Delta Live Tables Pipeline with SCD Type 1/2", "DLT-ADVANCED": "ADVANCED Delta Live Tables Pipeline with DQ"}
workflow_vals = list(workflows_dict.values())
default_sf = '10'
user_name = spark.sql("select current_user()").collect()[0][0].split("@")[0].replace(".","_")
cloud_provider = spark.conf.get('spark.databricks.cloudProvider') # "Azure" or "AWS"
default_job_name = f"{string.capwords(user_name.replace('_',' ')).replace(' ','-')}-TPCDI"
default_node_type = "m5d.2xlarge" if cloud_provider == 'AWS' else 'Standard_D8ds_v5'

# ENV-Specific API calls and variables
API_URL = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None)
TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)
if API_URL is None or TOKEN is None: 
  dbutils.notebook.exit("Unable to capture API/Token from dbutils. Please try again or open a ticket")
repo_src_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().split("/tools")[0]
node_labels, node_types = get_node_types(TOKEN, API_URL)
node_labels.sort()
dbrs, dbr_labels = get_dbr_versions(TOKEN, API_URL)
valid_dbr_labels = sorted([x for x in dbr_labels if 'aarch64' not in x and 'ML' not in x and 'snapshot' not in x.lower() and 'GPU' not in x and 'Photon' not in x and 'RC' not in x and 'Light' not in x and 'HLS' not in x and 'Beta' not in x and 'Latest' not in x and float(x.split()[0]) > 10.0], reverse=True)
default_dbr = [x for x in valid_dbr_labels if str(max([float(y.split()[0]) for y in [z for z in dbr_labels if 'LTS' in z]])) in x].pop()

shuffle_partitions_mult = .09
worker_core_count_mult = .06
cores_per_node = 8
