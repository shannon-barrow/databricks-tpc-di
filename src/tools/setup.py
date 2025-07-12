# Databricks notebook source
# DBTITLE 1,Install Jinja
pip install jinja2

# COMMAND ----------

import requests
import string
import json
import collections
import os

# COMMAND ----------

def api_call(json_payload=None, request_type=None, api_endpoint=None):
  headers = {'Content-type': 'application/json', 'Accept':'*/*', 'Authorization': f'Bearer {TOKEN}'}
  if request_type == "POST":
    response = requests.post(f"{API_URL}{api_endpoint}", json=json_payload, headers=headers)
  elif request_type == "GET":
    response = requests.get(f"{API_URL}{api_endpoint}", json=json_payload, headers=headers)
  else:
    dbutils.notebook.exit(f"Invalid request type: {request_type}")
    return
  if response.status_code == 200: return response
  else: dbutils.notebook.exit(f"API call failed with status code {response.status_code}: {response.text}")
  

def get_node_types():
  response = api_call(json_payload=None, request_type="GET", api_endpoint="/api/2.0/clusters/list-node-types")
  node_types_list = json.loads(response.text)['node_types']
  node_types_dict = {}
  for node in node_types_list:
    node_type_id = node.pop('node_type_id')
    node_types_dict[node_type_id] = node
  return collections.OrderedDict(sorted(node_types_dict.items()))

def get_dbr_versions(min_version=14.1):
  response = api_call(json_payload=None, request_type="GET", api_endpoint="/api/2.0/clusters/spark-versions")
  dbr_versions_list = json.loads(response.text)['versions']
  dbr_versions_dict = {}
  for dbr in dbr_versions_list:
    if not any(invalid in dbr['name'] for invalid in invalid_dbr_list):
      if float(dbr['name'].split(' ')[0]) >=  min_version:
        dbr_versions_dict[dbr['key']] = dbr['name']
  return collections.OrderedDict(sorted(dbr_versions_dict.items(), reverse=True))

def is_lighthouse():
  ## 
  # DEFAULT TO RETURN FALSE FOR THE MOMENT UNTIL LIGHTHOUSE IS MORE PREVALENT AND/OR A BETTER WAY TO DETEMINE A NOTEBOOK IS RUNNING IN LIGHTHOUSE IS IDENTIFIED
  ##
  return False

def is_serverless():
  return os.getenv("IS_SERVERLESS", "") == 'TRUE'


# COMMAND ----------

# ENV-Specific API calls and variables
API_URL = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None)
TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)
if API_URL is None or TOKEN is None: 
  dbutils.notebook.exit("Unable to capture API/Token from dbutils. Please try again or open a ticket")
repo_src_path = f"{dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().split('/src')[0]}/src"
workspace_src_path = f"/Workspace{repo_src_path}"
user_name = spark.sql("select lower(regexp_replace(split(current_user(), '@')[0], '(\\\W+)', ' '))").collect()[0][0]
workflows_dict      = {
  "CLUSTER": "Workspace Cluster Workflow", 
  "DBSQL": "DBSQL Warehouse Workflow",
  "DLT-CORE": "CORE Delta Live Tables Pipeline", 
  "DLT-PRO": "PRO Delta Live Tables Pipeline with SCD Type 1/2", 
  "DLT-ADVANCED": "ADVANCED Delta Live Tables Pipeline with DQ",
  # "DBT": "dbt Core on DB SQL Warehouse",
  # "STMV": "Streaming Tables and Materialized Views on DBSQL/DLT"
}
default_workflow   = workflows_dict['CLUSTER']
default_sf         = '10'
default_job_name   = f"{string.capwords(user_name).replace(' ','-')}-TPCDI"
default_wh         = f"{string.capwords(user_name).replace(' ','_')}_TPCDI"
min_dbr_version    = 14.1
invalid_dbr_list   = ['aarch64', 'ML', 'Snapshot', 'GPU', 'Photon', 'RC', 'Light', 'HLS', 'Beta', 'Latest']
features_or_perf   = ['Feature-Rich', 'Fastest Performance']
try: 
  lighthouse  = lh
except NameError: 
  lighthouse = is_lighthouse()

running_on_serverless = is_serverless()

if lighthouse:
  UC_enabled            = True
  cloud_provider        = 'AWS'
  serverless            = 'YES'
  default_catalog       = 'workspace'
  default_sf_options    = ['10', '100'] # Limited Scale Factor since 8-core driver will struggle to generate and also native XML lib will not be able to scale adequately for CustomerMgmt
else:
  default_sf_options    = ['10', '100', '1000', '5000', '10000']
  if running_on_serverless:
    UC_enabled            = True
    cloud_provider        = ''
    default_serverless    = 'YES'
  else:
    UC_enabled            = eval(string.capwords(spark.conf.get('spark.databricks.unityCatalog.enabled')))
    cloud_provider        = spark.conf.get('spark.databricks.cloudProvider') # "Azure", "GCP", or "AWS"
    default_serverless    = 'NO'
    
  node_types            = get_node_types()
  dbrs                  = get_dbr_versions(min_dbr_version)
  default_dbr_version   = list(dbrs.keys())[0]
  default_dbr           = list(dbrs.values())[0]
  worker_cores_mult     = 0.016
  if cloud_provider == 'AWS':
    default_worker_type = "m7gd.2xlarge"
    default_driver_type = "m7gd.xlarge"
    cust_mgmt_type      = "m7gd.16xlarge"
  elif cloud_provider == 'GCP':
    default_worker_type = "n2-standard-8"
    default_driver_type = "n2-standard-4"
    cust_mgmt_type      = "n2-standard-64"
    worker_cores_mult   = worker_cores_mult * 1.5
  elif cloud_provider == 'Azure':
    default_worker_type = "Standard_D8ads_v5" 
    default_driver_type = "Standard_D4as_v5"
    cust_mgmt_type      = "Standard_D64ads_v5"
  else:
    default_worker_type = (
      "Standard_D8ads_v5" if "Standard_D8ads_v5" in list(node_types.keys())
      else "n2-standard-8" if "n2-standard-8" in list(node_types.keys()) else "m7gd.2xlarge"
    )
    default_driver_type = (
      "Standard_D4as_v5" if "Standard_D4as_v5" in list(node_types.keys())
      else "n2-standard-4" if "n2-standard-4" in list(node_types.keys()) else "m7gd.xlarge"
    )
    cust_mgmt_type = (
      "Standard_D64ads_v5" if "Standard_D64ads_v5" in list(node_types.keys())
      else "n2-standard-64" if "n2-standard-64" in list(node_types.keys()) else "m7gd.16xlarge"
    )
  default_catalog = 'tpcdi' if UC_enabled else 'hive_metastore'

workflow_vals      = list(workflows_dict.values())
