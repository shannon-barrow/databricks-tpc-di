# Databricks notebook source
import requests
import string
import json
import collections

# COMMAND ----------

def api_call(json_payload=None, request_type=None, api_endpoint=None):
  headers = {'Content-type': 'application/json', 'Accept':'*/*', 'Authorization': f'Bearer {TOKEN}'}
  if request_type == "POST":
    response = requests.post(f"{API_URL}{api_endpoint}", json=json_payload, headers=headers)
  elif request_type == "GET":
    response = requests.get(f"{API_URL}{api_endpoint}", json=json_payload, headers=headers)
  else:
    print(f"Invalid request type: {request_type}")
    return
  status_cd = response.status_code
  return response

def get_node_types():
  response = api_call(json_payload=None, request_type="GET", api_endpoint="/api/2.0/clusters/list-node-types")
  node_types_list = json.loads(response.text)['node_types']
  node_types_dict = {}
  for node in node_types_list:
    node_type_id = node.pop('node_type_id')
    node_types_dict[node_type_id] = node
  return collections.OrderedDict(sorted(node_types_dict.items()))

def get_dbr_versions():
  response = api_call(json_payload=None, request_type="GET", api_endpoint="/api/2.0/clusters/spark-versions")
  dbr_versions_list = json.loads(response.text)['versions']
  dbr_versions_dict = {}
  for dbr in dbr_versions_list:
    if not any(invalid in dbr['name'] for invalid in invalid_dbr_list) and float(dbr['name'].split()[0]) > min_dbr_version:
      dbr_versions_dict[dbr['key']] = dbr['name']
  return collections.OrderedDict(sorted(dbr_versions_dict.items(), reverse=True))

# COMMAND ----------

# ENV-Specific API calls and variables
repo_src_path      = f"{dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().split('/src')[0]}/src"
workspace_src_path = f"/Workspace{repo_src_path}"
API_URL = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None)
TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)
if API_URL is None or TOKEN is None: 
  dbutils.notebook.exit("Unable to capture API/Token from dbutils. Please try again or open a ticket")
user_name = spark.sql("select current_user()").collect()[0][0].split("@")[0].replace(".","_")
cloud_provider = spark.conf.get('spark.databricks.cloudProvider') # "Azure" or "AWS"
min_dbr_version     = 10.0
invalid_dbr_list    = ['aarch64', 'ML', 'Snapshot', 'GPU', 'Photon', 'RC', 'Light', 'HLS', 'Beta', 'Latest']
node_types          = get_node_types()
dbrs                = get_dbr_versions()

workflows_dict      = {
  "NATIVE": "Native Notebooks Workflow", 
  "DLT-CORE": "CORE Delta Live Tables Pipeline", 
  "DLT-PRO": "PRO Delta Live Tables Pipeline with SCD Type 1/2", 
  "DLT-ADVANCED": "ADVANCED Delta Live Tables Pipeline with DQ"
}
try: 
  default_workflow = wf_type
except NameError: 
  default_workflow = workflows_dict['NATIVE']
workflow_vals       = list(workflows_dict.values())
default_sf          = '10'
default_job_name    = f"{string.capwords(user_name.replace('_',' ')).replace(' ','-')}-TPCDI"
if cloud_provider == 'AWS':
  default_worker_type = "m5d.2xlarge"
  default_driver_type = "m5d.xlarge"
elif cloud_provider == 'GCP':
  default_worker_type = "n2-standard-8"
  default_driver_type = "n2-standard-4"
elif cloud_provider == 'Azure':
  default_worker_type = "Standard_D8ads_v5" 
  default_driver_type = "Standard_D4as_v5"
else:
  dbutils.notebook.exit('Cloud Provider Unknown! Cannot determine whether AWS, GCP, or Azure')
shuffle_part_mult   = 354
worker_cores_mult   = 0.0576
