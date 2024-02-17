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

# COMMAND ----------

# DBTITLE 1,DBFS may not be accessible unless Cluster Access Mode set to "Single User" or "No Isolation Required"
UC_enabled = eval(string.capwords(spark.conf.get('spark.databricks.unityCatalog.enabled')))
tpcdi_directory = '/Volumes/tpcdi/tpcdi_raw_data/tpcdi_volume/' if UC_enabled else "/tmp/tpcdi/"
UC_mode = spark.conf.get("spark.databricks.clusterUsageTags.clusterUnityCatalogMode")
# if UC_enabled and UC_mode not in ['SINGLE_USER', 'NONE']:
#   dbutils.notebook.exit("DBFS is used to generate and store the raw geenerated date.  On Unity Catalog enabled clusters, DBFS may not be accessible unless Cluster Access Mode set to 'SINGLE_USER' or 'No Isolation Required'. Please execute on a cluster that will have access to generated files in DBFS using 'SINGLE_USER' or 'NONE' as the data_security_mode")

# COMMAND ----------

# ENV-Specific API calls and variables
repo_src_path      = f"{dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().split('/src')[0]}/src"
workspace_src_path = f"/Workspace{repo_src_path}"
API_URL = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None)
TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)
if API_URL is None or TOKEN is None: 
  dbutils.notebook.exit("Unable to capture API/Token from dbutils. Please try again or open a ticket")
user_name = spark.sql("select lower(regexp_replace(split(current_user(), '@')[0], '(\\\W+)', ' '))").collect()[0][0]
cloud_provider = spark.conf.get('spark.databricks.cloudProvider') # "Azure" or "AWS"
min_dbr_version     = 14.1
invalid_dbr_list    = ['aarch64', 'ML', 'Snapshot', 'GPU', 'Photon', 'RC', 'Light', 'HLS', 'Beta', 'Latest']
node_types          = get_node_types()
dbrs                = get_dbr_versions(min_dbr_version)

workflows_dict      = {
  "NATIVE": "Native Notebooks Workflow", 
  "DLT-CORE": "CORE Delta Live Tables Pipeline", 
  "DLT-PRO": "PRO Delta Live Tables Pipeline with SCD Type 1/2", 
  "DLT-ADVANCED": "ADVANCED Delta Live Tables Pipeline with DQ",
  "DBT": "dbt Core on DB SQL Warehouse",
  "STMV": "Streaming Tables and Materialized Views on DBSQL/DLT"
}

try: 
  default_workflow  = wf_type
except NameError: 
  default_workflow  = workflows_dict['NATIVE']
if default_workflow == '':
  raise Exception("Missing valid workflow type")

try: 
  default_serverless  = comp_type
except NameError: 
  default_serverless  = 'NO'

workflow_vals       = list(workflows_dict.values())
default_sf          = '10'
default_sf_options  = ['10', '100', '1000', '5000', '10000']
default_job_name    = f"{string.capwords(user_name).replace(' ','-')}-TPCDI"
default_wh          = f"{string.capwords(user_name).replace(' ','_')}_TPCDI"
default_catalog     = 'tpcdi' if UC_enabled else 'hive_metastore'
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
  raise Exception('Cloud Provider Unknown! Cannot determine whether AWS, GCP, or Azure')
  dbutils.notebook.exit('Cloud Provider Unknown! Cannot determine whether AWS, GCP, or Azure')
shuffle_part_mult   = 354
worker_cores_mult   = 0.0576
