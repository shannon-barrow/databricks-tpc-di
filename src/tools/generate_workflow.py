# Databricks notebook source
import math

# COMMAND ----------

workflow_api_endpoint  = "/api/2.1/jobs/create"
pipeline_api_endpoint  = "/api/2.0/pipelines"
wh_api_endpoint        = "/api/2.0/sql/warehouses"
wh_config_api_endpoint = "/api/2.0/sql/config/warehouses"
DLT_PIPELINE_TEMPLATE  = "dlt_pipeline_jinja_template.json"
WORKFLOW_TEMPLATE      = "_workflow_jinja_template.json"
WAREHOUSE_TEMPLATE     = "warehouse_jinja_template.json"
CUST_MGMT_PART_RATIO   = 0.047
shuffle_partitions     = 'auto' 
wh_scale_factor_map    = {
  "10": "2X-Small", 
  "100": "2X-Small", 
  "1000": "Medium", 
  "5000": "X-Large",
  "10000": "2X-Large"
}

# COMMAND ----------

try: 
  # total_avail_memory = node_types[worker_node_type]['memory_mb'] if worker_node_count == 0 else node_types[worker_node_type]['memory_mb']*worker_node_count
  # total_cores = node_types[worker_node_type]['num_cores'] if worker_node_count == 0 else node_types[worker_node_type]['num_cores']*worker_node_count
  # shuffle_partitions = int(total_cores * max(1, shuffle_part_mult * scale_factor / total_avail_memory))
  json_templates_path = f"{workspace_src_path}/tools/jinja_templates/"
  sku = wf_key.split('-')
  worker_node_count = round(scale_factor * worker_cores_mult / node_types[worker_node_type]['num_cores']) if sku[0] in ['DLT', 'NATIVE'] else 0
  if worker_node_count == 0:
    if sku[0] == 'DLT':
      worker_node_count = 1
    else:
      driver_node_type  = worker_node_type
  if sku[0] == 'DBT':
    # TEMPORARILY removing the following as options for dbt until they work as expected: Unity Catalog, volumes, and DBR 13.0 
    # catalog = 'hive_metastore'
    # UC_enabled = False
    dbr_version_id = '13.0.x-scala2.12'
  # For tech summit only run on EXISTING INTERACTIVE CLUSTER
  #compute_key = 'compute_key' if serverless == 'YES' else 'job_cluster_key'
  compute_key = "existing_cluster_id"
  cust_mgmt_worker_count = round(CUST_MGMT_PART_RATIO * scale_factor / node_types[worker_node_type]['num_cores'])
  wh_size = wh_scale_factor_map[f"{scale_factor}"]
  #wh_name = f"TPCDI_{wh_size}"
  wh_name = "DBAcademy Warehouse"
  existing_cluster_id = spark.conf.get("spark.databricks.clusterUsageTags.clusterId")
  dlt_policy_id = -1
  if sku[0] == 'DLT':
    response = api_call(json_payload=None, request_type="GET", api_endpoint="/api/2.0/policies/clusters/list")
    policy_list = json.loads(response.text)['policies']
    for policy in policy_list:
        if policy['name'] == 'DBAcademy DLT':
          dlt_policy_id = policy['policy_id']
          print(f"DBAcademy DLT policy ID found: {dlt_policy_id}")
          break
except NameError: 
  dbutils.notebook.exit(f"This notebook cannot be executed standalone and MUST be called from the workflow_builder notebook!")

# DAG of args to send to Jinja
dag_args = {
  "serverless":serverless,
  "catalog":catalog, 
  "wh_target":wh_target, 
  "tpcdi_directory":tpcdi_directory, 
  "scale_factor":scale_factor, 
  "job_name":job_name, 
  "repo_src_path":repo_src_path,
  "cloud_provider":cloud_provider,
  "worker_node_type":worker_node_type,
  "driver_node_type":driver_node_type,
  "worker_node_count":worker_node_count,
  "dbr":dbr_version_id,
  "shuffle_partitions":shuffle_partitions,
  "compute_key":compute_key,
  "cust_mgmt_worker_count":cust_mgmt_worker_count,
  "wh_name":wh_name,
  "wh_size":wh_size,
  "existing_cluster_id":existing_cluster_id
 }

if sku[0] in ['DBT', 'STMV']:
  compute = f"""Warehouse Name:             {wh_name}
Warehouse Size:             {wh_size}"""
else:
  compute = f"""Existing Cluster ID:        {existing_cluster_id}"""
# Print out details of the workflow to user
print(f"""
Workflow Name:              {job_name}
Workflow Type:              {workflow_type}
{compute}
Target TPCDI Catalog:       {catalog}
Target TPCDI Database:      {wh_target}
TPCDI Staging Database:     {wh_target}_stage
Raw Files DBFS Path:        {tpcdi_directory}
Scale Factor:               {scale_factor}
""")

# COMMAND ----------

def generate_dag(template_path, dag_args):
  from jinja2 import Template
  dag_str = open(template_path, "r").read()
  rendered_dag = Template(dag_str).render(dag_args)
  print(f"Finished rendering JSON from {template_path}")
  return json.loads(rendered_dag)

def submit_dag(dag_dict, api_endpoint, dag_type):
  response = api_call(dag_dict, "POST", api_endpoint)
  if response.status_code == 200:
    response_id = json.loads(response.text)[f'{dag_type}_id']
    print(f"{dag_type} {response_id} created.")
    return response_id
  else: dbutils.notebook.exit(f"API call for {dag_type} Submission failed with status code {response.status_code}: {response.text}")

def get_warehouse_id():
  warehouse_id = -1
  response = api_call(request_type="GET", api_endpoint=wh_api_endpoint)
  warehouses_list = json.loads(response.text)['warehouses']
  for wh in warehouses_list:
    if wh['name'] == wh_name:
      warehouse_id = wh['id']
      print(f"DB SQL Warehouse {wh_name} exists! Warehouse ID: {warehouse_id}")
      break
  if warehouse_id == -1:
    print(f"Warehouse does not exist yet, creating new warehouse")
    rendered_wh_dag = generate_dag(f"{json_templates_path}{WAREHOUSE_TEMPLATE}", dag_args)
    response = api_call(rendered_wh_dag, "POST", wh_api_endpoint)
    warehouse_id = json.loads(response.text)['id']
    print(f"DB SQL Warehouse {wh_name} Created! Warehouse ID: {warehouse_id}")
  return warehouse_id

# COMMAND ----------

def generate_workflow():
  if sku[0] == 'DLT':
    jinja_template_path = f"{json_templates_path}{DLT_PIPELINE_TEMPLATE}"
    dag_args['edition'] = sku[1]
    dag_args['dlt_policy_id'] = dlt_policy_id
    print(f"Rendering DLT Pipeline JSON via jinja template located at {jinja_template_path}")
    rendered_pipeline_dag = generate_dag(jinja_template_path, dag_args)
    print("Submitting rendered DLT Pipeline JSON to Databricks Pipelines API")
    dag_args['pipeline_id'] = submit_dag(rendered_pipeline_dag, pipeline_api_endpoint, 'pipeline')
  if sku[0] in ['DBT', 'STMV']:
    dag_args['wh_id'] = get_warehouse_id()
    dag_args['table_or_st'] = "STREAMING TABLE"
    dag_args['table_or_mv'] = "MATERIALIZED VIEW"
  jinja_template_path = f"{json_templates_path}{sku[0].lower()}{WORKFLOW_TEMPLATE}"
  print(f"Rendering New Workflow JSON via jinja template located at {jinja_template_path}")
  rendered_workflow_dag = generate_dag(jinja_template_path, dag_args)  
  print("Submitting rendered Workflow JSON to Databricks Jobs API")
  return submit_dag(rendered_workflow_dag, workflow_api_endpoint, 'job')

# COMMAND ----------

job_id = generate_workflow()
url = f"/#job/{job_id}"
displayHTML(f"<h1><a href={url}>Your Workflow can be found here!</a></h1>")
