# Databricks notebook source
pip install jinja2

# COMMAND ----------

import math

# COMMAND ----------

workflow_api_endpoint  = "/api/2.1/jobs/create"
pipeline_api_endpoint  = "/api/2.0/pipelines"
wh_api_endpoint        = "/api/2.0/sql/warehouses"
wh_config_api_endpoint = "/api/2.0/sql/config/warehouses"
DLT_PIPELINE_TEMPLATE  = "dlt_pipeline_jinja_template.json"
WORKFLOW_TEMPLATE      = "_workflow_jinja_template.json"
WAREHOUSE_TEMPLATE     = "warehouse_jinja_template.json"
wh_scale_factor_map    = {
  "10": "2X-Small", 
  "100": "2X-Small", 
  "1000": "Medium", 
  "5000": "X-Large",
  "10000": "2X-Large"
}

# COMMAND ----------

try: 
  json_templates_path = f"{workspace_src_path}/tools/jinja_templates/"
  sku = wf_key.split('-')
  exec_type = sku[0]
  worker_node_count = round(scale_factor * worker_cores_mult / node_types[worker_node_type]['num_cores'])
  dlt_worker_node_count = worker_node_count
  dlt_driver_node_type = driver_node_type
  dlt_worker_node_type = worker_node_type

  # DAG of args to send to Jinja
  dag_args = {
    "catalog":catalog, 
    "wh_target":wh_target, 
    "tpcdi_directory":tpcdi_directory, 
    "scale_factor":scale_factor, 
    "job_name":job_name, 
    "repo_src_path":repo_src_path,
    "cloud_provider":cloud_provider,
    "dbr":dbr_version_id,
    "exec_type":exec_type,
    "serverless":serverless
  }

  if serverless == 'YES':
    compute = "Serverless Workflow Selected"
    print(f"Serverless workflow selected.  If your workspace does not have access to Serverless then the workflow creation will potentially fail. Please select 'NO' for the Serverless widget if so.")
    worker_node_count = 0
    if scale_factor > 1000:
      worker_node_type = cust_mgmt_type
    driver_node_type = worker_node_type
  elif exec_type in ['DBT', 'ST_MVs', 'SQL_WH']:
    wh_size = wh_scale_factor_map[f"{scale_factor}"]
    wh_name = f"TPCDI_{wh_size}"
    dag_args['wh_name'] = wh_name
    dag_args['wh_size'] = wh_size
    worker_node_count = 0
    if scale_factor > 1000:
      worker_node_type = cust_mgmt_type
    driver_node_type = worker_node_type
    print(f"Your workflow type requires Databricks SQL. Serverless Warehouses are created by default.  If you do not have serverless SQL WHs available, please CREATE a non-serverless {wh_size} WH with the name '{wh_name}' and run this code again.")
    compute = f"""Warehouse Name:             {wh_name}
Warehouse Size:             {wh_size}"""
  elif exec_type in ['DLT']:
    if dlt_worker_node_count == 0:
      dlt_worker_node_count = 1
    if scale_factor > 1000:
      worker_node_type = cust_mgmt_type
    driver_node_type = worker_node_type
    dag_args['dlt_worker_node_type'] = dlt_worker_node_type
    dag_args['dlt_driver_node_type'] = dlt_driver_node_type
    dag_args['dlt_worker_node_count'] = dlt_worker_node_count
    compute = f"""Driver Type:                {dlt_driver_node_type}
Worker Type:                {dlt_worker_node_type}
Worker Count:               {dlt_worker_node_count}"""
  else:
    if worker_node_count == 0:
      driver_node_type = worker_node_type
    compute = f"""Driver Type:                {driver_node_type}
Worker Type:                {worker_node_type}
Worker Count:               {worker_node_count}
DBR Version:                {dbr_version_id}"""
  
  template_type = "dbsql_wh" if exec_type in ['ST_MVs', 'SQL_WH'] else exec_type
  dag_args['worker_node_type'] = worker_node_type
  dag_args['driver_node_type'] = driver_node_type
  dag_args['worker_node_count'] = worker_node_count
  
except NameError: 
  dbutils.notebook.exit(f"This notebook cannot be executed standalone and MUST be called from the workflow_builder notebook!")

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
  if exec_type == 'DLT':
    jinja_template_path = f"{json_templates_path}{DLT_PIPELINE_TEMPLATE}"
    dag_args['edition'] = sku[1]
    print(f"Rendering DLT Pipeline JSON via jinja template located at {jinja_template_path}")
    rendered_pipeline_dag = generate_dag(jinja_template_path, dag_args)
    print("Submitting rendered DLT Pipeline JSON to Databricks Pipelines API")
    dag_args['pipeline_id'] = submit_dag(rendered_pipeline_dag, pipeline_api_endpoint, 'pipeline')
  if exec_type in ['DBT', 'ST_MVs', 'SQL_WH']:
    dag_args['wh_id'] = get_warehouse_id()
  jinja_template_path = f"{json_templates_path}{template_type.lower()}{WORKFLOW_TEMPLATE}"
  print(f"Rendering New Workflow JSON via jinja template located at {jinja_template_path}")
  rendered_workflow_dag = generate_dag(jinja_template_path, dag_args)  
  print("Submitting rendered Workflow JSON to Databricks Jobs API")
  return submit_dag(rendered_workflow_dag, workflow_api_endpoint, 'job')

# COMMAND ----------

job_id = generate_workflow()
url = f"/#job/{job_id}"
displayHTML(f"<h1><a href={url}>Your Workflow can be found here!</a></h1>")
