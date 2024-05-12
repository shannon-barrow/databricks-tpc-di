# Databricks notebook source
try: 
  wh_scale_factor_map    = {
    "10": "2X-Small", 
    "100": "2X-Small", 
    "1000": "Medium", 
    "5000": "X-Large",
    "10000": "2X-Large"
  }
  workflow_api_endpoint  = "/api/2.1/jobs/create"
  pipeline_api_endpoint  = "/api/2.0/pipelines"
  wh_api_endpoint        = "/api/2.0/sql/warehouses"
  wh_config_api_endpoint = "/api/2.0/sql/config/warehouses"
  DLT_PIPELINE_TEMPLATE  = "dlt_pipeline_jinja_template.json" 
  if incremental:
    WORKFLOW_TEMPLATE    = "_incremental_workflow_jinja_template.json"
    print("Each of the 3 TPC-DI batches will be executed incrementally with batches 2 and 3 performing merges.")
  else:
    WORKFLOW_TEMPLATE    = "_workflow_jinja_template.json"
    print("All 3 TPC-DI batches will be executed in a single job batch.")
  WAREHOUSE_TEMPLATE     = "warehouse_jinja_template.json"
  json_templates_path    = f"{workspace_src_path}/tools/jinja_templates/"
  template_type          = "dbsql" if sku[0] == 'STMV' else sku[0]
  exec_folder            = "SQL" if sku[0] == 'DBSQL' else sku[0]

  # DAG of args to send to Jinja
  dag_args = {
    "catalog":catalog, 
    "wh_target":wh_target, 
    "tpcdi_directory":tpcdi_directory, 
    "scale_factor":scale_factor, 
    "job_name":job_name, 
    "repo_src_path":repo_src_path,
    "cloud_provider":cloud_provider,
    "exec_type":sku[0],
    "serverless":serverless,
    "pred_opt":pred_opt,
    "exec_folder":exec_folder
  }

  if sku[0] == 'CLUSTER' and serverless != 'YES':
    worker_node_count = round(scale_factor * worker_cores_mult / node_types[worker_node_type]['num_cores'])
    if worker_node_count == 0:
      driver_node_type = worker_node_type
    dag_args['worker_node_type'] = worker_node_type
    dag_args['driver_node_type'] = driver_node_type
    dag_args['worker_node_count'] = worker_node_count
    dag_args['dbr'] = dbr_version_id
    compute = f"""Driver Type:              {driver_node_type}\nWorker Type:              {worker_node_type}\nWorker Count:             {worker_node_count}\nDBR Version:              {dbr_version_id}"""
  else:
    dag_args['dbr'] = default_dbr_version
    dag_args['driver_node_type'] = cust_mgmt_type if scale_factor > 1000 else default_worker_type
    dag_args['worker_node_type'] = cust_mgmt_type if scale_factor > 1000 else default_worker_type
    dag_args['worker_node_count'] = 0
  if sku[0] in ['DBT', 'STMV', 'DBSQL']:
    wh_size = wh_scale_factor_map[f"{scale_factor}"]
    wh_name = f"TPCDI_{wh_size}"
    dag_args['wh_name'] = wh_name
    dag_args['wh_size'] = wh_size    
    print(f"Your workflow type requires Databricks SQL. \nServerless Warehouses are created by default. \nIf you do not have serverless SQL WHs available, please CREATE a non-serverless {wh_size} WH with the name '{wh_name}' and run this code again.")
    compute = f"""Warehouse Name:           {wh_name}\nWarehouse Size:           {wh_size}"""
  elif serverless == 'YES':
    compute = f"Serverless {sku[0]} selected. \nIf your workspace does not have access to Serverless then the workflow creation will potentially fail. \nIf workflow creation fails for this reason please select 'NO' for the Serverless widget."
  elif sku[0] in ['DLT']:
    dlt_worker_node_count = round(scale_factor * worker_cores_mult / node_types[worker_node_type]['num_cores'])
    if dlt_worker_node_count == 0: dlt_worker_node_count = 1
    dag_args['dlt_worker_node_type'] = worker_node_type
    dag_args['dlt_driver_node_type'] = driver_node_type
    dag_args['dlt_worker_node_count'] = dlt_worker_node_count
    compute = f"""Driver Type:              {driver_node_type}\nWorker Type:              {worker_node_type}\nWorker Count:             {dlt_worker_node_count}"""     
except NameError: 
  dbutils.notebook.exit(f"This notebook cannot be executed standalone and MUST be called from the workflow_builder notebook!")

# Print out details of the workflow to user
print(f"""
Workflow Name:            {job_name}
Workflow Type:            {workflow_type}
{compute}
Target TPCDI Catalog:     {catalog}
Target TPCDI Database:    {wh_target}
TPCDI Staging Database:   {wh_target}_stage
Raw Files Path:           {tpcdi_directory}
Scale Factor:             {scale_factor}
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
    print(f"Rendering DLT Pipeline JSON via jinja template located at {jinja_template_path}")
    rendered_pipeline_dag = generate_dag(jinja_template_path, dag_args)
    print("Submitting rendered DLT Pipeline JSON to Databricks Pipelines API")
    dag_args['pipeline_id'] = submit_dag(rendered_pipeline_dag, pipeline_api_endpoint, 'pipeline')
  if sku[0] in ['DBT', 'STMV', 'DBSQL']:
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
