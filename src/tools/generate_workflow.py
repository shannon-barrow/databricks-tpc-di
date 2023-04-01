# Databricks notebook source
import math

# COMMAND ----------

DLT_PIPELINE_TEMPLATE = "dlt_pipeline_jinja_template.json"
WORKFLOW_TEMPLATE     = "_workflow_jinja_template.json"
CUST_MGMT_PART_RATIO  = 0.047
workflow_api_endpoint = "/api/2.1/jobs/create"
pipeline_api_endpoint = "/api/2.0/pipelines"

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
  else: print(response.text)

# COMMAND ----------

def generate_workflow():
  json_templates_path = f"{workspace_src_path}/tools/jinja_templates/"
  sku = wf_key.split('-')
  if sku[0] == 'DLT':
    jinja_template_path = f"{json_templates_path}{DLT_PIPELINE_TEMPLATE}"
    dag_args['edition'] = sku[1]
    dag_args['cust_mgmt_worker_count'] = round(CUST_MGMT_PART_RATIO * dag_args['scale_factor'] / node_types[worker_node_type]['num_cores'])
    dag_args['cust_mgmt_partitions'] = int(node_types[worker_node_type]['num_cores']) if dag_args['cust_mgmt_worker_count'] == 0 else int(dag_args['cust_mgmt_worker_count'] * node_types[worker_node_type]['num_cores'])
    print("Rendering DLT Pipeline JSON via jinja template located at {jinja_template_path}")
    rendered_pipeline_dag = generate_dag(jinja_template_path, dag_args)
    #if DEBUG: print(f"rendered_pipeline_dag: {rendered_pipeline_dag}")
    print("Submitting rendered DLT Pipeline JSON to Databricks Pipelines API")
    dag_args['pipeline_id'] = submit_dag(rendered_pipeline_dag, pipeline_api_endpoint, 'pipeline')
  jinja_template_path = f"{json_templates_path}{sku[0].lower()}{WORKFLOW_TEMPLATE}"
  print("Rendering New Workflow JSON via jinja template located at {jinja_template_path}")
  rendered_workflow_dag = generate_dag(jinja_template_path, dag_args)  
  #if DEBUG: print(f"rendered_workflow_dag: {rendered_workflow_dag}")
  print("Submitting rendered Workflow JSON to Databricks Jobs API")
  return submit_dag(rendered_workflow_dag, workflow_api_endpoint, 'job')

# COMMAND ----------

job_id = generate_workflow()
url = f"/#job/{job_id}"
displayHTML(f"<h1><a href={url}>Your Workflow can be found here!</a></h1>")
