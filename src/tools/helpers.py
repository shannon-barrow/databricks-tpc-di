import os
import shutil
import subprocess
import shlex
import requests
import concurrent.futures
import json
from pyspark.dbutils import DBUtils

DLT_PIPELINE_TEMPLATE = "dlt_pipeline_jinja_template.json"
WORKFLOW_TEMPLATE     = "_workflow_jinja_template.json"
DEBUG                 = True
          
def copy_directory(source_dir, target_dir, overwrite):
  if os.path.exists(target_dir) and overwrite:
    print(f"Overwrite set to true. Deleting: {target_dir}.")
    shutil.rmtree(target_dir)
    print(f"Deleted {target_dir}.")
  try:
    dst = shutil.copytree(source_dir, target_dir)
    print(f"Copied {source_dir} to {target_dir} succesfully!")
    return dst
  except FileExistsError:
    print(f"The folder you're trying to write to exists. Please delete it or set overwrite=True.")
  except FileNotFoundError:
    print(f"The folder you're trying to copy doesn't exist: {source_dir}")

def api_call(pat_token=None, json_payload=None, request_type=None, api_base_url=None, api_endpoint=None):
  headers = {'Content-type': 'application/json', 'Accept':'*/*', 'Authorization': f'Bearer {pat_token}'}
  if request_type == "POST":
    response = requests.post(f"{api_base_url}{api_endpoint}", json=json_payload, headers=headers)
  elif request_type == "GET":
    response = requests.get(f"{api_base_url}{api_endpoint}", json=json_payload, headers=headers)
  else:
    print(f"Invalid request type: {request_type}")
    return
  status_cd = response.status_code
  return response
    
def workflow_submit(pat_token, api_base_url, dag_dict):
  response = api_call(pat_token=pat_token, json_payload=dag_dict, request_type="POST", api_base_url=api_base_url, api_endpoint="/api/2.1/jobs/create")
  if response.status_code == 200:
    job_id = json.loads(response.text)['job_id']
    print(f"Job {job_id} created.")
    return job_id
  else: print(response.text)

def pipeline_submit(pat_token, api_base_url, dag_dict):
  response = api_call(pat_token=pat_token, json_payload=dag_dict, request_type="POST", api_base_url=api_base_url, api_endpoint="/api/2.0/pipelines")
  if response.status_code == 200:
    pipeline_id = json.loads(response.text)['pipeline_id']
    print(f"Pipeline {pipeline_id} created.")
    return pipeline_id
  else: print(response.text)

def generate_dag(template_path, dag_args):
  from jinja2 import Template
  dag_str = open(template_path, "r").read()
  rendered_dag = Template(dag_str).render(dag_args)
  print(f"Finished rendering JSON from {template_path}")
  return json.loads(rendered_dag)

def generate_workflow(wf_type, json_templates_path, dag_dict, token, api):
  sku = wf_type.split('-')
  if sku[0] == 'DLT':
    jinja_template_path = f"{json_templates_path}{DLT_PIPELINE_TEMPLATE}"
    dag_dict['edition'] = sku[1]
    print("Rendering DLT Pipeline JSON via jinja template located at {jinja_template_path}")
    rendered_pipeline_dag = generate_dag(jinja_template_path, dag_dict)
    if DEBUG: print(f"rendered_pipeline_dag: {rendered_pipeline_dag}")
    print("Submitting rendered DLT Pipeline JSON to Databricks Pipelines API")
    dag_dict['pipeline_id'] = pipeline_submit(token, api, rendered_pipeline_dag)
  jinja_template_path = f"{json_templates_path}{sku[0].lower()}{WORKFLOW_TEMPLATE}"
  print("Rendering New Workflow JSON via jinja template located at {jinja_template_path}")
  rendered_workflow_dag = generate_dag(jinja_template_path, dag_dict)  
  if DEBUG: print(f"rendered_workflow_dag: {rendered_workflow_dag}")
  print("Submitting rendered Workflow JSON to Databricks Jobs API")
  return workflow_submit(token, api, rendered_workflow_dag)

def get_node_types(pat_token, api_url):
  response = api_call(pat_token=pat_token, json_payload=None, request_type="GET", api_base_url=api_url, api_endpoint="/api/2.0/clusters/list-node-types")
  node_types = json.loads(response.text)
  node_type_ids = [] 
  for node_type in node_types['node_types']:
    node_type_ids.append(node_type['node_type_id'])
  return node_type_ids, node_types

def get_dbr_versions(TOKEN, API_URL):
  response = api_call(pat_token=TOKEN, json_payload=None, request_type="GET", api_base_url=API_URL, api_endpoint="/api/2.0/clusters/spark-versions")
  dbr_versions = json.loads(response.text)
  dbr_labels = [] 
  for dbr_type in dbr_versions['versions']: dbr_labels.append(dbr_type['name'])
  dbrs = {dbr['name']:dbr['key'] for dbr in dbr_versions['versions']}
  return dbrs, dbr_labels