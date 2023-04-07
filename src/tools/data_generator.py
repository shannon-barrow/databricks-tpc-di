# Databricks notebook source
import os
import concurrent.futures
import requests
import shutil
import subprocess
import shlex

# COMMAND ----------

DRIVER_ROOT = f"/local_disk0"
try: 
  total_avail_memory = node_types[worker_node_type]['memory_mb'] if worker_node_count == 0 else node_types[worker_node_type]['memory_mb']*worker_node_count
  total_cores = node_types[worker_node_type]['num_cores'] if worker_node_count == 0 else node_types[worker_node_type]['num_cores']*worker_node_count
  shuffle_partitions = int(total_cores * max(1, shuffle_part_mult * scale_factor / total_avail_memory))
except NameError: 
  dbutils.notebook.exit(f"This notebook cannot be executed standalone and MUST be called from the workflow_builder notebook!")

# DAG of args to send to Jinja
dag_args = {
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
  "shuffle_partitions":shuffle_partitions
 }

# Print out details of the workflow to user
print(f"""
Workflow Name:              {dag_args['job_name']}
Workflow Type:              {workflow_type}
Target Warehouse Database:  {wh_target}_wh
Warehouse Staging Database: {wh_target}_stage
Raw Files DBFS Path:        {dag_args['tpcdi_directory']}
Scale Factor:               {dag_args['scale_factor']}
Driver Type:                {dag_args['driver_node_type']}
Worker Type:                {dag_args['worker_node_type']}
Worker Count:               {dag_args['worker_node_count']}
DBR Version:                {dag_args['dbr']}
Default Shuffle Partitions: {dag_args['shuffle_partitions']}
""")

# COMMAND ----------

def move_file(source_location, target_location):
  dbutils.fs.cp(source_location, target_location) 
  return f"Finished moving {source_location} to {target_location}"

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
    
def generate_data():
  datagen_path     = f"{tpcdi_directory}datagen/"
  driver_tmp_path  = f"{DRIVER_ROOT}{datagen_path}"
  datagen_out_path = f"{tpcdi_directory}sf={scale_factor}"
  driver_out_path  = f"{DRIVER_ROOT}{datagen_out_path}"
  dbfs_out_path    = f"/dbfs{datagen_out_path}"

  if os.path.exists(dbfs_out_path) and not FORCE_REWRITE:
    print("Data generation skipped since raw data/directory already exists for this scale factor. If you want to force a rewrite, change the FORCE_REWRITE Flag")
  else:
    if FORCE_REWRITE:
      print(f"Raw Data Directory {dbfs_out_path} Exists but overwriting generated data with new generated data per FORCE_REWRITE flag")
    else: 
      print(f"Raw Data Directory {dbfs_out_path} does not exist yet.  Proceeding to generate data for scale factor={scale_factor} into this directory")
    copy_directory(f"{workspace_src_path}/tools/datagen", driver_tmp_path, overwrite=True)
    print(f"Data generation for scale factor={scale_factor} is starting in directory: {driver_out_path}")
    DIGen(driver_tmp_path, scale_factor, driver_out_path)
    print(f"Data generation for scale factor={scale_factor} has completed in directory: {driver_out_path}")
    print(f"Moving generated files from Driver directory {driver_out_path} to DBFS directory {dbfs_out_path}")
    filenames = [os.path.join(root, name) for root, dirs, files in os.walk(top=driver_out_path , topdown=False) for name in files]
    with concurrent.futures.ThreadPoolExecutor(max_workers=sc.defaultParallelism) as executor:
      futures = []
      for filename in filenames:
        futures.append(executor.submit(move_file, source_location=f"file:{filename}", target_location=f"dbfs:{filename.replace(DRIVER_ROOT, '')}"))
      for future in concurrent.futures.as_completed(futures):
        try: print(future.result())
        except requests.ConnectTimeout: print("ConnectTimeout.")
        
def DIGen(digen_path, scale_factor, output_path):
  cmd = f"java -jar {digen_path}DIGen.jar -sf {scale_factor} -o {output_path}"
  print(f"Generating data and outputting to {output_path}")
  args = shlex.split(cmd)
  p3 = subprocess.Popen(
    args,
    cwd=digen_path,
    universal_newlines=True,
    stdin=subprocess.PIPE,
    stdout=subprocess.PIPE, 
    stderr=subprocess.PIPE)
  p3.stdin.write("\n")
  p3.stdin.flush()
  p3.stdin.write("YES\n")
  p3.stdin.flush()
  while True:
    output = p3.stdout.readline()
    if p3.poll() is not None and output == '':
      break
    if output:
      print (output.strip())
  p3.wait()

# COMMAND ----------

generate_data()
