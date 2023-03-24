# Databricks notebook source
from helpers import copy_directory
import os

# COMMAND ----------

DRIVER_ROOT = f"/local_disk0"

# COMMAND ----------

def move_file(source_location, target_location):
  dbutils.fs.mv(source_location, target_location) 
  return f"Finished moving {source_location} to {target_location}"

def generate_data(tpcdi_directory, repo_src_path, scale_factor, rewrite, threads):
  datagen_path     = f"{tpcdi_directory}datagen/"
  driver_tmp_path  = f"{DRIVER_ROOT}{datagen_path}"
  datagen_out_path = f"{tpcdi_directory}sf={scale_factor}"
  driver_out_path  = f"{DRIVER_ROOT}{datagen_out_path}"
  dbfs_out_path    = f"/dbfs{datagen_out_path}"

  if os.path.exists(dbfs_out_path) and not rewrite:
    print("Data generation skipped since raw data/directory already exists for this scale factor. If you want to force a rewrite, change the FORCE_REWRITE Flag")
  else:
    if rewrite:
      print(f"Raw Data Directory {dbfs_out_path} Exists but overwriting generated data with new generated data per FORCE_REWRITE flag")
    else: 
      print(f"Raw Data Directory {dbfs_out_path} does not exist yet.  Proceeding to generate data for scale factor={scale_factor} into this directory")
    copy_directory(f"/Workspace{repo_src_path}/tools/datagen", driver_tmp_path, overwrite=True)
    print(f"Data generation for scale factor={scale_factor} is starting in directory: {driver_out_path}")
    DIGen(driver_tmp_path, scale_factor, driver_out_path)
    print(f"Data generation for scale factor={scale_factor} has completed in directory: {driver_out_path}")
    
def DIGen(digen_path, scale_factor, output_path):
  cmd = f"java -jar {digen_path}DIGen.jar -sf {scale_factor} -o {output_path}"
  print(f"Generating data and outputting to {output_path}")
  args = shlex.split(cmd)
  p3 = subprocess.Popen(
    args,
    cwd=digen_path,
    #shell=True,
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
  
def generate_data(tpcdi_directory, repo_src_path, scale_factor, rewrite):
  datagen_path     = f"{tpcdi_directory}datagen/"
  driver_tmp_path  = f"{DRIVER_ROOT}{datagen_path}"
  datagen_out_path = f"{tpcdi_directory}sf={scale_factor}"
  driver_out_path  = f"{DRIVER_ROOT}{datagen_out_path}"
  dbfs_out_path    = f"/dbfs{datagen_out_path}"

  if os.path.exists(dbfs_out_path) and not rewrite:
    print("Data generation skipped since raw data/directory already exists for this scale factor. If you want to force a rewrite, change the FORCE_REWRITE Flag")
  else:
    if rewrite:
      print(f"Raw Data Directory {dbfs_out_path} Exists but overwriting generated data with new generated data per FORCE_REWRITE flag")
    else: 
      print(f"Raw Data Directory {dbfs_out_path} does not exist yet.  Proceeding to generate data for scale factor={scale_factor} into this directory")
    copy_directory(f"/Workspace{repo_src_path}/tools/datagen", driver_tmp_path, overwrite=True)
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

# COMMAND ----------

generate_data(tpcdi_directory, repo_src_path, scale_factor, FORCE_REWRITE)
