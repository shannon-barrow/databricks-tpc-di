# Databricks notebook source
import os
import concurrent.futures
import requests
import shutil
import subprocess
import shlex

# COMMAND ----------



# COMMAND ----------

def move_file(source_location, target_location):
  shutil.copyfile(source_location, target_location)
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
    
# def generate_data():
#   DRIVER_ROOT      = "/local_disk0"
#   tpcdi_tmp_path   = "/tmp/tpcdi/"
#   driver_tmp_path  = f"{DRIVER_ROOT}{tpcdi_tmp_path}datagen/"
#   driver_out_path  = f"{DRIVER_ROOT}{tpcdi_tmp_path}sf={scale_factor}"
#   blob_out_path    = f"{tpcdi_directory}sf={scale_factor}"
#   if UC_enabled:  # Unity Catalog enabled so use VOLUMES to store raw files
#     os_blob_out_path = blob_out_path
#   else: # No Unity Catalog enabled so use DBFS to store raw files instead of volumes
#     os_blob_out_path = f"/dbfs{blob_out_path}"
#     blob_out_path = f"dbfs:{blob_out_path}" 

#   if os.path.exists(os_blob_out_path) and not FORCE_REWRITE:
#     print("Data generation skipped since raw data/directory already exists for this scale factor. If you want to force a rewrite, change the FORCE_REWRITE Flag")
#   else:
#     if FORCE_REWRITE:
#       print(f"Raw Data Directory {blob_out_path} Exists but overwriting generated data with new generated data per FORCE_REWRITE flag")
#     else: 
#       print(f"Raw Data Directory {blob_out_path} does not exist yet.  Proceeding to generate data for scale factor={scale_factor} into this directory")
#     copy_directory(f"{workspace_src_path}/tools/datagen", driver_tmp_path, overwrite=True)
#     print(f"Data generation for scale factor={scale_factor} is starting in directory: {driver_out_path}")
#     DIGen(driver_tmp_path, scale_factor, driver_out_path)
#     print(f"Data generation for scale factor={scale_factor} has completed in directory: {driver_out_path}")
#     print(f"Moving generated files from Driver directory {driver_out_path} to Storage directory {blob_out_path}")
#     if catalog != 'hive_metastore':
#       catalog_exists = spark.sql(f"SELECT count(*) FROM system.information_schema.tables WHERE table_catalog = '{catalog}'").first()[0] > 0
#       if not catalog_exists:
#         spark.sql(f"""CREATE CATALOG IF NOT EXISTS {catalog}""")
#         spark.sql(f"""GRANT ALL PRIVILEGES ON CATALOG {catalog} TO `account users`""")
#       spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.tpcdi_raw_data COMMENT 'Schema for TPC-DI Raw Files Volume'")
#       spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.tpcdi_raw_data.tpcdi_volume COMMENT 'TPC-DI Raw Files'")
#     filenames = [os.path.join(root, name) for root, dirs, files in os.walk(top=driver_out_path , topdown=True) for name in files]
#     dbutils.fs.mkdirs(blob_out_path)
#     for dir in next(os.walk(driver_out_path))[1]:
#       dbutils.fs.mkdirs(f"{blob_out_path}/{dir}")
#     with concurrent.futures.ThreadPoolExecutor(max_workers=sc.defaultParallelism) as executor:
#       futures = []
#       for filename in filenames:
#         futures.append(executor.submit(move_file, source_location=filename, target_location=filename.replace(driver_out_path, os_blob_out_path)))
#       for future in concurrent.futures.as_completed(futures):
#         try: print(future.result())
#         except requests.ConnectTimeout: print("ConnectTimeout.")

def generate_data():
  DRIVER_ROOT      = "/local_disk0"
  tpcdi_tmp_path   = "/tmp/tpcdi/"
  driver_tmp_path  = f"{DRIVER_ROOT}{tpcdi_tmp_path}datagen/"
  driver_out_path  = f"{DRIVER_ROOT}{tpcdi_tmp_path}sf={scale_factor}"
  blob_out_path    = f"{tpcdi_directory}sf={scale_factor}"

  
  catalog_exists = spark.sql(f"SELECT count(*) FROM system.information_schema.tables WHERE table_catalog = '{catalog}'").first()[0] > 0
  if not catalog_exists:
    spark.sql(f"""CREATE CATALOG IF NOT EXISTS {catalog}""")
  
  spark.sql(f"""GRANT ALL PRIVILEGES ON CATALOG {catalog} TO `account users`""")
  spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.tpcdi_raw_data COMMENT 'Schema for TPC-DI Raw Files Volume'")
  spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.tpcdi_raw_data.tpcdi_volume COMMENT 'TPC-DI Raw Files'")
  
  print(blob_out_path)

  if os.path.exists(blob_out_path):
    print("Data generation skipped since raw data/directory already exists for this scale factor. If you want to force a rewrite, change the FORCE_REWRITE Flag")

  else:
    print(f"Raw Data Directory {blob_out_path} does not exist yet.  Proceeding to generate data for scale factor={scale_factor} into this directory")
    dbutils.fs.mkdirs(blob_out_path)
    copy_directory(f"{workspace_src_path}/tools/datagen", driver_tmp_path, overwrite=True)
    print(f"Data generation for scale factor={scale_factor} is starting in directory: {driver_out_path}")
    DIGen(driver_tmp_path, scale_factor, blob_out_path)  # Modified: Provide the output path directly to DIGen
    print(f"Data generation for scale factor={scale_factor} has completed in directory: {driver_out_path}")

        
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
