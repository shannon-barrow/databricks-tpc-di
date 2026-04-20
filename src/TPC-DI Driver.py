# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Welcome to the TPC-DI Spec Implementation on Databricks!
# MAGIC
# MAGIC ## Please refer to the README for additional documentation!
# MAGIC
# MAGIC ### To Execute the benchmark please follow these instructions:  
# MAGIC 1. **FIRST**, execute the first 2 cells after this initial documentation cell (the setup cell below and the widget generation cell).  
# MAGIC 2. **THEN**, choose from the widgets above, starting with the WORKFLOW TYPE
# MAGIC
# MAGIC ### Workflow Types:
# MAGIC - CLUSTER-based Workflow with Structured Streaming Notebooks  
# MAGIC   - Leverage traditional cluster and do an incremental (benchmarked) run OR execute all batches in one pass. 
# MAGIC - DBSQL WAREHOUSE-based Workflow with Structured Streaming Notebooks   
# MAGIC   - Same as cluster approached above BUT against a DBSQL Warehouse. Do an incremental (benchmarked) run OR execute all batches in one pass. 
# MAGIC - Delta Live Tables Pipeline: CORE Sku   
# MAGIC   - Leverage DLT for simplified, cost-effective ETL pipelines 
# MAGIC - Delta Live Tables Pipeline with SCD Type 1/2: PRO Sku   
# MAGIC   - Leverage APPLY CHANGES INTO to Simplify Slowly Changing Dimensions Ingestion, with both Type 1 and Type 2 SCD. 
# MAGIC - Delta Live Tables Pipeline with DQ: ADVANCED Sku   
# MAGIC   - Easily add in Data Quality to your pipeline to either limit ingestion of poor data or gain insights into your DQ - or both!  
# MAGIC
# MAGIC ### Other widget options include:
# MAGIC - **Scale Factor**:  
# MAGIC         The value chosen correlates to HOW MUCH data will be processed.  The total number of files/tables, the DAG of the workflow, etc do NOT change based on the scale factor size selected.  What will be adjusted is the amount of data per file.  In general the change in scale factor is reflected by a linear change in total rows/data size. For example, a scale factor of 10 aligns to roughly 1GB of raw data. It's default to 10 if you don't see the scaling factor.
# MAGIC - **Serverless**:  
# MAGIC         Serverless workflows and DLT are in preview! Ensure your workspace is configured to leverage the serverless preview otherwise this notebook will fail to generate a workflow for you.
# MAGIC - **Collective Batch or Incremental Batches**:  
# MAGIC         The "formal" benchmark requires audit checks in between batches.  However, dbt, Delta Live Tables, and Streaming Tables/Materialized Views do NOT support an incremental batch nature in which the job stops to do "official audit checks" since they are more declarative in nature and ingest ALL available files at once.  Therefore the fastest and preferred method to run this for unofficial use is to run all batches in a single batch.  If you prefer to run the auditable version of the code that incrementally processes only 1 batch at a time then you will need to choose the 'Incremental Batches' option and make sure to choose a Workspace Cluster or DBSQL Warehouse execution flow type.  
# MAGIC - **Predictive Optimization**:  
# MAGIC         Predictive Optimization removes the need to manually manage maintenance operations for Delta tables on Databricks. You could choose to enable or disable it. With predictive optimization enabled, Databricks automatically identifies tables that would benefit from maintenance operations and runs them for the user. Maintenance operations are only run as necessary, eliminating both unnecessary runs for maintenance operations and the burden associated with tracking and troubleshooting performance.
# MAGIC - **Job Name & Target Database**:  
# MAGIC         The job name and target database name has the pattern of [firstname]-[lastname]-TPCDI,  you could change it to the preferred job name and target database name if required.
# MAGIC - **Various cluster options**:  
# MAGIC         If you do not choose serverless you can adjust the DBR and worker/driver type.  The dropdowns will automatically select the best default option BUT the widgets do allow flexibility in case you want to choose a different DBR or node type.
# MAGIC - **Optimize For UC Features or Fastest Performance**:
# MAGIC         In some cases this benchmark is used to test Databricks SKUs against one another. In which case, feel free to leave as 'Feature-Rich'.  However, in other cases this benchmark is used to compare Databricks against competitive platforms that can not or will not implement the same features that optimize the downstream user experience and performance. To ensure these competitive benchmarks test platforms on a 'like-to-like' set of features, choose 'Fastest Performance' in this widget. Choosing this removes some of the constraints that are checked at write time (which can adversely affect performance to a small degree), removes optimized writes (which coalesces small files into bigger files that improves downstream consumption), and removes some other constraint checks that help enable PK/FK relationship.  In normal production scenarios we encourag customers to leverage these Delta and Unity Catalog features.  Only choose to disable them here when comparing against competitive platforms that cannot or will not also perform the same optimizations which improve user experience at the cost of some ETL performance.
# MAGIC
# MAGIC ### Many options are configurable from the workflow_builder EXCEPT the cluster or warehouse size.
# MAGIC - Serverless clusters/DLT pipelines are dynamically autoscaled
# MAGIC - Clusters and warehouses are sized to achieve the best TCO based on the scale factor chosen
# MAGIC - If you prefer changing the cluster/WH size for your own testing then it can be manually modified from the Workflow created by this notebook
# MAGIC
# MAGIC **NOTE**: The following execution types have been removed, at least temporarily:
# MAGIC 1. <a href="https://docs.getdbt.com/" target="_blank">dbt Core</a>  
# MAGIC 2. <a href="https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-streaming-table.html" target="_blank">Streaming Tables</a> and <a href="https://docs.databricks.com/en/sql/user/materialized-views.html" target="_blank">Materialized Views</a>

# COMMAND ----------

# DBTITLE 1,Setup: bootstrap tpcdi_config + import workflow-generator functions
# MAGIC %run ./tools/setup

# COMMAND ----------

# DBTITLE 1,Declare Widgets and Assign to Variables EXCEPT Worker Count
dbutils.widgets.dropdown("workflow_type", tpcdi_config.default_workflow, tpcdi_config.workflow_vals, "Workflow Type")
dbutils.widgets.dropdown("batched", 'Single Collective Batch', ['Single Collective Batch', 'Incremental Batches'], "Collective batch or incremental batches")
dbutils.widgets.dropdown("pred_opt", "DISABLE", ["ENABLE", "DISABLE"], "Predictive Optimization")
dbutils.widgets.dropdown("scale_factor", tpcdi_config.default_sf, tpcdi_config.default_sf_options, "Scale factor")
dbutils.widgets.text("job_name", tpcdi_config.default_job_name, "Job Name")
dbutils.widgets.text("wh_target", tpcdi_config.default_wh, 'Target Database')
dbutils.widgets.text("catalog", tpcdi_config.default_catalog, 'Target Catalog')
dbutils.widgets.dropdown("perf_or_features", tpcdi_config.features_or_perf[0], tpcdi_config.features_or_perf, 'Optimize For UC Features or Fastest Performance')
dbutils.widgets.dropdown("regenerate_data", "NO", ["YES", "NO"], "Regenerate Data")

perf_opt_flg      = True if dbutils.widgets.get("perf_or_features") == tpcdi_config.features_or_perf[1] else False
catalog           = dbutils.widgets.get("catalog")
scale_factor      = int(dbutils.widgets.get("scale_factor"))
workflow_type     = dbutils.widgets.get('workflow_type')
pred_opt          = dbutils.widgets.get('pred_opt')
wh_target         = dbutils.widgets.get("wh_target")
wf_key            = list(tpcdi_config.workflows_dict)[tpcdi_config.workflow_vals.index(workflow_type)]
sku               = wf_key.split('-')
job_name          = f"{dbutils.widgets.get('job_name')}-SF{scale_factor}-{wf_key}"
incremental       = True if dbutils.widgets.get("batched") == 'Incremental Batches' else False
tpcdi_directory   = f'/Volumes/{catalog}/tpcdi_raw_data/tpcdi_volume/'

dbutils.widgets.dropdown("serverless", tpcdi_config.default_serverless, ['YES', 'NO'], "Enable Serverless")
dbutils.widgets.dropdown("worker_type", tpcdi_config.default_worker_type, list(tpcdi_config.node_types.keys()), "Worker Type")
dbutils.widgets.dropdown("driver_type", tpcdi_config.default_driver_type, list(tpcdi_config.node_types.keys()), "Driver Type")
dbutils.widgets.dropdown("dbr", tpcdi_config.default_dbr, list(tpcdi_config.dbrs.values()), "Databricks Runtime")
serverless        = 'YES' if sku[0] not in ['CLUSTER','DLT'] else dbutils.widgets.get('serverless')
worker_node_type  = dbutils.widgets.get("worker_type")
driver_node_type  = dbutils.widgets.get("driver_type")
dbr_version_id    = list(tpcdi_config.dbrs.keys())[list(tpcdi_config.dbrs.values()).index(dbutils.widgets.get("dbr"))]

if serverless == 'YES':
  dbutils.widgets.remove('worker_type')
  dbutils.widgets.remove('driver_type')
  dbutils.widgets.remove('dbr')

if sku[0] in ['DBSQL']:
  dbutils.widgets.remove('serverless')

if sku[0] not in ['CLUSTER','DBSQL']:
  dbutils.widgets.remove('batched')
  dbutils.widgets.remove('perf_or_features')
  incremental = False

# COMMAND ----------

# DBTITLE 1,Create the Data Generation Workflow (serverless, single notebook task)
datagen_job_id = generate_datagen_workflow(
    job_name=job_name,
    scale_factor=scale_factor,
    catalog=catalog,
    regenerate_data=dbutils.widgets.get("regenerate_data"),
    log_level="INFO",
    repo_src_path=tpcdi_config.repo_src_path,
    workspace_src_path=tpcdi_config.workspace_src_path,
    api_call=tpcdi_config.api_call,
)
displayHTML(f"<h2><a href=/#job/{datagen_job_id}>Data Generation Workflow</a></h2>")

# COMMAND ----------

# DBTITLE 1,Create the Benchmark Workflow (ingest + dim/fact pipeline)
benchmark_job_id = generate_benchmark_workflow(
    wf_key=wf_key,
    workflow_type=workflow_type,
    job_name=job_name,
    catalog=catalog,
    wh_target=wh_target,
    scale_factor=scale_factor,
    tpcdi_directory=tpcdi_directory,
    repo_src_path=tpcdi_config.repo_src_path,
    workspace_src_path=tpcdi_config.workspace_src_path,
    cloud_provider=tpcdi_config.cloud_provider,
    serverless=serverless,
    pred_opt=pred_opt,
    perf_opt_flg=perf_opt_flg,
    incremental=incremental,
    api_call=tpcdi_config.api_call,
    worker_node_type=worker_node_type if serverless != "YES" else None,
    driver_node_type=driver_node_type if serverless != "YES" else None,
    dbr_version_id=dbr_version_id if serverless != "YES" else None,
    default_dbr_version=tpcdi_config.default_dbr_version,
    default_worker_type=tpcdi_config.default_worker_type,
    cust_mgmt_type=tpcdi_config.cust_mgmt_type,
    worker_cores_mult=tpcdi_config.worker_cores_mult,
    node_types=tpcdi_config.node_types,
)
displayHTML(f"<h2><a href=/#job/{benchmark_job_id}>Benchmark Workflow</a></h2>")