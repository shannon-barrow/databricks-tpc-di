# Databricks TPC-DI

Databricks TPC-DI (Data Integration) is an implementation of specifications derived from the [TPC-DI](http://tpc.org/tpcdi/default5.asp) Benchmark.  
This repo includes multiple implementations and interpretations of the **TPC-DI v1.1.0**.  We suggest executing any of the workflow types on the Databricks Runtime **14.1** or higher. 

[![DatabricksRuntime](https://img.shields.io/badge/Databricks%20Runtime-14.1-orange)](https://docs.databricks.com/release-notes/runtime/releases.html)
[![Benchmark](https://img.shields.io/badge/Benchmark-TPC--DI%20v1.1.0-blue)](http://tpc.org/tpcdi/default5.asp)

## Summary
Historically, the process of synchronizing a decision support system with data from operational systems has been referred to as Extract, Transform, Load (ETL) and the tools supporting such process have been referred to as ETL tools. Recently, ETL was replaced by the more comprehensive acronym, data integration (DI). DI describes the process of extracting and combining data from a variety of data source formats, transforming that data into a unified data model representation and loading it into a data store. The TPC-DI benchmark combines and transforms data extracted from an On-Line Transaction Processing (OTLP) system along with other sources of data, and loads it into a data warehouse. The source and destination data models, data transformations and implementation rules have been designed to be broadly representative of modern data integration requirements.

* The current TPC-DI specification can be found on the [TPC Documentation Webpage](https://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp).  
* The current implementation follows the [TPC-DI v1.1.0 spec](https://www.tpc.org/TPC_Documents_Current_Versions/pdf/TPC-DI_v1.1.0.pdf). 

The TPC-DI spec does not provide code, instead relying on documenting business rules and requirements of the eventual table outputs of the data warehouse.   
This repo is an implementation of the spec only - one that executes only on the cloud-based Databricks platform.   

### LIMITATIONS
* The code base is implemented to execute successfully ONLY on the Databricks platform (GCP, AZURE, OR AWS).    
* This code requires:
  * Databricks E2 Deployment. 
  * Databricks Repos functionality. 
  * Access to GitHub via public internet access. 
* Serverless has been introduced as an optional execution mechanism but requires access to serverless compute
* Unity Catalog will be required to use a catalog outside of hive_metastore. Additionally, UC is a prerequisite for lineage and primary/foreign key constraints

### Notes on Scoring/Metrics and Publication
As of writing, the TPC-DI has not modified its submittal/scoring metrics to accomodate cloud-based platforms and infrastructure. Furthermore, as of writing, there has never been an official submission to this benchmark.  Therefore, no throughput metrics are mentioned or discussed in this code repository.  Databricks is attempting to work with the TPC to amend the rules for submission and modernize the throughput metrics for a future "official submittal" to be published by the TPC.  Until then, this code has been opened for demonstrating how such a Data Integration ETL application could be built upon the Databricks Lakehouse - without any performance metrics being published.

# Execution Mechanisms
The benchmark has been implemented in various ways to demonstrate the versatility of the Databricks Lakehouse platform.  Users can reference each implementation side-by-side to better enable them to build other similar data engineering pipelines:

1. Traditional Native Notebooks Workflow
2. Databricks DBSQL Warehouse based Workflow 
2. Databricks Delta Live Tables CORE
3. Databricks Delta Live Tables with SCD Type 1/ PRO: introduces easier handling of SCD TYPE 1/2 historical tracking using the new APPLY CHANGES INTO syntax
4. Databricks Delta Live Tables with DQ ADVANCED: introduces Data Quality metrics as well so data engineers can more easily handle bad data quality, analysts can more confidently trust their data, and support teams can more easily monitor the quality of data as it enters the Lakehouse.

There are 2 additional options that a user can toggle for **EACH** of the above deployment choices and they include:
1. **Scale Factor**: The value chosen correlates to HOW MUCH data will be processed. The total number of files/tables, the DAG of the workflow, etc do NOT change based on the scale factor size selected. What will be adjusted is the amount of data per file. In general the change in scale factor is reflected by a linear change in total rows/data size. For example, a scale factor of 10 aligns to roughly 1GB of raw data. It's default to 10 if you don't see the scaling factor. 
2. **Serverless**: choose faster performance and startup time with the Databricks SERVERLESS Workflows or SERVERLESS Delta Live Tables (which includes Enzyme as a standard feature)
3. **Collective Batch or Incremental Batches**: You could choose to run all batches in a single collective batch, or if you prefer to run the auditable version of the code that incrementally processes only 1 batch at a time then you will need to choose the 'Incremental Batches' option and make sure to choose a Workspace Cluster or DBSQL Warehouse execution flow type.
4. **Predictive Optimization**: Predictive Optimization removes the need to manually manage maintenance operations for Delta tables on Databricks. You could choose to enable or disable it. While enabled, Databricks automatically identifies tables that would benefit from maintenance operations and runs them for the user. Maintenance operations are only run as necessary, eliminating both unnecessary runs for maintenance operations and the burden associated with tracking and troubleshooting performance.
5. **Job Name & Target Database**: The job name and target database name has the pattern of [firstname]-[lastname]-TPCDI, you could change it to the preferred job name and target database name if required.  Your workflow type chosen will also be appended to this name upon creation to remove conflicts of generating multiple workflows for various execution mechanisms (i.e. cluster, DBSQL, DLT, etc)
6. **Various cluster options**: If you do not choose serverless you can adjust the DBR and worker/driver type. The dropdowns will automatically select the best default option BUT the widgets do allow flexibility in case you want to choose a different DBR or node type.


## Components of Execution
The TPC-DI has an initial "setup" step, followed by the benchmark execution.  

![Execution Journey](/src/tools/readme_images/tpcdi_execution_journey.png "Execution Journey")

### Step 1) Setup: Data Generation and Workflow Generation  
This portion is **NOT benchmarked**.  The sole purpose of this step is to:
1. Create the necessary raw data and store them in UNITY CATALOG
2. Create a Databricks Workflow (this created workflow is what executes the ingestion and transformations for the benchmark).  

#### Data Generation
- Data generation is a requisite step by TPC to create the required raw data in the various formats.  
- All Raw Data Generation is executed through the provided TPC ***DIGen.JAR*** Java executable file, provided in the repo to simplify the user experience.  To download the original source code from the TPC website, it can be found [here](https://www.tpc.org/TPC_Documents_Current_Versions/download_programs/tools-download-request5.asp?bm_type=TPC-DI&bm_vers=1.1.0&mode=CURRENT-ONLY)
- The number of files does not change with increasing scale factors - the size of the files increases as the scale factor increases
- The steps in the data generator are as follows:
  1) Copy the *TPC-provided JAR and dependencies* to the driver from the {repo_root}/src/tools/datagen folder
  2) Execute a python wrapper to the JAR, which generates all required files into the driver's /localdisk0/tmp/tpcdi folder
  3) Then move the generated files from the driver into a Unity Catalog volume inside the catalog designated. The default catalog created is named *tpcdi*.  Therefore the raw data volume location for this tpcdi catalog would be: */Volumes/tpcdi/tpcdi_raw_data/tpcdi_volume*

![Type of Raw Data Generated](/src/tools/readme_images/data_gen.png "Type of Raw Data Generated")  

##### IMPORTANT NOTES
- The Data Generation JAR was developed by TPC - it is out-of-scope for this effort, though it is required for the benchmark
- The data generation JAR is **NOT** a distributed spark step, or even very multi-threaded for that matter.  
  - This step may execute in less than a minute for scale_factor=10, or as long as 2 hours on scale_factor=10000.  
- Data generation executes on the driver, make sure you have a driver with local storage and enough memory to handle this type of data generation step. 
  - This shouldn't be a problem unless running data generation on scale factors over 1000. 
- The default setting for this implementation is to execute on a scale factor of 10 - which generates only 1GB of data in less than 1 minute - since the code/workflow are the same NO MATTER THE SCALE FACTOR.
- **If generating a large scale factor** (i.e. over 1000), via the workflow_builder:
  - We suggest a **storage-optimized single node cluster** to generate the raw data.
  - Please be patient with the execution as you may go several minutes without a log entry into the stdout of the cell when the very large files on large scale factors are being generated
  - The scale factor increase will ONLY affect how much data is generated, how long the job takes to execute, and how many resources it will take to benchmark in the subsequent workflow.  
  - Therefore we do not suggest increasing the scale factor unless you are trying to actually run benchmarks.
- To account for multiple users attempting to generate files repeatedly for a workspace, the data generation step will check to see if the requested scale factor raw data exists in Unity Catalog Volumes already. 
  - If it does (meaning you or someone else has executed the TPC-DI in this workspace already) then this step will be skipped - thus removing unnecessary data being stored and also removing the need to generate the data again.

#### Workflow Creation
The Databricks TPC-DI is developed in a way to make the execution simple and easy.  
- The TPC-DI Driver will provide simple default dropdown widgets. These widgets are populated based on your environment after running the setup command in the first cell.
  - Some are set to defaults that we suggest changing - such as workflow type and serverless.
  - Others are set to the suggested values and pre-populated with the actual options available.  These include DBR, Worker/Driver Type, Job Name, Target Catalog (we suggest only changing this one in cases where you don't have permissions to create a catalog), and target database
- If you prefer to change some values such as DBR, Worker/Driver Types, Scale Factor, etc, then the widgets give you the flexibility to do so.
  - Note that the widgets are dynamic and are added, removed, and populated based on your Databricks workspace configuration, cluster configuration, workflow type chosen, and whether Serverless is chosen.  For example, worker/driver type and DBR are removed as widgets if choosing serverless.
- Workflow creation should take less than 1 second.  When complete you will be given a link to open your workflow.

##### CLUSTER/WAREHOUSE SIZING
###### DBSQL Warehouse Size
- A ***SERVERLESS*** DBSQL Warehouse will **automatically** be generated for you, if one doesn't exist already.
- Additionally, to keep down on the number of warehouses created if multiple users will be launching this benchmark in the workspace, the warehouse names are generic and are intended to be shared across other users.
- All created warehouses are only as big as they need to be and are configured with an extremelely low auto-terminate value of 1 minute to keep costs as low as possible.
- The names and sizes are below:  
  | Scale Factor | Name and Size  |
  | ------------ | -------------- |
  | 10           | TPCDI_2X-Small |
  | 100          | TPCDI_2X-Small |
  | 1000         | TPCDI_Small    |
  | 5000         | TPCDI_Large    |
  | 10000        | TPCDI_X-Large  |

###### Cluster Size
*The following applies only when **not choosing SERVERLESS** as the compute choice. Serverless will automatically scale to meet the demands of the job*
- The workflow that is created by running these notebooks will size the cluster according to your scale factor and node type selected.  
- To reduce complexity in making this "generic" approach to creating a dynamic job, we abstracted away the sizing of the cluster (number of worker nodes).  Repeated tests have revealed the optimal number of cores per scale factor to achieve the BEST TCO (not necessarily the fastest job but the cheapest way to execute - which is the goal of the benchmark).  A single node cluster could handle all the way up to a scale factor of 5000, depending on the size of that node.  The chart is below.
    | Scale Factor | Total Raw Data | Optimal Worker Cores|
    | ------------ | -------------- | ------------------- |
    | 10           | .961 GB        | As low as 1         |
    | 100          | 9.66 GB        | As low as 2         |
    | 1000         | 97 GB          | 16                  |
    | 10000        | 970 GB         | 144                 |
- **IF** you truly want to change these values, we suggest building your workflow using the workflow_builder and then changing the cluster configuration from the workflow created.  

### Step 2) TPC-DI Workflow Execution
- After opening the link provided for your workflow, to begin execution click the **Run** button. To watch execution, open the new Job Run.  

# Footnote Disclaimer
As the Databricks implementation of the TPC-DI is an unpublished TPC Benchmark, the following is required legalese per TPC Fair Use Policy:  
*The Databricks TPC-DI is derived from the TPC-DI and as such is not comparable to published TPC-DI results.  The current scoring metrics for the TPC-DI preclude any official submission for cloud-based execution and therefore The Databricks TPC-DI can not OFFIALLY be submitted under current scoring metrics.  Databricks withholds the ability to submit an official submission to the TPC for this benchmark upon future revision of its scoring metrics. Prior to that, we maintain that this implementation follows all guidelines, rules, and audits required of the official TPC-DI specification.*
