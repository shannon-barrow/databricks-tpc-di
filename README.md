# Databricks TPC-DI

Databricks TPC-DI (Data Integration) is an implementation of specifications derived from the [TPC-DI](http://tpc.org/tpcdi/default5.asp) Benchmark.  
This repo includes multiple implementations and interpretations of the **TPC-DI v1.1.0**.  We suggest executing any of the workflow types on the Databricks Runtime **10.4 LTS** or higher. 

[![DatabricksRuntime](https://img.shields.io/badge/Databricks%20Runtime-10.4%20LTS-orange)](https://docs.databricks.com/release-notes/runtime/releases.html)
[![Benchmark](https://img.shields.io/badge/Benchmark-TPC--DI%20v1.1.0-blue)](http://tpc.org/tpcdi/default5.asp)

## Summary
Historically, the process of synchronizing a decision support system with data from operational systems has been referred to as Extract, Transform, Load (ETL) and the tools supporting such process have been referred to as ETL tools. Recently, ETL was replaced by the more comprehensive acronym, data integration (DI). DI describes the process of extracting and combining data from a variety of data source formats, transforming that data into a unified data model representation and loading it into a data store. The TPC-DI benchmark combines and transforms data extracted from an On-Line Transaction Processing (OTLP) system along with other sources of data, and loads it into a data warehouse. The source and destination data models, data transformations and implementation rules have been designed to be broadly representative of modern data integration requirements.

* The current TPC-DI specification can be found on the [TPC Documentation Webpage](https://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp).  
* The current implementation follows the [TPC-DI v1.1.0 spec](https://www.tpc.org/TPC_Documents_Current_Versions/pdf/TPC-DI_v1.1.0.pdf). 

The TPC-DI spec does not provide code, instead relying on documenting business rules and requirements of the eventual table outputs of the data warehouse.   
This repo is an implementation of the spec only - one that executes only on the cloud-based Databricks platform.  

### Notes on Scoring/Metrics and Publication
As of writing, the TPC-DI has not modified its submittal/scoring metrics to accomodate cloud-based platforms and infrastructure. Furthermore, as of writing, there has never been an official submission to this benchmark.  Therefore, no throughput metrics are mentioned or discussed in this code repository.  Databricks is attempting to work with the TPC to amend the rules for submission and modernize the throughput metrics for a future "official submittal" to be published by the TPC.  Until then, this code has been opened for demonstrating how such a Data Integration ETL application could be built upon the Databricks Lakehouse - without any performance metrics being published.

# Execution Mechanisms
The benchmark has been implemented in various ways to demonstrate the versatility of the Databricks Lakehouse platform, and so users can reference such implementations side-by-side to better enable them to build other similar data engineering pipelines:

1. Traditional Notebooks Workflow
2. Databricks Delta Live Tables CORE
3. Databricks Delta Live Tables PRO: introduces easier handling of SCD TYPE 1/2 historical tracking using the new APPLY CHANGES INTO syntax
4. Databricks Delta Live Tables ADVANCED: introduces Data Quality metrics as well so data engineers can more easily handle bad data quality, analysts can more confidently trust their data, and support teams can more easily monitor the quality of data as it enters the Lakehouse.

## Components of Execution
The TPC-DI has an initial "setup" step, followed by the benchmark execution.  

### Step 1) Setup: Data Generation and Workflow Generation  
This portion is **NOT benchmarked**.  The sole purpose of this step is to:
1. Create the necessary raw data and store them in DBFS
2. Create a Databricks Workflow (this created workflow is what executes the ingestion and transformations for the benchmark).  

#### Data Generation
- Data generation is a requisite step by TPC to create the required raw data in the various formats. 
- The number of files does not change with increasing scale factors - the size of the files increases as the scale factor increases
- The steps in the data generator are as follows:
  1) Copy the *TPC-provided JAR and dependencies* to the driver from the {repo_root}/src/tools/datagen folder
  2) Execute a python wrapper to the JAR, which generates all required files into the driver's /localdisk0/tmp/tpcdi folder
  3) Then move the generated files from the driver into the /tmp/tpcdi/sf={scale_factor} DBFS folder  

![Type of Raw Data Generated](/src/tools/readme_images/data_gen.png "Type of Raw Data Generated")  

**IMPORTANT:** The data generation JAR is *NOT* a distributed spark step, or even very multi-threaded for that matter.  Therefore this step may execute in less than a minute for scale_factor=10, or as long as 2 hours on scale_factor=10000.  
- Therefore, since this all executes on the driver, make sure you have a driver with storage and enough memory to handle this type of data generation step. 
- This shouldn't be a problem unless running data generation on scale factors over 1000. 
- If you do wish to execute TPC-DO on a scale factor over 1000, we suggest a **storage-optimized single node cluster** to generate the raw data.
  - Please be patient with the execution as you may go several minutes without a log entry into the stdout of the cell (specifically tradehistory and watchhistory can take several minutes as they are large files).  Unless the job has failed/aborted, it is very likely the driver is still generating files for you.  It would not be ideal to cancel a job that is running as expected only to have to re-generate all the files all over again.

**NOTE:** this should NOT be an issue unless running large scale factors - which the default settings for this code does not do (the default is scale_factor=10).  
- The code/steps are the same NO MATTER THE SCALE FACTOR.  
- The scale factor increase will ONLY affect how much data is generated, how long the job takes to execute, and how many resources it will take to benchmark in the subsequent workflow.  
- Therefore we do not suggest increasing the scale factor unless you are trying to actually run benchmarks.

**ANOTHER NOTE:** To account for multiple users attempting to generate files repeatedly for a workspace, the data generation step will check to see if data exists in DBFS already for the requested scale factor. If it does (meaning you or someone else has executed the TPC-DI in this workspace already) then this step will be skipped - this removing unnecessary data being stored and also removing the need to generate the data again.

#### Workflow Creation
The Databricks TPC-DI is developed in a way to make the execution simple and easy.  
- The wrapper/driver will provide a simple dropdown widget with 4 options - which type of workflow you want to create.  This should be enough for most.  
- However, if you prefer to create a custom workflow, you can opt for the workflow_builder notebook (also in the src folder with the Driver notebook). This version will offer many more widgets including scale factor, which DBR to use, which worker/driver type to use, where to store/read raw data, what to name the job/warehouse, etc.  We encourage keeping most of these values as default settings.  However, if you want to experiment with different scale factors, DBRs, or node types, this gives you that flexibility.

**CLUSTER SIZING:** The workflow that is created by running these notebooks will size the cluster according to your scale factor and node type selected.  To reduce complexity in making this "generic" approach to creating a dynamic job, we abstracted away the sizing of the cluster (number of worker nodes). Enough benchmarks have been executed by the Databricks field to know how big the cluster needs to be per scale factor, and this is dynamically adjusted based the worker selected as well - including the number of appropriate shuffle partitions needed.  **IF** you truly want to change these values, we suggest building your workflow using the workflow_builder and then changing the cluster configuration from the ensuing workflow created.

### Step 2) 


#### The size of cluster will depend on the Scale Factor and Worker Type Selected! 
* The size of raw data is 1 GB per 10 Scale Factor, meaning a single node could do a default 10 Scale Factor.
* We have tested several scale factors including 10, 100, 1000, 5000, and 10000 and have converged on the optimal size of the cluster. Based on the worker type's total memory and number of cores the cluster will be sized accordingly.
* Default settings at various sizes:
  * SF 100   = 8 shuffle partitions   = 8 Worker Cores
  * SF 1000  = 86 shuffle partitions  = 58 Worker Cores
  * SF 10000 = 864 shuffle partitions = 576 Worker Cores

# Footnote Disclaimer
TO-DO: Find out if we need to keep the following disclaimer. It needs to be present wherever *derived* results are presented. Likely won't be results presented in this repo though...  
As the Databricks implementation of the TPC-DI is an unpublished TPC Benchmark, the following is required legalese per TPC Fair Use Policy:  
*“The Databricks TPC-DI is derived from the TPC-DI and as such is not comparable to published TPC-DI results, as the Databricks TPC-DI results do not comply with the TPC-DI specification."*