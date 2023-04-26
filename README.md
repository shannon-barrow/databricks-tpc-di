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

For additional context of this code base, more information is found in the following deck [TPC-DI Workflow Execution Flow, Notable Features, and Capabilities](https://docs.google.com/presentation/d/1UgVshJHaNYPGIjpQgsUEzyLOFPkfkeUNhwySnbDhrG0/edit?usp=sharing).  

### LIMITATIONS
* The code base is implemented to execute successfully ONLY on the Databricks platoform, on either GCP, AZURE, OR AWS.    
* This code requires:
  * Databricks E2 Deployment. 
  * Databricks Repos functionality. 
  * Access to GitHub via public internet access. 

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

![Execution Journey](/src/tools/readme_images/tpcdi_execution_journey.png "Execution Journey")

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
- To account for multiple users attempting to generate files repeatedly for a workspace, the data generation step will check to see if data exists in DBFS already for the requested scale factor. 
  - If it does (meaning you or someone else has executed the TPC-DI in this workspace already) then this step will be skipped - thus removing unnecessary data being stored and also removing the need to generate the data again.

#### Workflow Creation
The Databricks TPC-DI is developed in a way to make the execution simple and easy.  
- The wrapper/driver will provide a simple dropdown widget with 4 options - which type of workflow you want to create - which should suffice for most situations  
- However, if you prefer to create a custom workflow, you can opt for the workflow_builder notebook (also in the src folder with the Driver notebook). 
  - This version will offer many more widgets including scale factor, which DBR to use, which worker/driver type to use, where to store/read raw data, what to name the job/warehouse, etc.  
  - We encourage keeping most of these values as default settings.  However, if you want to experiment with different scale factors, DBRs, or node types, this gives you that flexibility.
- Workflow creation should take less than 1 second.  When complete you will be given a link to open your workflow.

![Workflow Created!](/src/tools/readme_images/workflow_created.png "Workflow Created!")  

##### CLUSTER SIZING
- The workflow that is created by running these notebooks will size the cluster according to your scale factor and node type selected.  
- To reduce complexity in making this "generic" approach to creating a dynamic job, we abstracted away the sizing of the cluster (number of worker nodes). 
- The correct number of workers and shuffle partition count will automatically be applied, based on prior testing across clouds, worker types, and cluster sizes, according to your selected workflow type, scale factor, and worker type. 
  - **IF** you truly want to change these values, we suggest building your workflow using the workflow_builder and then changing the cluster configuration from the ensuing workflow created.
  - The size of raw data is 1 GB per 10 Scale Factor, meaning a single node could do a default 10 Scale Factor.
  - Default settings at various sizes:
    | Scale Factor | Shuffle Partitions | Worker Cores |
    | ------------ | ------------------ | ------------ |
    | 100          | 8                  | 8            |
    | 1000         | 86                 | 58           |
    | 10000        | 864                | 576          |

### Step 2) TPC-DI Workflow Execution
- After opening the link provided for your workflow, to begin execution click the **Run** button. To watch execution, open the new Job Run.

![Workflow Page After Completing 1 Run](/src/tools/readme_images/workflow.png "Workflow")  

![Job Run of The Workflow](/src/tools/readme_images/workflow_run.png "Workflow Run")  


# Footnote Disclaimer
As the Databricks implementation of the TPC-DI is an unpublished TPC Benchmark, the following is required legalese per TPC Fair Use Policy:  
*The Databricks TPC-DI is derived from the TPC-DI and as such is not comparable to published TPC-DI results.  The current scoring metrics for the TPC-DI preclude any official submission for cloud-based execution and therefore The Databricks TPC-DI can not OFFIALLY be submitted under current scoring metrics.  Databricks withholds the ability to submit an official submission to the TPC for this benchmark upon future revision of its scoring metrics. Prior to that, we maintain that this implementation follows all guidelines, rules, and audits required of the official TPC-DI specification.*
