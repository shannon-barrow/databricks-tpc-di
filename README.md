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
The benchmark has been implemented in various ways to demonstrate the versatility of the Databricks Lakehouse platform and so users can reference such implementations side-by-side to better enable them to build other similar data engineering pipelines:

1. Traditional Notebooks Workflow
2. Databricks Delta Live Tables CORE
3. Databricks Delta Live Tables PRO: introduces easier handling of SCD TYPE 1/2 historical tracking using the new APPLY CHANGES INTO syntax
4. Databricks Delta Live Tables ADVANCED: introduces Data Quality metrics as well so data engineers can more easily handle bad data quality, analysts can more confidently trust their data, and support teams can more easily monitor the quality of data as it enters the Lakehouse.

# User Journey
(in progress). 


# Footnote Disclaimer
TO-DO: Find out if we need to keep the following disclaimer. It needs to be present wherever *derived* results are presented. Likely won't be results presented in this repo though...  
As the Databricks implementation of the TPC-DI is an unpublished TPC Benchmark, the following is required legalese per TPC Fair Use Policy:  
*“The Databricks TPC-DI is derived from the TPC-DI and as such is not comparable to published TPC-DI results, as the Databricks TPC-DI results do not comply with the TPC-DI specification."*