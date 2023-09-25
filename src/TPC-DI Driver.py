# Databricks notebook source
# MAGIC %md
# MAGIC # WRAPPER For TPC-DI Implementation in Databricks
# MAGIC ## Reference the <a href="https://github.com/shannon-barrow/databricks-tpc-di/blob/main/README.md" target="_blank">READ ME</a> For Details About the Benchmark and This Implementation of It 
# MAGIC * This notebook is a WRAPPER over the ***workflow_builder*** notebook. This notebook builds a **DEFAULT** pipeline of your choice with a scale factor = 10. 
# MAGIC * If this is your first time running the TPC-DI, we highly encourage you to run this notebook *out-of-the-box*, which will deploy a default pipeline with all dependenices already satisfied.  
# MAGIC   * Moving to a higher scale factor will not change the pipeline in any way, but it may require a bigger cluster size to execute - the pipeline and core code will not change.  Therefore, the only motivation for increasing the scale factor would be in benchmarking scenarios.
# MAGIC * Reference the README file for details about the various parameters that are being passed to the Databricks Jobs API before making changes to the workflow_builder notebook. The defaults should be satisfactory in vast amount of cases, but, if you want to change them, please first reference the docs. 

# COMMAND ----------

# MAGIC %md
# MAGIC ## The Following Cell Will Populate 2 Widgets At The Top Of The Page To Determine Which Variations Of The Job To Build:
# MAGIC 1. Native Workflow with Structured Streaming Notebooks  
# MAGIC &nbsp;&nbsp;&nbsp;&nbsp;  * This version would be used for an "official" benchmark. Full audit checks and validation at the end of the run
# MAGIC 2. Delta Live Tables Pipeline: CORE Sku  
# MAGIC &nbsp;&nbsp;&nbsp;&nbsp;  * Best TCO Option
# MAGIC 3. Delta Live Tables Pipeline: PRO Sku  
# MAGIC &nbsp;&nbsp;&nbsp;&nbsp;  * Leverage APPLY CHANGES INTO to Simplify Slowly Changing Dimensions Ingestion, with both Type 1 and Type 2 SCD
# MAGIC 4. Delta Live Tables Pipeline: ADVANCED Sku  
# MAGIC &nbsp;&nbsp;&nbsp;&nbsp;  * Easily add in Data Quality to your pipeline to either limit ingestion of poor data or gain insights into your DQ - or both!
# MAGIC 5. <a href="https://docs.getdbt.com/" target="_blank">dbt Core</a>  
# MAGIC &nbsp;&nbsp;&nbsp;&nbsp;  * Built using dbt Core, this version allows the code to be run consistently across other DWs for comparing TCO!  
# MAGIC &nbsp;&nbsp;&nbsp;&nbsp;  * The <a href="https://github.com/rlsalcido24/dbtpcdi" target="_blank">dbt TPC-DI repo</a> exists externally but is called by the Workflow created here with all dependencies met
# MAGIC 6. <a href="https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-streaming-table.html" target="_blank">Streaming Tables</a> and <a href="https://docs.databricks.com/en/sql/user/materialized-views.html" target="_blank">Materialized Views</a>  
# MAGIC &nbsp;&nbsp;&nbsp;&nbsp;  * Built using dbt Core, this version allows the code to be run consistently across other DWs for comparing TCO!  

# COMMAND ----------

workflow_types = ["", "Native Notebooks Workflow", "CORE Delta Live Tables Pipeline", "PRO Delta Live Tables Pipeline with SCD Type 1/2", "ADVANCED Delta Live Tables Pipeline with DQ", "dbt Core on DB SQL Warehouse", "Streaming Tables and Materialized Views on DBSQL/DLT"]
dbutils.widgets.dropdown("workflow_type", "", workflow_types, "Workflow Type")

wf_type = dbutils.widgets.get('workflow_type')
if wf_type == '':
  displayHTML(f"<h1>Please select a Workflow Type from the widget above and rerun</h1>")
  raise Exception("Missing valid workflow type. Please select a Workflow Type from the widget above and rerun")

# COMMAND ----------

# DBTITLE 1,Run This Cell to Build Your Workflow!
# MAGIC %run ./workflow_builder
