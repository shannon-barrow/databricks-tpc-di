# Databricks notebook source
# MAGIC %md
# MAGIC # Driver For TPC-DI Implementation in Databricks
# MAGIC ## Reference the <a href="https://github.com/databricks/tpcdi-sql/blob/main/README.md" target="_blank">READ ME</a> For Details About the Benchmark and This Implementation of It 
# MAGIC * This notebook is a WRAPPER over the ***workflow_builder*** notebook. This notebook builds a **DEFAULT** pipeline of your choice with a scale factor = 10. 
# MAGIC * If this is your first time running the TPC-DI, we highly encourage you to run this notebook *out-of-the-box*, which will deploy a default pipeline with all dependenices already satisfied.  
# MAGIC   * Moving to a higher scale factor will not change the pipeline in any way, but it may require a bigger cluster size to execute - the pipeline and core code will not change.  Therefore, the only motivation for increasing the scale factor would be in benchmarking scenarios.
# MAGIC * Reference the README file for details about the various parameters that are being passed to the Databricks Jobs API before making changes to the workflow_builder notebook. The defaults should be satisfactory in vast amount of cases, but, if you want to change them, please first reference the docs. 

# COMMAND ----------

# MAGIC %md
# MAGIC ## The Following Cell Will Populate a Widget At The Top Of The Page To Determine Which Variation Of The Job To Build:
# MAGIC 1. Native Workflow with Structured Streaming Notebooks  
# MAGIC &nbsp;&nbsp;&nbsp;&nbsp;  * This version would be used for an "official" benchmark. Full audit checks and validation at the end of the run
# MAGIC 2. Delta Live Tables Pipeline: CORE Sku  
# MAGIC &nbsp;&nbsp;&nbsp;&nbsp;  * Best TCO Option
# MAGIC 3. Delta Live Tables Pipeline: PRO Sku  
# MAGIC &nbsp;&nbsp;&nbsp;&nbsp;  * Leverage APPLY CHANGES INTO to Simplify Slowly Changing Dimensions Ingestion, with both Type 1 and Type 2 SCD
# MAGIC 4. Delta Live Tables Pipeline: ADVANCED Sku  
# MAGIC &nbsp;&nbsp;&nbsp;&nbsp;  * Easily add in Data Quality to your pipeline to either limit ingestion of poor data or gain insights into your DQ - or both!

# COMMAND ----------

workflow_types = ["Native Notebooks Workflow", "CORE Delta Live Tables Pipeline", "PRO Delta Live Tables Pipeline with SCD Type 1/2", "ADVANCED Delta Live Tables Pipeline with DQ"]
dbutils.widgets.dropdown("workflow_type", "Native Notebooks Workflow", workflow_types, "Workflow Type")
wf_type = dbutils.widgets.get('workflow_type')
displayHTML(f"<h1>To protect against unintenional 'RUN ALL' commands, the cell below is an Intentional Notebook Stoppage to confirm your workflow choice of {wf_type.upper()} is correct! If so, run the last cell below to create your workflow.</h1>")

# COMMAND ----------

dbutils.notebook.exit(f"Intentional notebook stoppage command, to confirm your workflow choice of {wf_type.upper()} is correct! If so, run the cell below to create your workflow.")

# COMMAND ----------

# DBTITLE 1,Run This Cell to Build Your Workflow!
# MAGIC %run ./workflow_builder
