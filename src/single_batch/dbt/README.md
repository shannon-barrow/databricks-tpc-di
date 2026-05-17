# tpcdi_dbt

## Requirements:
- Fill in required arguments in the profiles.yml file
- Ensure the required files have been loaded into BLOB storage and put the location in the dbt_project.yml file
- Fill in remaining required fields in dbt_project.yml file
- If executing in Snowflake:
	- Refer to the file formats directory and create the required file formats in your schema
	- Make a copy of the customermgmt table.  See the customermgmt helper file
- If executing in Snowflake:
	- The customermgmt table is a prequisite.  It can be run from the main portion of the repo as a Databricks job
- Choose an execution type and replace the type in the model-paths field in dbt_project.yml file:  
 	`model-paths:  ["[Databricks|Snowflake]_[CSV|PARQUET|CSV_CLUSTERED]/models/"]`
- Use the command `dbt run` to launch