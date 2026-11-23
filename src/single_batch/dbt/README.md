# tpcdi_dbt

## Requirements:
- Fill in required arguments in the profiles.yml file
- Ensure the required files have been loaded into BLOB storage and put the location in the dbt_project.yml file
- Fill in remaining required fields in dbt_project.yml file
- If executing in Snowflake:
	- Create the file formats in the run's schema from the `helpers` directory (`Snowflake_CSV/helpers`: TXT_PIPE, TXT_CSV, TXT_FIXED_WIDTH, TXT_XML)
	- `Snowflake_CSV` builds CustomerMgmt itself from Batch1's CustomerMgmt XML (DIGen's single file or the Spark generator's numbered `CustomerMgmt_N.xml` files), so no pre-built customermgmt table is needed
	- `Snowflake_PARQUET` still expects a pre-built customermgmt table
- Choose an execution type and replace the type in the model-paths field in dbt_project.yml file:  
 	`model-paths:  ["[Databricks|Snowflake]_[CSV|PARQUET|CSV_CLUSTERED]/models/"]`
- Use the command `dbt run` to launch