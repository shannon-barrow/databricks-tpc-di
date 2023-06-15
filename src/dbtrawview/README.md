# DBTPCDI

This repo is an end to end implementation of tpcdi using dbt and databricks. Assuming dbt is installed, and that the customermgmt xml file has already been parsed and catalog.schema.table is hard coded in base/customermgmtview.sql, here is six step process to get repo up and running!

i) Update profiles.yml with your prod schema, host, token, and http_path\
ii) Update sources.yml with your staging schema\
iii) Update project.yml with the benchmark you want to run \
iv) dbt deps \
v) dbt run-operation stage_external_sources \
vi) dbt run
