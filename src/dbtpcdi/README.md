# DBTPCDI

This repo is an end to end implementation of
[TPC-DI](https://www.tpc.org/tpcdi/default5.asp). The repo is desinged to
be run against Databricks, Snowflake, BigQuery, Redshift, and Synapse.
You can select your desired target warehouse with different targets that
are defined in the profiles.yml; for ex `dbt run --target databricks`
will build the TPCDI tables in Databricks while `dbt run --target redshift`
will build the tables in Redshift. The default is to build the tables in Databricks but you 
can build the tables in other warehouses by adjusting the model-paths key to the desried warehouse 
The steps below will be applicable regardless of the target warehouse. For warehouse specific considerations,
refer to the read.me within the relevant directories of the models directory. 

## Data Preparation 
This project uses the TPC-DI Kit TPC-DI Data Generator https://github.com/haydarai/tpcdi-kit

### Generating test data files
Using the DIGen Tool

```shell
java -jar DIGen.jar -o ../staging/10/ -sf 10
```
Once data files is generated, upload the files to your working cloud
storage account. We recommend using the cli tool for the relevant cloud
(aws CLI for Redshift, azure SLI for synapse, gcloud sli for big query,
etc). 

Please refer to the individual readmes to determine how to best handle the
customermgmt xml file.


### Configuration and running

(Note that do not run dbt deps as some of the packages/macros the code relies on are custom)
1. create your prod and staging schemas (if not already created)
2. Update profiles.yml with your prod schema and other warehouse specific
configurations.
3. Update project.yml with desired scalefactor, bucketname, and staging schema
4. dbt run-operation stage_external_sources
5. dbt run

### Contributing

Additional contributions are welcome.

For small changes, please submit a pull request with your changes.

For larger changes that would change the majority of code in a single
file or span multiple files, please open an issue first for discussion.

#### Linting

This project uses several formatters and linters for different file types. Each linter should be run before submitting
a pull request and the appropriate changes should be made to resolve any errors or warnings. This projects has
built-in checks that will run automatically when a pull request is submitted and need to pass before the pull request
can be merged. The file `dev-requirements.txt` contains the packages needed to run the linters. 

The `dev-requirements.txt` file can be used to install all of the packages at once with the command
`pip install -r dev-requirements.txt`. It's considered best practice to install the packages in a python virtual
environment.

Instructions to install each package will be linked below if you do not wish to use the `dev-requirements.txt` file.

##### yaml
We use [yamllint](https://yamllint.readthedocs.io/en/stable/) to lint the yaml files. To run the linter, run
`yamllint .` from the root of the project. Errors and warnings will be printed to the console for review.

[Instructions to install yamllint](https://yamllint.readthedocs.io/en/stable/quickstart.html#installing-yamllint).

##### python
We use [black](https://black.readthedocs.io/en/stable/usage_and_configuration/the_basics.html) to format and
[flake8](https://flake8.pycqa.org/en/latest/) to lint python files. In addition, `black-jupyter` is an extension of
`black` for Jupyter Notebooks, which end with an `.ipynb` extension.

We recommend running `black .` to format code prior to `flake8 .` to lint the code since `black` will auto-correct
many `flake8` violations. Errors and warnings will be printed to the console for review by both tools.

[Instructions to install black](https://black.readthedocs.io/en/stable/installation_and_usage.html#installing-black).
[Instructions to install flake8](https://flake8.pycqa.org/en/latest/#installation).
[Instructions to install black-jupyter](https://github.com/n8henrie/jupyter-black).

##### sql
We use [sqlfluff](https://docs.sqlfluff.com/en/stable/) to lint the sql files. To run the formatter, run
`sqlfluff format .`, and to run the linter, run `sqlfluff lint .` from the root of the project. Errors and warnings will
be printed to the console for review.

No additional configurations, including specification of the SQL dialect is required since the project uses a global
`.sqlfluff` file at the root directory to manage configurations and a `.sqlfluff` file to specify the dialect for each
Warehouse directory.
