# Hostaway Senior Data Engineer Take-Home Assignment

## Overview
This project cleans and loads sales data from a `csv` file (including database preparation) and transforms the data using dbt.

## Architecture
`docker-compose.yml` contains definitions of containers for Airflow (webserver, scheduler) and Postgres database.
The pipeline consists of database preparation (schema/table creation), data ingestion and data transformation (dbt).
`sales.py` has the DAG definition for the pipeline. It is scheduled to run every day at 5:00 AM UTC (this time is selected so that the data is processed after the day is over but before the start of the next working day).
The preparation and ingestion part is handled by the `SalesIngestionOperator` – a custom-written operator that conducts database objects creation, data checks, data cleaning and data loading.
Then indexes are created using `SQLExecuteQueryOperator` (that runs multiple SQL statements in a row). Afterward a small setup of environment is needed to run dbt.
This setup and the `dbt build` command are run with the help of `BashOperator`.
Please see below for run instructions and potential improvements.

### Part 1: Data cleaning
`_clean_data(df)` checks for data format, handles missing values, drops duplicates and performs type conversions.

### Part 2: Database preparation
`_prepare_db()` creates schema and table if they don't exist, as well as truncates the table to ensure no duplicates appear after insertion.

### Part 3: Data Ingestion
`_insert_into_db(df)` adds data into the table in the `raw` schema. Then indexes on some columns are created to facilitate filtering.
In this case, `ProductID`, `RetailerID`, `Location` and `Date` are selected, as these dimensions are popular in reports based on sales data.
`dbt` command is then installed in the environment, so that we can use it in the transformation step.

### Part 4: Data Transformation
A dbt project contains all the transformations. The project is structured into 3 layers:
- `staging`
  - This layer contains data from raw layer (with renamed columns). We also use it to perform simple tests, such as `id not null` test or allowed values for channnel.
- `intermediate`
  - This layer contains incrementally loaded dimension and fact tables for sales. In this case, a variant of star schema was selected. There are possible improvements to the data model choice (please see below).
- `mart`
  - This layer contains aggregated data that might be used in reporting layers. Here a sample mart model was chosen with sales metrics per day. 

## Instructions to run
- Clone the repository.
- Run `python3 -m pip install -r requirements.txt` from the root of the repository.
- Run `docker-compose up -d`. 
- Go to `localhost:8080`.
- Log in using login `airflow` and password `airflow`.
- Go to "Connections" and add a connection `db` with host `postgres`, database `sales`, login `postgres` and password `mysecretpassword`.
- Enable the DAG `sales` and run it.
The data should load into all the Postgres database. 
You can use any database tool (e.g. DBeaver) to connect to the database using credentials found in `profiles.yml` folder (the port should be `6543`).

## Potential improvements
- Documentation can be added directly to dbt models (with `dbt docs` added to the pipeline or to the CI/CD for the repository).
- Instead of dbt Core, the project can be ported into dbt Cloud, enabling the use of dbt Cloud API to trigger jobs created there
- Notifications can be added (Slack/E-mail), so that pipeline failures are handled by engineers before missing data alerts stakeholders
- A `location` dimension can be established. Furthermore, the schema can be transformed into a snowflake model, if data consistency is an important factor.
- Incremental load can be applied to the source data with the use of a dedicated CDC table. Each run would start with extracting the latest date from the CDC table to only process records with date after the CDC value. This can help with not having to process all data, but requires an additional database call on the cleaning stage.