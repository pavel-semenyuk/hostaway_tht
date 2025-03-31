from datetime import datetime, timedelta

from airflow import DAG

from SalesIngestionPlugin import SalesIngestionOperator
from airflow.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

with DAG(
    dag_id="sales_data",
    start_date=datetime(2025, 3, 30),
    description="Sales data ingestion and transformation",
    schedule_interval="0 5 * * *",
    dagrun_timeout=timedelta(hours=2),
    max_active_runs=1,
    catchup=False,
    doc_md=__doc__,
) as dag:

    ingest_sales_data = SalesIngestionOperator(
        task_id='ingest_sales_data',
        data_file_path='/dags/data/generated_sales_data.csv',
        postgres_conn_id='db',
        schema='raw',
        table_name='sales'
    )

    create_index = SQLExecuteQueryOperator(
        task_id='create_index',
        conn_id='db',
        sql="""
        create index if not exists raw_sales_product_index on raw.sales ("ProductID");
        create index if not exists raw_sales_retailer_index on raw.sales ("RetailerID");
        create index if not exists raw_sales_location_index on raw.sales ("Location");
        create index if not exists raw_sales_date_index on raw.sales ("Date");
        """
    )

    setup_dbt_env = BashOperator(
        task_id='setup_dbt_env',
        bash_command='pip install dbt-core dbt-postgres'
    )

    build_dbt = BashOperator(
        task_id='build_dbt',
        bash_command='cd ${AIRFLOW_HOME}/dags/dbt/hostaway_tht && dbt build --profiles-dir ./..'
    )

    (
        ingest_sales_data >>
        create_index >>
        setup_dbt_env >>
        build_dbt
    )
