# -*- coding: utf-8 -*-
import logging
import os
from pathlib import Path

import pandas as pd
from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)


class SalesIngestionException(Exception):
    pass


class SalesIngestionOperator(BaseOperator):
    def __init__(
            self,
            data_file_path: str or Path,
            postgres_conn_id: str,
            schema: str,
            table_name: str,
            *args,
            **kwargs
    ):
        super().__init__(*args, **kwargs)

        self.data_file_path = data_file_path
        self.postgres_conn_id = postgres_conn_id
        self.schema = schema
        self.table_name = table_name

    def execute(self, context: dict):
        """
        Ingest sales data into Postgres database
        """
        sales_df = pd.read_csv(os.getenv('AIRFLOW_HOME') + self.data_file_path)

        clean_df = self._clean_data(sales_df)

        self._prepare_db()

        self._insert_into_db(clean_df)

    def _clean_data(self, sales_df: pd.DataFrame) -> pd.DataFrame:
        """
        Handle data quality issues (missing values, duplicates):
        1. Convert numeric and date columns to uniform format (any non-standard values will become NaN/NaT)
        2. Handle null values (set strings to N/A, numerics to 0)
        3. Handle incorrect input (negative metrics)
        4. Drop duplicates
        :param sales_df: Raw sales data from csv
        :return: Cleaned sales data to insert into the database
        """

        columns = {'SaleID', 'ProductID', 'ProductName', 'Brand', 'Category', 'RetailerID', 'RetailerName', 'Channel',
                   'Location', 'Quantity', 'Price', 'Date'}

        if set(sales_df.columns.tolist()) != columns:
            raise SalesIngestionException('CSV file schema is incorrect.')

        numeric_columns = ['ProductID', 'RetailerID', 'Quantity', 'Price']

        try:
            for column in numeric_columns:
                sales_df[column] = pd.to_numeric(sales_df[column], errors='coerce')
                sales_df[column] = sales_df[column].fillna(0).astype(int)
                if column == 'Quantity':
                    sales_df[column] = sales_df[column].abs().astype(int)
                elif column == 'Price':
                    sales_df[column] = sales_df[column].abs().astype(float)

            sales_df['Date'] = pd.to_datetime(sales_df['Date'], errors='coerce', format='mixed')
            sales_df['Location'] = sales_df['Location'].fillna('N/A')
        except Exception as e:
            raise SalesIngestionException("Missing values and type conversion failed") from e
        try:
            sales_df = sales_df.drop_duplicates(subset='SaleID', keep='first')
        except Exception as e:
            raise SalesIngestionException("Drop duplicates failed") from e

        return sales_df

    def _insert_into_db(self, clean_df: pd.DataFrame):
        try:
            engine = PostgresHook(self.postgres_conn_id).get_sqlalchemy_engine()

            clean_df.to_sql(
                name=self.table_name,
                schema=self.schema,
                con=engine,
                if_exists="append",
                index=False,
                method="multi",
            )
        except Exception as e:
            raise SalesIngestionException("Insert into database failed") from e

    def _prepare_db(self):
        """
        Create schema and table if they don't exist
        """
        try:
            conn = PostgresHook(self.postgres_conn_id).get_conn()
            cursor = conn.cursor()

            cursor.execute(f"""
            SELECT EXISTS(SELECT 1 FROM information_schema.schemata 
                  WHERE schema_name = '{self.schema}');
            """)
            schema_exists = cursor.fetchone()[0]

            if not schema_exists:
                cursor.execute(f"""
            CREATE SCHEMA {self.schema};
            """)
            conn.commit()

            cursor.execute(f"""
            SELECT EXISTS(SELECT 1 FROM information_schema.tables 
               WHERE  table_schema = '{self.schema}'
               AND    table_name   = '{self.table_name}'
               )
            """)
            table_exists = cursor.fetchone()[0]

            if not table_exists:
                cursor.execute(f"""
            CREATE TABLE {self.schema}.{self.table_name} (
                "SaleID"            int primary key,
                "ProductID"         int,
                "ProductName"       text,
                "Brand"             text,
                "Category"          text,
                "RetailerID"        int,
                "RetailerName"      text,
                "Channel"           text,
                "Location"          text,
                "Quantity"          int,
                "Price"             decimal(18,2),
                "Date"              date
            );
            """)
            conn.commit()

            cursor.execute(f"""
            TRUNCATE TABLE {self.schema}.{self.table_name};
            """)
            conn.commit()
        except Exception as e:
            raise SalesIngestionException("Database preparation failed") from e


class SalesIngestionPlugin(AirflowPlugin):
    name = "SalesIngestionPlugin"
    operators = [SalesIngestionOperator]

