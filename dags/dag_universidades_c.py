import logging
from datetime import datetime, timedelta
from os import path, makedirs

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(message)s',
    datefmt='%Y-%m-%d'
    )
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

default_args = {
    'owner': 'alkemy',
    'depends_on_past': False,
    'email': ['alkemy@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        dag_id='dag_universidades_c',
        schedule_interval='@hourly',
        start_date=datetime(2022, 3, 27),
        catchup=False,
        default_args=default_args
        ) as dag:

    def extract(query, university):
        """PythonOperator and PostgresHook to get the data from the
        database and save it in a csv file in the include/tmp folder
        of the project.

        Args:
            query (str): The query file to be executed in the database.
            example: query_universidades_c.sql
            university (str): The name of the university to be extracted.
            example: 'universidad_de_los_andes'
        """
        base_path = path.abspath(path.join(path.dirname(__file__), ".."))
        path_tmp = path.abspath(path.join(base_path, 'include', 'tmp'))
        if not path.isdir(path_tmp):
            makedirs(path_tmp)
        path_csv = path.join(path_tmp, university)
        path_query = path.join(base_path, 'sql', query)
        pg_hook = PostgresHook(
            postgres_conn_id='alkemy_db',
            schema='training'
        )
        with open(path_query, 'r') as file:
            pg_hook = PostgresHook(
                postgres_conn_id='alkemy_db',
                schema='training'
            )
            pg_conn = pg_hook.get_conn()
            df_to_csv = pd.read_sql(file.read(), pg_conn)
            logger.info(f'{university} - {df_to_csv.shape}')
            df_to_csv.to_csv(path_csv)

    # PythonOperator and PostgresHook to get the data from the database and
    #  save it in a csv file in the include/tmp folder of the project.
    extract_task_palermo = PythonOperator(
        task_id='extract_palermo',
        python_callable=extract,
        op_kwargs={
            'query': 'query_palermo.sql',
            'university': 'palermo.csv'
        }
    )
    extract_task_jujuy = PythonOperator(
        task_id='extract_jujuy',
        python_callable=extract,
        op_kwargs={
            'query': 'query_universidad_nacional_de_jujuy.sql',
            'university': 'universidad_nacional_de_jujuy.csv'}
    )
    # PythonOperator and pandas to read the csv file and create a /dataframe.
    # the procesed data are saved in .txt file
    transform_task = DummyOperator(
        task_id='transform',
        dag=dag,
    )

    # PythonOperator and boto3 to upload the .txt file to S3.
    # The file is deleted after the upload.
    load_task = DummyOperator(
        task_id='load',
        dag=dag,
    )

    [extract_task_palermo, extract_task_jujuy] >> transform_task >> load_task
