import logging
import os
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from decouple import config
from sqlalchemy import create_engine, text


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
        start_date=datetime.now() - timedelta(days=1),
        catchup=False,
        default_args=default_args
        ) as dag:

    def extract(query, university):
        """PythonOperator and PostgresHook to get the data from the
        database and save it in a csv file in the include/tmp folder
        of the project.

        Args:
            query (str): The query file name and extension to be executed\
                in the database.
            example: query_universidades_c.sql
            university (str): The name of the university to be extracted.
            example: 'universidad_de_los_andes'
        """
        DB_USER = config('DB_USER')
        DB_PASSWORD = config('DB_PASSWORD')
        DB_HOST = config('DB_HOST')
        DB_PORT = config('DB_PORT')
        DB_DATABASE = config('DB_DATABASE')
        db_url = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:\
            {DB_PORT}/{DB_DATABASE}"
        base_path = os.path.abspath(
            os.path.join(
                os.path.dirname(__file__),
                ".."))
        path_tmp = os.path.abspath(os.path.join(base_path, 'include', 'tmp'))
        os.makedirs(path_tmp, exist_ok=True)
        path_csv = os.path.join(path_tmp, university)
        path_query = os.path.join(base_path, 'sql', query)

        with open(path_query, 'r') as file:
            engine = create_engine(db_url)
            df_to_csv = pd.read_sql(text(file.read()), engine)
            logger.info(f'{university} - {df_to_csv.shape}')
            df_to_csv.to_csv(path_csv)

    # PythonOperator to get the data from the database and
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
