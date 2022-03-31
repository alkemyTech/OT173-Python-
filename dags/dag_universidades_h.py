import logging
import os
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from decouple import config
from sqlalchemy import create_engine
from sqlalchemy.sql import text

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(message)s',
    datefmt='%Y-%m-%d',
    level=logging.INFO,
    filemode="a"
    )
logger = logging.getLogger(__name__)

default_args = {
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}


def extract_data(sql, csv):
    """Extact data. Connection to DB, execute SQL query and export to CSV.

    Args:
        sql (str): Name of SQL script to be executed in '../sql/' directory.
        csv (str): Name of CSV output to be generated in '../files/' directory (creates it automatically if not exists).
    """

    # Database Connection
    user = config('USER')
    passwd = config('PASSWD')
    host = config('HOST')
    port = config('PORT')
    db = config('DB')
    url = f'postgresql://{user}:{passwd}@{host}:{port}/{db}'
    engine = create_engine(url, encoding='utf8')
    con = engine.connect()

    # Instance directories
    current_dir = os.path.abspath(os.path.dirname(__file__))
    parent_dir = os.path.abspath(os.path.join(current_dir, os.pardir))

    # Execute SQl query
    file = open(f'{parent_dir}/sql/{sql}')
    query = text(file.read())
    df = pd.read_sql(query, con)
    logger.info(sql + csv)
    if not os.path.exists(f'{parent_dir}/files'):
        os.makedirs(f'{parent_dir}/files')
    df.to_csv(f'{parent_dir}/files/{csv}', encoding='utf-8')
    con.close()


with DAG(
    'dag_universidades_h',
    default_args=default_args,
    description='DAG for processing data from Universidad Del Cine and Universidad De Buenos Aires',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2022, 3, 20)
) as dag:

    # Extract data with SQL query - Python operator
    extract_uba = PythonOperator(
                    task_id='extract_uba',
                    python_callable=extract_data,
                    op_kwargs={
                        'sql': 'query_uba.sql',
                        'csv': 'extract_uba.csv'
                        }
                    )
    extract_cine = PythonOperator(
                    task_id='extract_cine',
                    python_callable=extract_data,
                    op_kwargs={
                        'sql': 'query_cine.sql',
                        'csv': 'extract_cine.csv'
                        }
                    )

    # Transform data with Pandas - Python operator
    transform_uba = DummyOperator(task_id='transform_uba')
    transform_cine = DummyOperator(task_id='transform_cine')

    # Load data to S3 - Python operator
    load_uba = DummyOperator(task_id='load_uba')
    load_cine = DummyOperator(task_id='load_cine')

    [extract_uba >> transform_uba >> load_uba, extract_cine >> transform_cine >> load_cine]
