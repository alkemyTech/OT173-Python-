import logging
import os
from datetime import datetime, timedelta

import boto3
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from botocore.exceptions import ClientError
from decouple import config

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

# Instance directories
current_dir = os.path.abspath(os.path.dirname(__file__))
parent_dir = os.path.abspath(os.path.join(current_dir, os.pardir))


def load_data(file_name, object_name=None):
    """Upload a file to an S3 bucket

    Args:

        file_name (str): File to upload
        object_name (str): S3 object name. If not specified then file_name is used

    return True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    s3_client = boto3.client(
                            's3',
                            aws_access_key_id=config('AWS_PUBLIC_KEY'),
                            aws_secret_access_key=config('AWS_SECRET_KEY')
                        )
    try:
        response = s3_client.upload_file(f'{parent_dir}/files/{file_name}', config('AWS_BUCKET_NAME'), object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True


with DAG(
    'dag_universidades_h',
    default_args=default_args,
    description='DAG for processing data from Universidad Del Cine and Universidad De Buenos Aires',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2022, 3, 20)
) as dag:
    # Extract data with SQL query - Postgres operator
    extract_uba = DummyOperator(task_id='extract_uba')
    extract_cine = DummyOperator(task_id='extract_cine')

    # Transform data with Pandas - Python operator
    transform_uba = DummyOperator(task_id='transform_uba')
    transform_cine = DummyOperator(task_id='transform_cine')

    # Load data to S3 - S3 operator
    load_uba = PythonOperator(
                    task_id='load_uba',
                    python_callable=load_data,
                    op_kwargs={
                        'file_name': 'transform_uba.txt'
                        }
                    )
    load_cine = PythonOperator(
                    task_id='load_cine',
                    python_callable=load_data,
                    op_kwargs={
                        'file_name': 'transform_cine.txt'
                        }
                    )

    [extract_uba >> transform_uba >> load_uba, extract_cine >> transform_cine >> load_cine]
