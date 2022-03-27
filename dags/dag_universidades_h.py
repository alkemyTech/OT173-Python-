from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator

with DAG(
    'dag_universidades_h',
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
    load_uba = DummyOperator(task_id='load_uba')
    load_cine = DummyOperator(task_id='load_cine')

    [extract_uba >> transform_uba >> load_uba, extract_cine >> transform_cine >> load_cine]
