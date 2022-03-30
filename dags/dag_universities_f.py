from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator

default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'dag_universities_f',
    description='DAG for universities group F',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2022, 3, 19)
) as dag:
    # Extract data
    extract_univ_moron_task = DummyOperator(task_id='extract_univ_moron')
    extract_univ_rio_cuarto_task = DummyOperator(task_id='extract_univ_rio_cuarto')

    # Transform data
    transform_task = DummyOperator(task_id='transform_data')

    # Load data
    load_task = DummyOperator(task_id='load_data')

    [extract_univ_moron_task, extract_univ_rio_cuarto_task] >> transform_task >> load_task
