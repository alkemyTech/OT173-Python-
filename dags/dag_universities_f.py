from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator

with DAG(
    'dag_universities_f',
    description='DAG',
    schedule_interval=timedelta(hours=1),
    start_date=datetime.today()
) as dag:
    # Extract data
    extract_univ_moron_task = DummyOperator(task_id='extract_univ_moron')
    extract_univ_rio_cuarto_task = DummyOperator(task_id='extract_univ_rio_cuarto')

    # Transform data
    transform_task = DummyOperator(task_id='transform_data')
    
    # Load data
    load_task = DummyOperator(task_id='load_data')

    [extract_univ_moron_task, extract_univ_rio_cuarto_task] >> transform_task >> load_task
