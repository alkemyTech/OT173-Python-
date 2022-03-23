from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator

with DAG(
    'dag_universidades_e',
    description='Tasks universities group e',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 4, 15),
    end_date=datetime(2022, 5, 15)
) as dag:
    tarea_1 = DummyOperator(task_id='execute_queries')
    tarea_2 = DummyOperator(task_id='get_csv')
    tarea_3 = DummyOperator(task_id='preprocessing_data')
    tarea_4 = DummyOperator(task_id='upload_data')

    tarea_1 >> [tarea_2, tarea_3, tarea_4]
