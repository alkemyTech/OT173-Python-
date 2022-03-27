import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator

logging.basicConfig(level=logging.INFO, datefmt=("%Y-%m-%d"),
                    format='%(asctime)s - %(levelname)s - %(message)s')

log = logging.getLogger("dag_universidades_e")

# added attribute default_args into DAG: default_args = default_args.
default_args = {
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'dag_universidades_e',
    default_args=default_args,
    description='Tasks universities group e',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2022, 4, 15)
) as dag:
    # Using PythonOperator:
    # Extract data from Postgresql
    execute_query_inter = DummyOperator(task_id='execute_query_inter')
    execute_query_pampa = DummyOperator(task_id='execute_query_pampa')

    # Transform data with Pandas
    convert_to_csv = DummyOperator(task_id='convert_to_csv')
    preprocessing_data = DummyOperator(task_id='preprocessing_data')
    convert_to_txt = DummyOperator(task_id='convert_to_txt')

    # Load .txt to S3 server
    upload_data = DummyOperator(task_id='upload_data')

    execute_query_inter >> convert_to_csv >> preprocessing_data >> convert_to_txt >> upload_data,
    execute_query_pampa >> convert_to_csv >> preprocessing_data >> convert_to_txt >> upload_data
