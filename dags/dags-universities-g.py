import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow', 
    'depends_on_past': False,
    'start_date': datetime(2022, 3, 23),
    # 'email': ['airflowexample.com'],
    # 'email_on_failure': False,
    # 'email_on_retry': False,
    # 'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


# Get data from SQL
def get_data():
    logging.info('Getting data')


# Process data in a DataFrame
def data_process():
    logging.info('Processing data')


# Save data in S3
def save_data():
    logging.info('Saving data in S3')


with DAG(
    'dag-g-PT173-34',
    default_args=default_args,
    description='Dags Universities',
    schedule_interval='@hourly',
    # start_date = datetime(2022, 1, 19),
    tags=['universities'],
) as dag:
    getdata_task = PythonOperator(task_id='get_data', python_callable=get_data)
    dataprocess_task = PythonOperator(task_id='data_process', python_callable=data_process)
    savedata_task = PythonOperator(task_id='save_data', python_callable=save_data)

    getdata_task >> dataprocess_task >> savedata_task