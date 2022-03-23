from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.dummy import DummyOperator

import logging

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

# Save date in S3
def save_data():
    logging.info('Saving data in S3')

with DAG(
    'dag-g-PT173-34',
    default_args = default_args,
    description = 'Dags Universities',
    schedule_interval = '@hourly',
    # start_date = datetime(2022, 1, 19),
    tags = ['dags'],

) as dag:
    getData_task = DummyOperator(task_id='getData', python_callable=getData)
    dataProcess_task = DummyOperator(task_id='dataProcess', python_callable=dataProcess)
    saveData_task = DummyOperator(task_id='saveData', python_callable=saveData)
    
    getData_task >> dataProcess_task >> saveData_task