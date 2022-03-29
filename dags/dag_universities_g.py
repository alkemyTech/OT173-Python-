import logging
from datetime import datetime, timedelta
from time import strftime

from airflow import DAG
from airflow.operators.python import PythonOperator

logging.basicConfig(level=logging.INFO, datefmt=strftime("%Y-%m-%d"),
                    format='%(asctime)s - %(name)s - %(message)s')

logger = logging.getLogger("Universities_G")

default_args = {
    'owner': 'Alkemy',
    # 'depends_on_past': False,
    # 'start_date': datetime(2022, 3, 23),
    # 'email': ['airflowexample.com'],
    # 'email_on_failure': False,
    # 'email_on_retry': False,
    'retries': 5, # Try in 5 opportunities to run the script
    'retry_delay': timedelta(minutes=5), # Wait 5 minutes to try to run the script again
}


def get_data():
    """ Get data from SQL"""

    logger.info('Getting data')


def data_process():
    """ Process data in a DataFrame """

    logger.info('Processing data')

 
def save_data():
    """ Save data in S3 """

    logger.info('Saving data in S3')


with DAG(
    'Dag_Universities_G',
    default_args=default_args,
    description='Dag Universities',
    schedule_interval='@hourly',
    start_date=datetime(2022, 3, 23),
    tags=['universities'],
) as dag:
    getdata_sociales_task = PythonOperator(task_id='get_data_sociales', python_callable=get_data)
    getdata_kennedy_task = PythonOperator(task_id='get_data_kennedy', python_callable=get_data)
    dataprocess_task = PythonOperator(task_id='data_process', python_callable=data_process)
    savedata_task = PythonOperator(task_id='save_data', python_callable=save_data)

    [getdata_sociales_task, getdata_kennedy_task] >> dataprocess_task >> savedata_task
