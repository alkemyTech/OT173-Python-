import logging
from datetime import datetime, timedelta
from time import strftime
from pathlib import Path

import pandas as pd
from airflow import DAG
from decouple import config
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine, text

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
    'retries': 5,  # Try in 5 opportunities to run the script
    'retry_delay': timedelta(minutes=5),  # Wait 5 minutes to try to run the script again
}


def connect_db():
    """ Connect to db """

    try: 
        db_database = config('DB_DATABASE')
        db_host = config('DB_HOST')
        db_password = config('DB_PASSWORD')
        db_port = config('DB_PORT')
        db_user = config('DB_USER')

        engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_database}')

        connection = engine.connect()

        logger.info('Connected to the DataBase!')

        return connection
    except KeyError:
        return logger.exception('Connection failed.')


def get_data(**kwargs):
    """ Get data from SQL and convert to CSV """

    # connect_db()
    connection = connect_db()

    root_dir = Path(__file__).resolve().parent.parent
    file_path = Path(f'{root_dir}/sql/{kwargs["sql_file"]}')

    with open(file_path) as f:
        # passing a plain string directly to Connection.execute() is deprecated
        # and we should use text() to specify a plain SQL query string instead.
        sql_text = text(f.read())
        result = connection.execute(sql_text)
        df = pd.DataFrame(result.fetchall())
        df.columns = result.keys()
        csv_path = Path(f"{root_dir}/csv").mkdir(parents=True, exist_ok=True)
        csv_path = str(csv_path)
        file_csv = df.to_csv(f"{root_dir}/csv/{kwargs['file_name']}")

    logger.info('Getting data')

    # return df
    return file_csv


def data_process():
    """ Process data in a DataFrame """

    logger.info('Processing data')


def save_data():
    """ Save data in S3 """

    logger.info('Saving data in S3')


with DAG(
    'dag_universities_g',
    default_args=default_args,
    description='Dag Universities',
    schedule_interval='@hourly',
    start_date=datetime(2022, 3, 23),
    tags=['universities'],
) as dag:
    getdata_sociales_task = PythonOperator(
        task_id='get_data_sociales',
        python_callable=get_data,
        op_kwargs={
            'sql_file': 'query_sociales.sql',
            'file_name': 'sociales.csv'
        }
    )
    getdata_kennedy_task = PythonOperator(
        task_id='get_data_kennedy',
        python_callable=get_data,
        op_kwargs={
            'sql_file': 'query_kenedy.sql',
            'file_name': 'kenedy.csv'
        }
    )
    dataprocess_task = PythonOperator(task_id='data_process', python_callable=data_process)
    savedata_task = PythonOperator(task_id='save_data', python_callable=save_data)

    [getdata_sociales_task, getdata_kennedy_task] >> dataprocess_task >> savedata_task
