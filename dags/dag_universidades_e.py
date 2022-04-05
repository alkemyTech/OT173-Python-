import logging
import os
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from decouple import config
from sqlalchemy import create_engine
from sqlalchemy.sql import text

logging.basicConfig(level=logging.INFO, datefmt=("%Y-%m-%d"),
                    format='%(asctime)s - %(levelname)s - %(message)s')

log = logging.getLogger("dag_universidades_e")


def extract_data(query_name, csv_name):
    '''
    Extract data from Postgresql database using SQLAlchemy and finally, save csv file.
    Required:
        query_name: name of file sql that is inside of the folder sql.
        csv_name: name of csv file that will store in a folder called csv.
    '''
    engine = create_engine(
        config('DIALECT')+"://"+
        config('POSTGRESQL_USER')+":"+
        config('POSTGRESQL_PASSWORD')+"@" +
        config('POSTGRESQL_HOST')+"/"+
        config('POSTGRESQL_DB'))

    path_sql = os.path.abspath(os.path.join(
        os.path.dirname(__file__), '..', 'sql', query_name))

    with engine.connect() as conn:
        file = open(path_sql, 'r', encoding='utf-8')
        query = text(file.read())
        resultset = conn.execute(query)
        df = pd.DataFrame(resultset.fetchall())
        df.columns = resultset.keys()
        logging.info("query has been executed successfully")

    folder_csv = os.path.abspath(os.path.join(
        os.path.dirname(__file__), '..', 'files'))
    if not os.path.exists(folder_csv):
        logging.info("folder was created successfully")
        os.mkdir(folder_csv)
    else:
        logging.info("folder already exist")

    filename = csv_name
    path_csv = os.path.join(folder_csv, filename)
    df.to_csv(path_csv, encoding='utf-8', index=False)
    logging.info(filename + " was created successfully")


default_args = {
    'retries': 5,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    'dag_universidades_e',
    default_args=default_args,
    description='Uni e',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2022, 3, 29)
) as dag:

    # Extract data from Postgresql
    extract_data_pampa = PythonOperator(
        task_id='execute_query_inter',
        python_callable=extract_data,
        op_kwargs={
            'query_name': 'query_nacional_pampa.sql',
            'csv_name': 'nacional_pampa.csv'})

    extract_data_inter = PythonOperator(
        task_id='execute_query_pampa',
        python_callable=extract_data,
        op_kwargs={
            'query_name': 'query_interamericana.sql',
            'csv_name': 'interamericana.csv'})

     # Transform data with Pandas
    tranform_inter = PythonOperator(
        task_id='transform_data_interamericana',
        python_callable=processing_data_inter)

    transform_nacional = PythonOperator(
        task_id='transform_data_nacionalpampa',
        python_callable=processing_data_pampa)

    # Upload data to AWS
    upload_data_interamericana = PythonOperator(
        task_id='upload_data_uni_interamericana',
        python_callable=upload_data_inter,
        op_kwargs={
            'local_file': '/universidad_nacional_pampa.txt',
            'bucket': config('AWS_BUCKET_NAME'),
            's3_file': 's3_universidad_nacional_pampa.txt'})

    upload_data_nacional_pampa = PythonOperator(
        task_id='upload_data_uni_nacional_pampa',
        python_callable=upload_data_nacional,
        op_kwargs={
            'local_file': '/universidad_interamericana.txt',
            'bucket': config('AWS_BUCKET_NAME'),
            's3_file': 's3_universidad_interamericana.txt'})

    extract_data_inter >> tranform_inter >> upload_data_interamericana,
    extract_data_pampa >> transform_nacional >> upload_data_nacional_pampa
