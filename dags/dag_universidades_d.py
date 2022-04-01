from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator

default_args = {
    'owner': 'alkemy',
    'depends_on_past': False,
    'email': ['mail@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
}

with DAG(
    dag_id='dag_universidades_d',
    schedule_interval='@hourly',
    start_date=datetime(2022, 4, 16),
    catchup=False,
    default_args=default_args
        ) as dag:

    # PythonOperator: Get the data from the database and
    # save it as a csv file in the tmp folder.
    extract_task_utn = DummyOperator(
        task_id='extract_utn',
        dag=dag,
    )

    # PythonOperator: Get the data from the database and
    # save it as a csv file in the tmp folder.
    extract_task_tres_de_febrero = DummyOperator(
        task_id='extract_tres_de_febrero',
        dag=dag,
    )
    # PythonOperator: Read csv file and create dataframe,
    # process it, and save it in .txt file.
    process_task = DummyOperator(
        task_id='process',
        dag=dag,
    )

    # PythonOperator: Load to S3 server.
    upload_task = DummyOperator(
        task_id='upload',
        dag=dag,
    )

    [extract_task_utn, extract_task_tres_de_febrero] >> process_task >> upload_task
    