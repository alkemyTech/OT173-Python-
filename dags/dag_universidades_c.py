from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator

default_args = {
    'owner': 'alkemy',
    'depends_on_past': False,
    'email': ['alkemy@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
}

with DAG(
        dag_id='dag_universidades_c',
        schedule_interval='@hourly',
        start_date=datetime(2022, 3, 27),
        catchup=False,
        default_args=default_args
        ) as dag:

    # PythonOperator and PostgresHook to get the data from the database and
    #  save it in a csv file in the include/tmp folder of the project.
    extract_task_palermo = DummyOperator(
        task_id='extract_palermo',
        dag=dag,
    )

    # PythonOperator and PostgresHook to get the data from the database and
    #  save it in a csv file in the include/tmp folder of the project.
    extract_task_jujuy = DummyOperator(
        task_id='extract_jujuy',
        dag=dag,
    )
    # PythonOperator and pandas to read the csv file and create a /dataframe.
    # the procesed data are saved in .txt file
    transform_task = DummyOperator(
        task_id='transform',
        dag=dag,
    )

    # PythonOperator and boto3 to upload the .txt file to S3.
    # The file is deleted after the upload.
    load_task = DummyOperator(
        task_id='load',
        dag=dag,
    )

    [extract_task_palermo, extract_task_jujuy] >> transform_task >> load_task
