import logging
import os
from datetime import date, datetime, timedelta
from os import path
from time import strftime

import boto3
import pandas as pd
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from botocore.exceptions import ClientError
from decouple import config
from sqlalchemy import create_engine
from sqlalchemy.sql import text

logging.basicConfig(level=logging.INFO, datefmt=strftime("%Y-%m-%d"),
                    format='%(asctime)s - %(name)s - %(message)s')

logger = logging.getLogger("Universidades_A")

default_args = {
    'retries': 5,  # Try in 5 opportunities to run the script
    'retry_delay': timedelta(minutes=5)  # Wait 5 minutes to try to run the script again
}


# With this function I will create de engine to conect to the Database.
def connect_function():

    # I take the credentials from .env:
    db_database = config('DB_DATABASE')
    db_host = config('DB_HOST')
    db_password = config('DB_PASSWORD')
    db_port = config('DB_PORT')
    db_user = config('DB_USER')

    # Return the engine
    engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_database}')

    return(engine.connect())


def query_to_csv(**kwargs):  # With this function I will create de .csv files of Universities A

    conn = connect_function()
    # root folder
    root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    print(root_dir)
    # Create 'include/temp' folder if not exist
    new_folder = os.path.join(root_dir, 'include', 'tmp')
    os.makedirs(new_folder, exist_ok=True)

    file_path = os.path.join(root_dir, 'sql', kwargs['sql_file'])
    print(file_path)
    with open(file_path) as file:
        query = text(file.read())
        result = conn.execute(query)
        df = pd.DataFrame(result.fetchall())
        df.columns = result.keys()
        csv_path = os.path.join(root_dir, 'include', 'tmp', kwargs['file_name'])
        df.to_csv(csv_path, sep=',', index=False)


root_folder = path.abspath(path.join(path.dirname(__file__), ".."))


# With this function we can obtain the age of Villa Maria database
def age_calc2(born):
    born = datetime.strptime(born, "%d-%b-%y").date()
    today = date.today()
    age = today.year - born.year - ((today.month, today.day) < (born.month, born.day))
    if age < 0:
        age += 100
    return age


# With this function we can obtain the age of Flores database
def age_calc(born):
    born = datetime.strptime(born, "%Y-%m-%d").date()
    today = date.today()
    age = today.year - born.year - ((today.month, today.day) < (born.month, born.day))
    return age


# This function process de data of Flores database
def processing_flores():
    df_flores = pd.read_csv(f'{root_folder}/include/tmp/flores.csv')

    for col in df_flores.columns:
        if col != 'age':
            df_flores[col] = df_flores[col].astype('string')

    columnas = ['university', 'career', 'name', 'gender', 'postal_code', 'email']
    for col in columnas:
        df_flores[col] = df_flores[col].apply(lambda x: x.lower().replace('-', ' ').strip(' '))

    df_flores['gender'] = df_flores['gender'].apply(lambda x: x.lower().replace('m', 'male')
                                                    .replace('f', 'female').strip(' '))
    df_flores['age'] = df_flores['age'].apply(age_calc)

    titles = {
                    'mr.': '',
                    'dr.': '',
                    'mrs.': '',
                    'ms.': '',
                    'md': '',
                    'dds': '',
                    'jr.': '',
                    'dvm': '',
                    'phd': ''
                }

    for title, blank in titles.items():
        df_flores['name'] = df_flores['name'].apply(lambda x: x.replace(title, blank))

    df_flores['name'] = df_flores['name'].apply(lambda x: x.strip(' '))
    df_flores['name'] = df_flores['name'].astype('string')
    df_flores['first_name'] = df_flores['name'].apply(lambda x: x.split(' ')[0])
    df_flores['last_name'] = df_flores['name'].apply(lambda x: x.split(' ')[-1])
    df_flores = df_flores.drop(['name'], axis=1)

    df_flores = df_flores[['university', 'career', 'inscription_date',
                           'first_name', 'last_name',
                           'gender', 'age', 'postal_code',
                           'email']]

    df_flores.to_csv(f'{root_folder}/include/tmp/flores.txt', index=None)


# This function process de data of Villa Maria database
def processing_villa_maria():
    df_villa_maria = pd.read_csv(f'{root_folder}/include/tmp/villa_maria.csv')
    df_cp = pd.read_csv(f'{root_folder}/files/codigos_postales.csv', encoding='utf-8')
    df_cp.rename(columns={'localidad': 'location', 'codigo_postal': 'postal_code'}, inplace=True)
    df_cp['location'] = df_cp['location'].apply(lambda x: x.lower().strip(' '))

    for col in df_villa_maria.columns:
        if col != 'age':
            df_villa_maria[col] = df_villa_maria[col].astype('string')

    columnas = ['university', 'career', 'name', 'gender', 'location', 'email']
    for col in columnas:
        df_villa_maria[col] = df_villa_maria[col].apply(lambda x: x.lower().replace('-', ' ').strip(' '))

    df_villa_maria['gender'] = df_villa_maria['gender'].apply(lambda x: x.lower()
                                                              .replace('m', 'male').replace('f', 'female').strip(' '))

    df_villa_maria['age'] = df_villa_maria['age'].apply(age_calc2)

    df_villa_maria = df_villa_maria.merge(df_cp, on='location', how='left')

    titles = {
                    'mr.': '',
                    'dr.': '',
                    'mrs.': '',
                    'ms.': '',
                    'md': '',
                    'dds': '',
                    'jr.': '',
                    'dvm': '',
                    'phd': ''
                }

    for title, blank in titles.items():
        df_villa_maria['name'] = df_villa_maria['name'].apply(lambda x: x.replace(title, blank))

    df_villa_maria['name'] = df_villa_maria['name'].apply(lambda x: x.strip(' '))
    df_villa_maria['name'] = df_villa_maria['name'].astype('string')
    df_villa_maria['first_name'] = df_villa_maria['name'].apply(lambda x: x.split('_')[0])
    df_villa_maria['last_name'] = df_villa_maria['name'].apply(lambda x: x.split('_')[-1])
    df_villa_maria = df_villa_maria.drop(['name'], axis=1)

    df_villa_maria['inscription_date'] = df_villa_maria['inscription_date'].apply(lambda x: datetime.strftime(
                                                                                    datetime.strptime(x, '%d-%b-%y'),
                                                                                    '%Y-%m-%d'))

    df_villa_maria = df_villa_maria[['university', 'career', 'inscription_date',
                                    'first_name', 'last_name', 'gender', 'age', 'postal_code', 'location', 'email']]

    df_villa_maria.to_csv(f'{root_folder}/include/tmp/villa_maria.txt', index=None)


# This function is for upload the file flores.txt to S3 bucket
def S3_flores():
    bucket_name = config('AWS_BUCKET_NAME')
    s3 = boto3.client('s3',
                      aws_access_key_id=config('AWS_PUBLIC_KEY'),
                      aws_secret_access_key=config('AWS_SECRET_KEY'),
                      )

    with open(f'{root_folder}/include/tmp/flores.txt', 'rb') as file:
        try:
            s3.upload_fileobj(file, bucket_name, 'S3_flores.txt')
        except ClientError as error:
            print(error)


# This function is for upload the file villa_maria.txt to S3 bucket
def S3_villa_maria():
    bucket_name = config('AWS_BUCKET_NAME')
    s3 = boto3.client('s3',
                      aws_access_key_id=config('AWS_PUBLIC_KEY'),
                      aws_secret_access_key=config('AWS_SECRET_KEY'),
                      )

    with open(f'{root_folder}/include/tmp/villa_maria.txt', 'rb') as file:
        try:
            s3.upload_fileobj(file, bucket_name, 'S3_villa_maria.txt')
        except ClientError as error:
            print(error)


with DAG(
    "dag_universidades_a",
    default_args=default_args,
    description="Tarea PT-173-28:Configurar un DAG sin operators para grupo de Universidades A",
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2022, 3, 19)
) as dag:
    tarea_1 = PythonOperator(
                    task_id='Query_Flores',
                    python_callable=query_to_csv,
                    op_kwargs={
                        'sql_file': 'query_flores.sql',
                        'file_name': 'flores.csv'
                        }
                    )
    tarea_2 = PythonOperator(
                    task_id='Query_Villa_Maria',
                    python_callable=query_to_csv,
                    op_kwargs={
                        'sql_file': 'query_villa_maria.sql',
                        'file_name': 'villa_maria.csv'
                        }
                    )
    tarea_3 = PythonOperator(
        task_id='Processing_Flores',
        python_callable=processing_flores,
        dag=dag
    )  # PythonOperator to process the data with Pandas
    tarea_4 = PythonOperator(
        task_id='Processing_Villa_Maria',
        python_callable=processing_villa_maria,
        dag=dag
    )  # PythonOperator to process data with Pandas
    tarea_5 = PythonOperator(
        task_id='Charging_S3_Flores',
        python_callable=S3_flores,
        dag=dag
    )  # Charge the data of Flores with S3Operator
    tarea_6 = PythonOperator(
        task_id='Charging_S3_Villa_Maria',
        python_callable=S3_villa_maria,
        dag=dag
    )  # Charge the data of Villa Maria with S3Operator

    tarea_1 >> tarea_3 >> tarea_5
    tarea_2 >> tarea_4 >> tarea_6