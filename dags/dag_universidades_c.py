import logging
import os
from datetime import datetime, timedelta
from venv import create

import pandas as pd
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from decouple import config
from sqlalchemy import create_engine, text

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(message)s',
    datefmt='%Y-%m-%d'
    )

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

default_args = {
    'owner': 'alkemy',
    'depends_on_past': False,
    'email': ['alkemy@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        dag_id='dag_universidades_c',
        schedule_interval='@hourly',
        start_date=datetime.now() - timedelta(days=1),
        catchup=False,
        default_args=default_args
        ) as dag:

    base_path = os.path.abspath(
            os.path.join(
                os.path.dirname(__file__),
                ".."))

    path_tmp = os.path.abspath(os.path.join(base_path, 'include', 'tmp'))

    def extract(query, university):
        """PythonOperator and PostgresHook to get the data from the
        database and save it in a csv file in the include/tmp folder
        of the project.

        Args:
            query (str): The query file name and extension to be executed\
                in the database.
            example: query_universidades_c.sql.
            university (str): The name of the university to be extracted.
            example: 'universidad_de_los_andes'.
        """

        db_user = config('DB_USER')
        db_password = config('DB_PASSWORD')
        db_host = config('DB_HOST')
        db_port = config('DB_PORT')
        db_database = config('DB_DATABASE')
        db_url = f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:\
            {db_port}/{db_database}"
        logger.info(f"Connecting to database")
        os.makedirs(path_tmp, exist_ok=True)
        path_csv = os.path.join(path_tmp, university)
        path_query = os.path.join(base_path, 'sql', query)
        with open(path_query, 'r') as file:
            logger.info(f"Reading query file")
            engine = create_engine(db_url)
            logger.info("create engine")
            df_to_csv = pd.read_sql(text(file.read()), engine)
            logger.info("read query")
            df_to_csv.to_csv(path_csv)
            logger.info("write csv")

    def transform_palermo(university, csv_file):
        """PythonOperator and Pandas to transform the data in the csv file

        Args
            university (str): The name of the university to be saved in the\
                txt file.
            csv_file (str): the csv file to be transformed.

        Returns:
            _type_: _description_
        """
        logger.info(f"Transforming {university}")
        path_csv = os.path.join(path_tmp, csv_file)
        logger.info(f"Reading csv file")
        # Read the data
        df = pd.read_csv(path_csv, sep=',', header=0, index_col=0)
        # normalize the data
        logger.info(f"Normalizing data")
        for col in df[['university', 'career', 'name', 'email']]:
            df[col] = df[col].str.lower()
            df[col] = df[col].str.replace("-", " ")
            df[col] = df[col].str.replace("_", " ")
            df[col] = df[col].str.strip()
        # replace name values
        logger.info(f"Replacing name values")
        df['name'] = df['name'].str.lower()
        df['name'] = df['name'].str.replace("dr.", " ")
        df['name'] = df['name'].str.replace("dra.", " ")
        df['name'] = df['name'].str.replace("ms.", " ")
        df['name'] = df['name'].str.replace("mrs.", " ")
        df['name'] = df['name'].str.strip()
        # replace gender "f" and "m" for female an male
        df['gender'] = df['gender'].str.lower()
        df['gender'] = df['gender'].str.replace("-", " ")
        df['gender'] = df['gender'].str.replace("f", "female")
        df['gender'] = df['gender'].str.replace("m", "male")
        df['gender'] = df['gender'].str.strip()
        # postal_code to string
        df['postal_code'] = df['postal_code'].astype(str)
        logger.info('split name to generate first_name and last_name')
        # split name to generate first_name and last_name
        df['first_name'] = df['name'].str.split().str.get(0)
        df['last_name'] = df['name'].str.split().str.get(1)
        logger.info('split name to generate first name and last_name')
        df = df.drop('name', axis=1)
        logger.info('drop name')

        def calculate_age(age_date):
            """
            Calculate age from date of birth
            Parameters
            ----------
            age_date : str
                date of birth
            """
            logger.info(f"Calculating age from date of birth")
            nac = datetime.strptime(age_date, '%d/%b/%y').date()
            if nac > datetime.today().date():
                nac2 = nac.strftime(f'{nac.year-100}-%m-%d')
                return (
                    datetime.today().date().year - datetime.strptime(
                        nac2,
                        '%Y-%m-%d').date().year)
            else:
                return (
                    datetime.today().date().year-datetime.strptime(
                        age_date,
                        '%d/%b/%y').date().year)

        df['age'] = df['age'].apply(calculate_age)

        path_txt = os.path.abspath(
            os.path.join(base_path, 'include', university)
            )
        logger.info(f"Saving {university}")
        df.to_csv(path_txt, sep=',', index=False)
        logger.info(f"Saved {university}")

    # PythonOperator to get the data from the database and
    #  save it in a csv file in the include/tmp folder of the project.
    extract_task_palermo = PythonOperator(
        task_id='extract_palermo',
        python_callable=extract,
        op_kwargs={
            'query': 'query_palermo.sql',
            'university': 'palermo.csv'
        }
    )
    extract_task_jujuy = PythonOperator(
        task_id='extract_jujuy',
        python_callable=extract,
        op_kwargs={
            'query': 'query_universidad_nacional_de_jujuy.sql',
            'university': 'universidad_nacional_de_jujuy.csv'}
    )
    # PythonOperator and pandas to read the csv file and create a /dataframe.
    # the procesed data are saved in .txt file
    transform_task_palermo = PythonOperator(
        task_id='transform_palermo',
        python_callable=transform_palermo,
        op_kwargs={
            'university': 'palermo.txt',
            'csv_file': 'palermo.csv'
        },
        dag=dag,
    )

    # PythonOperator and boto3 to upload the .txt file to S3.
    # The file is deleted after the upload.
    load_task = DummyOperator(
        task_id='load',
        dag=dag,
    )

    [
        extract_task_palermo,
        extract_task_jujuy
        ] >> transform_task_palermo >> load_task
