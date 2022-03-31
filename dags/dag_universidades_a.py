import logging
import os
from datetime import datetime, timedelta
from time import strftime

import pandas as pd
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from decouple import config
from sqlalchemy import create_engine
from sqlalchemy.sql import text


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

def query_to_csv(**kwargs):  #With this function I will create de .csv files of Universities A
   
    conn = connect_function()
    # root folder
    root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    print(root_dir)
    # Create 'include/temp' folder if not exist
    new_folder = os.path.join(root_dir, 'include', 'tmp')
    os.makedirs(new_folder, exist_ok=True)

    file_path = os.path.join(root_dir,'sql',kwargs['sql_file'])
    print(file_path)
    with open(file_path) as file:
        query = text(file.read())
        result = conn.execute(query)
        df = pd.DataFrame(result.fetchall())
        df.columns = result.keys()
        csv_path = os.path.join(root_dir,'include', 'tmp', kwargs['file_name'])
        df.to_csv(csv_path, sep = ',', index=False)

logging.basicConfig(level=logging.INFO, datefmt=strftime("%Y-%m-%d"),
                    format='%(asctime)s - %(name)s - %(message)s')

logger = logging.getLogger("Universidades_A")

default_args = {
    'retries': 5,  # Try in 5 opportunities to run the script
    'retry_delay': timedelta(minutes=5)  # Wait 5 minutes to try to run the script again
}

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
    tarea_3 = DummyOperator(task_id="Process_Data")      # PythonOperator to process the data with Pandas
    tarea_4 = DummyOperator(task_id="Charge_Data")  # Charge the data with S3Operator

    [tarea_1, tarea_2] >> tarea_3 >> tarea_4
