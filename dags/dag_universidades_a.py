import logging
from time import strftime
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator

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
    tarea_1 = DummyOperator(task_id="Query_Flores")      # PythonOperator to do the query
    tarea_2 = DummyOperator(task_id="Query_Villa_Maria")  # PythonOperator to do the query
    tarea_3 = DummyOperator(task_id="Process_Data")      # PythonOperator to process the data with Pandas
    tarea_4 = DummyOperator(task_id="Charge_Data")  # Charge the data with S3Operator

    [tarea_1, tarea_2] >> tarea_3 >> tarea_4
