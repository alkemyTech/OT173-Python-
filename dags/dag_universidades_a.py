from datetime import timedelta, datetime

from airflow import DAG

from airflow.operators.dummy import DummyOperator

with DAG(
    "dag_universidades_a",
    description="Tarea PT-173-28:Configurar un DAG sin operators para grupo de Universidades A",
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2022, 3, 19)
) as dag:
    tarea_1 = DummyOperator(task_id="Query_Flores")      # PythonOperator to do the query
    tarea_2 = DummyOperator(task_id="Query_Villa_Maria")  # PythonOperator to do the query
    tarea_3 = DummyOperator(task_id="Process_Data")      # PythonOperator to process the data with Pandas
    tarea_4 = DummyOperator(task_id="Charge_Data")  # Charge the data with S3Operator

    [tarea_1, tarea_2] >> tarea_3 >> tarea_4
