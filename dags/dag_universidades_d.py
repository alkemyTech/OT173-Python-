from datetime import datetime, timedelta
import logging
from os import path

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from decouple import config
import pandas as pd
import psycopg2

default_args = {
    "owner": "alkemy",
    "depends_on_past": False,
    "email": ["mail@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 5,
    "retry_delay": timedelta(minutes=5),
}

logging.basicConfig(format="%(asctime)s - %(name)s - %(message)s", datefmt="%Y-%m-%d")

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Main folder directory
mainFolder_dir = path.abspath(path.join(path.dirname(__file__), ".."))


def saveCSVfromDB(sql_FileName, csv_Name):
    """
    Gets the data from database and saves it as a csv file in the files folder
    of the project.
    Args:
        sql_FileName (str): The sql file name to query the database.

        csv_Name (str): Output name for the extracted csv file.
    """

    host = config("DB_HOST")
    port = config("DB_PORT")
    db = config("DB_NAME")
    db_user = config("DB_USER")
    db_password = config("DB_PASSWORD")

    logger.info("Connecting to database...")
    conn = psycopg2.connect(
        host=host, database=db, port=port, user=db_user, password=db_password
    )

    logger.info("Saving .csv file...")
    with open(f"{mainFolder_dir}/sql/{sql_FileName}.sql") as f:
        sql_FileName = f.read()
        df_query = pd.read_sql_query(sql_FileName, conn)
        df_query.to_csv(f"{mainFolder_dir}/files/{csv_Name}.csv", index=False)


def process(university):
    """
    Transforms and processes the data and then saves it as a txt file in the files folder of the project.
    Args:
        university (str): name of the university.
    """

    logger.info("Making dataframe from csv...")
    df = pd.read_csv(
        f"{mainFolder_dir}/files/extract_{university}.csv",
        sep=",",
        header=0,
        index_col=None,
    )

    logger.info("Normalizing university, career, email columns...")
    # Normalizing university, career and email columns
    for col in df[["university", "career", "email"]]:
        df[col] = df[col].str.lower()
        df[col] = df[col].str.replace("-", " ").replace("_", " ")
        df[col] = df[col].str.strip()

    logger.info("first_name, last_name columns...")
    # first_name, last_name columns
    name_sep = "_" if university == "tres_de_febrero" else " "
    df["name"] = df["name"].str.lower()
    df["first_name"] = df["name"].str.split(name_sep, expand=True)[0]
    df["last_name"] = df["name"].str.split(name_sep, expand=True)[1]
    df.drop("name", axis=1, inplace=True)

    logger.info("Replacing gender char with respective gender...")
    # Replacing gender char with respective gender
    df["gender"] = df["gender"].map({"m": "male", "f": "female"})

    logger.info("Calculating age...")
    # Calculating age
    if university != "utn":
        df["inscription_date"] = df["inscription_date"].apply(
            lambda x: (datetime.strptime(x, "%d/%b/%y").strftime("%d/%m/%Y"))
        )
        df["age"] = df["age"].apply(
            lambda x: (datetime.strptime(x, "%d/%b/%y").strftime("%Y/%m/%d"))
        )

    df["age"] = pd.to_datetime(df["age"])
    df["age"] = df["age"].apply(
        lambda x: (
            (datetime.now().year - x.year)
            if (datetime.now().year - x.year) > 0
            else 100 + (datetime.now().year - x.year)
        )
    )

    logger.info("Formatting Inscription date...")
    # Formatting inscription date
    df["inscription_date"] = pd.to_datetime(
        df["inscription_date"], format="%Y/%m/%d" if university == "utn" else "%d/%m/%Y"
    )
    df["inscription_date"] = df["inscription_date"].dt.strftime("%Y-%m-%d")

    logger.info("Saving .txt file...")
    df.to_csv(
        f"{mainFolder_dir}/files/transform_{university}.txt", sep=",", index=False
    )


def multiProcess(listUniversities):
    for x in listUniversities:
        process(x)


with DAG(
    dag_id="dag_universidades_d",
    schedule_interval="@hourly",
    start_date=datetime(2022, 4, 16),
    catchup=False,
    default_args=default_args,
) as dag:

    # PythonOperator: Get the data from the database and
    # save it as a csv file in the tmp folder.
    extract_task_utn = PythonOperator(
        task_id="extract_utn",
        python_callable=saveCSVfromDB,
        op_args=["query_utn", "extract_utn"],
        dag=dag,
    )

    # PythonOperator: Get the data from the database and
    # save it as a csv file in the tmp folder.
    extract_task_tres_de_febrero = PythonOperator(
        task_id="extract_tres_de_febrero",
        python_callable=saveCSVfromDB,
        op_args=["query_tres_de_febrero", "extract_tres_de_febrero"],
        dag=dag,
    )
    # PythonOperator: Read csv file and create dataframe,
    # process it, and save it in .txt file.
    process_task = PythonOperator(
        task_id="process",
        python_callable=multiProcess,
        op_args=[["utn", "tres_de_febrero"]],
        dag=dag,
    )

    # PythonOperator: Load to S3 server.
    upload_task = DummyOperator(
        task_id="upload",
        dag=dag,
    )

    [extract_task_utn, extract_task_tres_de_febrero] >> process_task >> upload_task
