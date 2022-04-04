import logging
from datetime import date, datetime, timedelta
from pathlib import Path
from time import strftime

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from decouple import config
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

# Directories
root_dir = Path(__file__).resolve().parent.parent


def connect_db():
    """ Connect to DataBase """

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

    # call function
    connection = connect_db()

    # root_dir = Path(__file__).resolve().parent.parent
    file_path = Path(f'{root_dir}/sql/{kwargs["sql_file"]}')

    with open(file_path) as f:
        # passing a plain string directly to Connection.execute() is deprecated
        # and we should use text() to specify a plain SQL query string instead.
        sql_text = text(f.read())
        result = connection.execute(sql_text)
        df = pd.DataFrame(result.fetchall())
        df.columns = result.keys()
        Path(f"{root_dir}/csv").mkdir(parents=True, exist_ok=True)
        file_csv = df.to_csv(f"{root_dir}/csv/{kwargs['file_name']}", index=False, encoding='utf-8')

    logger.info('Getting data')

    # return df
    return file_csv


def data_process(**kwargs):
    """ Process data from 'kenedy.csv' and 'sociales.csv' files in 'df'
        and create a '.txt' file

        Args:
            kenedy.csv
            sociales.csv
    """

    # Read files.csv
    df_sociales = pd.read_csv(f"{root_dir}/csv/{kwargs['sociales']}", encoding='utf-8')
    df_kenedy = pd.read_csv(f"{root_dir}/csv/{kwargs['kenedy']}", encoding='utf-8')

    # Read "codigos_postales.csv" to merge with df_kenedy
    df_cp = pd.read_csv(f"{root_dir}/csv/codigos_postales.csv", encoding='utf-8')
    df_cp.rename(columns = {'codigo_postal': 'postal_code', 'localidad': 'location'}, inplace = True)
    df_cp['location'] = df_cp['location'].apply(lambda x: x.lower().strip(' '))
    df_cp['postal_code'] = df_cp['postal_code'].astype(str)

    Path(f"{root_dir}/txt").mkdir(parents=True, exist_ok=True)  # create txt directory

    sort_columns = [
                'university',
                'career',
                'inscription_date',
                'first_name',
                'last_name',
                'gender',
                'age',
                'postal_code',
                'location',
                'email'
                ]

    delete_abreviations = {
                    'mr. ': '',
                    'dr. ': '',
                    'mrs. ': '',
                    'ms. ': '',
                    'md ': '',
                    'dds ': '',
                    'jr. ': '',
                    'dvm ': '',
                    'phd ': ''
                    }

    def age(birth_date):
        ''' Calculate age with the birth date '''

        birth_date = datetime.strptime(birth_date, '%Y-%m-%d').date()
        today = date.today()
        age = today.year - birth_date.year - ((today.month, today.day) < (birth_date.month, birth_date.day))

        if age <= 0:
            age += 100

        return age

    # TRANFORM DATA
    # university: str minúsculas, sin espacios extras, ni guiones
    # career: str minúsculas, sin espacios extras, ni guiones
    # name: str minúscula y sin espacios, ni guiones
    # last_name: str minúscula y sin espacios, ni guiones
    # location: str minúscula sin espacios extras, ni guiones
    # email: str minúsculas, sin espacios extras, ni guiones

    # >>>> FACULTAD LAT. DE CIENCIAS SOCIALES <<<<
    for column in df_sociales[['university', 'career', 'name', 'location', 'email']]:
        df_sociales[column] = df_sociales[column].apply(lambda x: x.lower().replace('-', ' ').strip(' '))

    # Split name in "first_name" & "last_name"
    # delete abreviations in name column
    for abreviation, blank in delete_abreviations.items():
        df_sociales['name'] = df_sociales['name'].apply(lambda x: x.replace(abreviation, blank))

    new = df_sociales['name'].str.split(' ', n=1, expand=True)  # new data frame with split value columns
    df_sociales['first_name'] = new[0]  # making separate first_name
    df_sociales["last_name"]= new[1]  # making separate last_name column from new data frame
    df_sociales.drop(columns=['name'], inplace=True)  # Dropping old Name columns

    # inscription_date: str %Y-%m-%d format / age: %Y-%m-%d format
    for column in df_sociales[['inscription_date', 'age']]:
        df_sociales[column] = df_sociales[column].apply(lambda x: datetime.strftime(
                                                                  datetime.strptime(x, '%d-%m-%Y'), '%Y-%m-%d'))
    
    df_sociales['age'] = df_sociales['age'].apply(age)  # age: int

    # gender: str choice(male, female)
    df_sociales['gender'] = df_sociales['gender'].apply(lambda x: x.replace('M', 'male')
                                                                   .replace('F', 'female')).astype('category')

    # postal_code: str
    df_sociales['postal_code']= df_sociales['postal_code'].astype(str)

    # Sort columns
    df_sociales = df_sociales[sort_columns]

    # Save data
    df_sociales.to_csv(f"{root_dir}/csv/new_sociales.csv", index=False, encoding='utf-8')
    df_sociales.to_csv(f"{root_dir}/txt/sociales.txt", index=False, encoding='utf-8')

    # >>>> UNIVERSIDAD J. F. KENNEDY <<<<
    for column in df_kenedy[['university', 'career', 'name', 'email']]:
        df_kenedy[column] = df_kenedy[column].apply(lambda x: x.lower().replace('-', ' ').strip(' '))

    # Split name in "first_name" & "last_name"
    for abreviation, blank in delete_abreviations.items():  # delete abreviations in name column
        df_kenedy['name'] = df_kenedy['name'].apply(lambda x: x.replace(abreviation, blank)) 
    
    new = df_kenedy['name'].str.split(' ', n=1, expand=True)  # new data frame with split value columns
    df_kenedy['first_name'] = new[0]  # making separate first_name
    df_kenedy["last_name"] = new[1]  # making separate last_name column from new data frame
    df_kenedy.drop(columns=['name'], inplace=True)  # Dropping old Name columns

    # inscription_date: str %Y-%m-%d format / age: %Y-%m-%d format
    for column in df_kenedy[['inscription_date', 'age']]:
        df_kenedy[column] = df_kenedy[column].apply(lambda x: datetime.strftime(datetime.strptime(x, '%y-%b-%d'), '%Y-%m-%d'))

    df_kenedy['age'] = df_kenedy['age'].apply(age)

    # gender: str choice(male, female)
    df_kenedy['gender'] = df_kenedy['gender'].apply(lambda x: x.replace('m', 'male')
                                                               .replace('f', 'female')).astype('category')

    # postal_code: str & merge data
    df_kenedy['postal_code'] = df_kenedy['postal_code'].astype(str)

    df_kenedy = df_kenedy.merge(df_cp, how='left', on='postal_code')

    # Sort columns
    df_kenedy = df_kenedy[sort_columns]

    # Save data
    df_kenedy.to_csv(f"{root_dir}/csv/new_kenedy.csv", index=False, encoding='utf-8')
    df_kenedy.to_csv(f"{root_dir}/txt/kenedy.txt", index=False, encoding='utf-8')

    logger.info('Data was processed!')


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
    dataprocess_task = PythonOperator(
        task_id='data_process',
        python_callable=data_process,
        op_kwargs={
            'sociales': 'sociales.csv',
            'kenedy': 'kenedy.csv'
        }
    )
    savedata_task = PythonOperator(task_id='save_data', python_callable=save_data)

    [getdata_sociales_task, getdata_kennedy_task] >> dataprocess_task >> savedata_task
