import logging
from datetime import date, datetime, timedelta
import pandas as pd
import os

from airflow import DAG
from airflow.operators.dummy import DummyOperator

logging.basicConfig(level=logging.INFO, datefmt=("%Y-%m-%d"),
                    format='%(asctime)s - %(levelname)s - %(message)s')

log = logging.getLogger("dag_universidades_e")

# added attribute default_args into DAG: default_args = default_args.
default_args = {
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'dag_universidades_e',
    default_args=default_args,
    description='Tasks universities group e',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2022, 4, 15)
) as dag:
    # Using PythonOperator:
    # Extract data from Postgresql
    execute_query_inter = DummyOperator(task_id='execute_query_inter')
    execute_query_pampa = DummyOperator(task_id='execute_query_pampa')

    # Transform data with Pandas
    convert_to_csv = DummyOperator(task_id='convert_to_csv')
    preprocessing_data = DummyOperator(task_id='preprocessing_data')
    convert_to_txt = DummyOperator(task_id='convert_to_txt')

    # Load .txt to S3 server
    upload_data = DummyOperator(task_id='upload_data')

    execute_query_inter >> convert_to_csv >> preprocessing_data >> convert_to_txt >> upload_data,
    execute_query_pampa >> convert_to_csv >> preprocessing_data >> convert_to_txt >> upload_data


def processing_data_inter():
    """
    Data normalization of University Interamericana.
    Saved data as universidad_interamericana.txt
    """
    folder_csv = os.path.abspath(os.path.join(
        os.path.dirname(__file__), '..', 'files'))

    df = pd.read_csv(folder_csv + "/interamericana.csv")
    df_cod = pd.read_csv(folder_csv + "/codigos_postales.csv")

    # Split full name in two columns first_name and last_name: str
    df.name = df.name.str.lower()

    # Delete abreviaton
    abreviations = ['mr.-', 'dr.-', 'mrs.-', 'ms.-',
                    'md-', 'dds-', 'jr.-', 'dvm-', 'phd-']

    for x in abreviations:
        df.name = df.name.str.replace(x, ' ', regex=False)

    df.name = df.name.str.strip()

    # Split full name in two columns first_name and last_name: str
    new = df["name"].str.split("-", n=1, expand=True)

    df["first_name"] = new[0]
    df["last_name"] = new[1]

    df.drop(columns=["name"], inplace=True)

    # Get postal_code through location : str
    df.location = df.location.str.lower()
    df.location = df.location.str.replace(' ', '')
    df.location = df.location.str.replace('-', ' ')

    df_cod.rename(columns={'localidad': 'location'}, inplace=True)
    df_cod.rename(columns={'codigo_postal': 'postal_code'}, inplace=True)

    df_cod.location = df_cod.location.str.lower()
    df_cod.location = df_cod.location.str.replace('-', ' ')

    df_complete = pd.merge(df, df_cod, on='location', how='left')
    df_complete.postal_code = df_complete.postal_code.astype(str)

    # location: str minúscula sin espacios extras, ni guiones
    df.location = df.location.str.lower()
    df.location = df['location'].str.replace('-', ' ')

    # university: str minúsculas, sin espacios extras, ni guiones
    # career: str minúsculas, sin espacios extras, ni guiones
    # location: str minúscula sin espacios extras, ni guiones
    # email: str minúsculas, sin espacios extras, ni guiones
    def clean_data(df):
        df = df.str.lower()
        df = df.str.replace(' ', '')
        df = df.str.replace('-', ' ')
        df = df.str.strip()
        return df

    df_complete[[
        'university', 'career', 'first_name', 
        'last_name', 'email']] = df_complete[[
        'university', 'career', 'first_name', 
        'last_name', 'email']].apply(clean_data)

    # inscription_date: str %Y-%m-%d format
    df_complete['inscription_date'] = pd.to_datetime(
        df_complete.inscription_date)
    df_complete['inscription_date'] = pd.to_datetime(
        df_complete['inscription_date'].astype(str), format='%Y-%m-%d')

    # gender: str choice(male, female)
    df_complete.gender = df_complete['gender'].str.replace('M', 'male')
    df_complete.gender = df_complete['gender'].str.replace('F', 'female')

    # age: int
    def parse_date(date_str):
        '''
        Read correctly date format and fixed dates. 
        Format receive: year two digits / 3 first letter of month / day number
        Fixed dates example: wrong date 40 = 2040. correct date 40 = 1940
        '''
        parsed = datetime.strptime(date_str, '%y/%b/%d')
        current_date = datetime.now()
        if parsed > current_date:
            parsed = parsed.replace(year=parsed.year - 100)
        return parsed

    def get_age(DoB):
        '''
        Get age through Day of Birth(DoB)
        '''
        today = date.today()
        return today.year - DoB.year - ((today.month, today.day) < (DoB.month, DoB.day))

    df_complete['date_fixed'] = df_complete['age'].apply(parse_date)
    df_complete['age'] = df_complete['date_fixed'].apply(get_age)
    df_complete.drop(columns=["date_fixed"], inplace=True)

    # Sort columns
    df_complete = df_complete[[
        'university', 'career', 'postal_code', 'location', 'inscription_date', 
        'first_name', 'last_name',  'gender', 'age', 'email']]

    df_complete.to_csv(
        folder_csv + "/universidad_interamericana.txt", index=False, encoding='utf-8')


def processing_data_pampa():
    """
    Data normalization of University Nacional Pampa.
    Saved data as universidad_nacional_pampa.txt
    """
    folder_csv = os.path.abspath(os.path.join(
        os.path.dirname(__file__), '..', 'files'))

    df = pd.read_csv(folder_csv + "/nacional_pampa.csv")
    df_cod = pd.read_csv(folder_csv + "/codigos_postales.csv")

    # Split full name in two columns first_name and last_name: str
    df.name = df.name.str.lower()

    # Delete abreviaton
    abreviations = ['mr. ', 'dr. ', 'mrs. ', 'ms. ',
                    'md ', 'dds ', 'jr. ', 'dvm ', 'phd ']

    for x in abreviations:
        df.name = df.name.str.replace(x, '', regex=False)

    new = df["name"].str.split(" ", n=1, expand=True)

    df["first_name"] = new[0]
    df["last_name"] = new[1]

    df.drop(columns=["name"], inplace=True)

    # Get the location through postal_code : str
    df_cod.rename(columns={'localidad': 'location'}, inplace=True)
    df_cod.rename(columns={'codigo_postal': 'postal_code'}, inplace=True)

    df_complete = pd.merge(df, df_cod, on='postal_code', how='left')
    df_complete.postal_code = df_complete.postal_code.astype(str)

    # university: str minúsculas, sin espacios extras, ni guiones
    # career: str minúsculas, sin espacios extras, ni guiones
    # location: str minúscula sin espacios extras, ni guiones
    # email: str minúsculas, sin espacios extras, ni guiones
    def clean_data(df):
        df = df.str.lower()
        return df

    df_complete[[
        'university', 'career', 'first_name', 
        'last_name', 'location', 'email'
        ]] = df_complete[[
            'university', 'career', 'first_name', 
            'last_name', 'location', 'email']].apply(clean_data)

    # inscription_date: str %Y-%m-%d format
    df_complete['inscription_date'] = pd.to_datetime(
        df_complete['inscription_date'], infer_datetime_format=True)
    df_complete['inscription_date'] = df_complete['inscription_date'].dt.strftime(
        '%Y-%m-%d').astype(str)

    # gender: str choice(male, female)
    df_complete.gender = df_complete['gender'].str.replace('M', 'male')
    df_complete.gender = df_complete['gender'].str.replace('F', 'female')

    # age: int
    def parse_date(date_str):
        '''
        Read correctly date format and fixed dates. 
        Format receive: day number / month number 01-12 / year in 4 digits
        '''
        parsed = datetime.strptime(date_str, '%d/%m/%Y')
        current_date = datetime.now()
        if parsed > current_date:
            parsed = parsed.replace(year=parsed.year - 100)
        return parsed

    def get_age(DoB):
        '''
        Get age through Day of Birth(DoB)
        '''
        today = date.today()
        return today.year - DoB.year - ((today.month, today.day) < (DoB.month, DoB.day))

    df_complete['date_fixed'] = df_complete['age'].apply(parse_date)
    df_complete['age'] = df_complete['date_fixed'].apply(get_age)
    df_complete.drop(columns=["date_fixed"], inplace=True)

    df_complete = df_complete[[
        'university', 'career', 'postal_code',  'location', 'inscription_date',
        'first_name', 'last_name', 'gender', 'age', 'email']]

    df_complete.to_csv(
        folder_csv + "/universidad_nacional_pampa.txt", index=False, encoding='utf-8')