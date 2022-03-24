from datetime import timedelta

default_args = {
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}
