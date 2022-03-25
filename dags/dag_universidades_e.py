from datetime import timedelta

# add attribute default_args into DAG: default_args = default_args.
default_args = {
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}
