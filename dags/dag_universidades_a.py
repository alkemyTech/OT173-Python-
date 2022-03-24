from datetime import timedelta

default_args = {
    'retries': 5,  # Try in 5 opportunities to run the script
    'retry_delay': timedelta(minutes=5)  # Wait 5 minutes to try to run the script again
}
