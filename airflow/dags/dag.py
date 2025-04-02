from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
from extraction_app import extract_data

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 4, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define the DAG
with DAG(
    "ethereum_dag",  # Changed from eutherium to ethereum (if that's what you meant)
    default_args=default_args,
    description="Extract, transform, load for Ethereum data",
    schedule_interval="0 12 * * *",  # Runs daily at noon
    catchup=False,
) as dag:

    @task(task_id="extraction")
    def task_extraction(): 
        extract_data()

    extraction = task_extraction() 

