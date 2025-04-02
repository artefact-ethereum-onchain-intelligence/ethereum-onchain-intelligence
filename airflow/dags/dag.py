from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from datetime import datetime, timedelta
from applications.extraction_app import extract_data


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 4, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@task(task_id="extraction")
def task_extraction(): 
  extract_data()
  
# Define the DAG
with DAG(
    "eutherium_dag",
    default_args=default_args,
    description="extract, transform, load",
    schedule_interval="0 12 * * *",  
    catchup=False,
) as dag:
    extraction = task_extraction()

    # Set task dependencies
    extraction  

