from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from ..applications.extraction_app import extract_data




def print_hello():
    print("Hello, Airflow!")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 4, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define the DAG
with DAG(
    "eutherium_dag",
    default_args=default_args,
    description="extract, transform, load",
    schedule_interval="0 12 * * *",  
    catchup=False,
) as dag:

   
    task_extraction = PythonOperator(
        task_id="extraction",
        python_callable=extract_data,
    )

    # Set task dependencies
    task_extraction  

