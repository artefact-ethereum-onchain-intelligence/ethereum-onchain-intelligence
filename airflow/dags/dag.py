from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta




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
    "example_dag",
    default_args=default_args,
    description="A simple example DAG",
    schedule_interval="0 12 * * *",  
    catchup=False,
) as dag:

   
    task_hello = PythonOperator(
        task_id="print_hello",
        python_callable=print_hello,
    )

    # Set task dependencies
    task_hello  

