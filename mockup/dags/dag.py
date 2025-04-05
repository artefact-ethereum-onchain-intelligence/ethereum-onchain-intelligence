from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
from extraction_app import extract_data
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.operators.bash import BashOperator
import os
import subprocess

BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "default-bucket-name")
DESTINATION_TABLE = os.getenv("BQ_DESTINATION_TABLE", "default_dataset.default_table")
SOURCE_OBJECT = os.getenv("GCS_SOURCE_OBJECT", "default_source_object.json")
DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", "/path/to/dbt/project")

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
        
    @task
    def upload_to_gcs():
        upload_file_to_gcs = LocalFilesystemToGCSOperator(
            task_id='upload_file_to_gcs',
            bucket_name=BUCKET_NAME,
            object_name=SOURCE_OBJECT,
            filename='data/file',
        )
        upload_file_to_gcs.execute(context={})
        
    @task(task_id="transfer from bucket to big query")
    def upload_to_bq():
        gcs_to_bq_task = GCSToBigQueryOperator(
            task_id="gcs_to_bigquery",
            bucket=BUCKET_NAME,
            source_objects=[SOURCE_OBJECT],
            destination_project_dataset_table=DESTINATION_TABLE,
            write_disposition="WRITE_TRUNCATE",
            source_format="NEWLINE_DELIMITED_JSON",
            autodetect=True,
            dag=dag,  # Attach to DAG
        )
        gcs_to_bq_task.execute(context={})
    
    @task
    def run_dbt_tests():
        command = f"cd {DBT_PROJECT_DIR} && dbt run --target dev && dbt test --target dev"
        result = subprocess.run(command, shell=True, check=True, capture_output=True)
        if result.returncode != 0:
            raise Exception(f"dbt test failed: {result.stderr.decode('utf-8')}")
        print(f"dbt test result: {result.stdout.decode('utf-8')}")
        

    # extraction_to_vm = task_extraction()
    # from_vm_to_bucket = upload_to_gcs()
    # from_bucket_to_bq = upload_to_bq()
    test = run_dbt_tests()
    
    test

