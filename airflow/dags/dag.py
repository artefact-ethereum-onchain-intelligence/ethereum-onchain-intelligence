import logging
import os  # Add os import for environment variables
from datetime import datetime, timedelta
from typing import Any, Dict

from constants import UNI, UNISWAP_UNIVERSAL_ROUTER, UNISWAP_V2_ROUTER
from extraction_app import extract_data
from loading_app import create_loading_tasks, set_task_dependencies

from airflow.decorators import dag, task, task_group

# Configure logger
logger = logging.getLogger(__name__)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.cloud_storage import GCSUploadFileOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
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
@dag(
    "ethereum_dag",  # Changed from eutherium to ethereum (if that's what you meant)
    default_args=default_args,
    description="Extract, transform, load for Ethereum data",
    schedule_interval="0 12 * * *",  # Runs daily at noon
    catchup=False,
)
def ethereum_onchain_intelligence_dag() -> None:
    @task(task_id="extraction", multiple_outputs=True)
    def task_extraction() -> dict[str, str]:
        """Extract data from sources and return paths to output files"""
        result = extract_data()
        logger.info(f"Extraction complete, returning: {result}")
        return result

    @task_group(group_id="data_loading")
    def data_loading(uni_path: str, universal_router_path: str, v2_router_path: str) -> Dict[str, Any | None]:
        """Task group for loading processed data into BigQuery"""

        logger.info(f"UNI file path: {uni_path}")
        logger.info(f"Universal Router file path: {universal_router_path}")
        logger.info(f"V2 Router file path: {v2_router_path}")

        # Get required environment variables
        dataset_id = os.environ.get("BQ_DATASET_ID")
        bucket_name = os.environ.get("GCS_BUCKET_NAME")

        # Validate required environment variables
        if not dataset_id:
            raise ValueError("BQ_DATASET_ID environment variable is required")
        if not bucket_name:
            raise ValueError("GCS_BUCKET_NAME environment variable is required")

        logger.info(f"Using dataset_id={dataset_id}, bucket_name={bucket_name}")

        # Create a dictionary with extraction results
        extraction_results = {
            UNI: uni_path,
            UNISWAP_UNIVERSAL_ROUTER: universal_router_path,
            UNISWAP_V2_ROUTER: v2_router_path,
        }

        # Create all loading tasks using the loading module
        loading_tasks = create_loading_tasks(
            extraction_results=extraction_results,
            dataset_id=dataset_id,
            bucket_name=bucket_name,
            gcp_conn_id="google_cloud_default",
        )

        # Set up dependencies between tasks
        set_task_dependencies(loading_tasks)

        # The return statement creates XComs that can be pulled by downstream tasks
        return {
            "uni_table": loading_tasks["tables"].get("uni_table"),
            "universal_router_table": loading_tasks["tables"].get("universal_router_table"),
            "v2_router_table": loading_tasks["tables"].get("v2_router_table"),
        }



    @task
    def run_dbt_tests():
        command = f"cd {DBT_PROJECT_DIR} && dbt run --target dev && dbt test --target dev"
        result = subprocess.run(command, shell=True, check=True, capture_output=True)
        if result.returncode != 0:
            raise Exception(f"dbt test failed: {result.stderr.decode('utf-8')}")
        print(f"dbt test result: {result.stdout.decode('utf-8')}")

    @task(task_id="run_dbt")
    def run_dbt_command():

        command = "dbt run --project-dir /opt/airflow/dbt_project"  # Point to mounted dbt project
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        if result.returncode != 0:
            raise Exception(f"DBT run failed: {result.stderr}")
        print(result.stdout)


    dbt = run_dbt_command()


    # Get the result from the extraction task
    extraction_results = task_extraction()



    data_loading(
        uni_path=extraction_results[UNI],
        universal_router_path=extraction_results[UNISWAP_UNIVERSAL_ROUTER],
        v2_router_path=extraction_results[UNISWAP_V2_ROUTER],
    )


ethereum_onchain_intelligence_dag()
