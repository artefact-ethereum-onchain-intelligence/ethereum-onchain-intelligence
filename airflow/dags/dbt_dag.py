import logging
import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

# Configure logger
logger = logging.getLogger(__name__)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 4, 1),
    "retries": 2,
    "execution_timeout": timedelta(seconds=30),
    "retry_delay": timedelta(seconds=5),
    "email_on_failure": True,
    "email_on_retry": False,
}

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
DBT_PROJECT_DIR = os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt_project")
DBT_LOG_PATH = os.path.join(DBT_PROJECT_DIR, "logs")


@task
def validate_environment() -> bool:
    """Validate that all required environment variables are set"""
    required_vars = ["GCP_PROJECT_ID", "BQ_DATASET_ID"]
    missing_vars = [var for var in required_vars if not os.environ.get(var)]

    if missing_vars:
        error_msg = f"Missing required environment variables: {', '.join(missing_vars)}"
        logger.error(error_msg)
        raise ValueError(error_msg)

    # Log environment information
    logger.info(f"Running with GCP_PROJECT_ID: {os.environ.get('GCP_PROJECT_ID')}")
    logger.info(f"Running with BQ_DATASET_ID: {os.environ.get('BQ_DATASET_ID')}")
    logger.info(f"DBT project directory: {DBT_PROJECT_DIR}")

    # Ensure the log directory exists
    os.makedirs(DBT_LOG_PATH, exist_ok=True)
    logger.info(f"DBT log directory: {DBT_LOG_PATH}")

    return True


# Define the DAG
@dag(
    "dbt_ethereum_workflow",
    default_args=default_args,
    description="Run DBT workflows for Ethereum data",
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
)
def dbt_ethereum_workflow_dag() -> None:
    """
    DAG for running dbt models, tests, and documentation for Ethereum data.

    This DAG is now triggered by the ethereum_dag.
    """

    # Validate environment before any DBT commands
    validate_env_task = validate_environment()

    # Task group for staging models
    with TaskGroup(group_id="staging_models") as staging_models:
        # Run staging models using BashOperator
        dbt_run_staging = BashOperator(
            task_id="run_staging_models",
            bash_command=(
                f"dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR} "
                f"--target dev --models staging --log-path {DBT_LOG_PATH}"
            ),
            cwd=DBT_PROJECT_DIR,
        )

        # Test staging models using BashOperator
        dbt_test_staging = BashOperator(
            task_id="test_staging_models",
            bash_command=(
                f"dbt test --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR} "
                f"--target dev --models staging --log-path {DBT_LOG_PATH}"
            ),
            cwd=DBT_PROJECT_DIR,
        )

        # Set task dependencies within the group
        dbt_run_staging >> dbt_test_staging

    # Generate documentation
    dbt_docs_generate = BashOperator(
        task_id="generate_documentation",
        bash_command=(
            f"dbt docs generate --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR} "
            f"--target dev --log-path {DBT_LOG_PATH}"
        ),
        cwd=DBT_PROJECT_DIR,
    )

    # Define task dependencies
    validate_env_task >> staging_models >> dbt_docs_generate


# Instantiate the DAG
dbt_ethereum_workflow_dag()
