import logging
from typing import Any, Dict, List, Optional

from constants import DEFAULT_LOCATION, FRIENDLY_NAMES, GCS_OBJECTS, TABLE_NAMES, TASK_IDS

from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator

# Configure logger
logger = logging.getLogger(__name__)


def create_transaction_schema() -> List[Dict[str, str]]:
    """
    Create the schema for transaction data in BigQuery

    Returns:
        List containing the schema fields
    """
    return [
        {"name": "blockNumber", "type": "STRING"},
        {"name": "timeStamp", "type": "STRING"},
        {"name": "hash", "type": "STRING"},
        {"name": "nonce", "type": "STRING"},
        {"name": "blockHash", "type": "STRING"},
        {"name": "transactionIndex", "type": "STRING"},
        {"name": "from", "type": "STRING"},
        {"name": "to", "type": "STRING"},
        {"name": "value", "type": "STRING"},
        {"name": "gas", "type": "STRING"},
        {"name": "gasPrice", "type": "STRING"},
        {"name": "isError", "type": "STRING"},
        {"name": "txreceipt_status", "type": "STRING"},
        {"name": "input", "type": "STRING"},
        {"name": "contractAddress", "type": "STRING"},
        {"name": "cumulativeGasUsed", "type": "STRING"},
        {"name": "gasUsed", "type": "STRING"},
        {"name": "confirmations", "type": "STRING"},
    ]


def create_dataset(
    task_id: str, dataset_id: str, location: str, gcp_conn_id: str = "google_cloud_default"
) -> BigQueryCreateEmptyDatasetOperator:
    """
    Create a BigQuery dataset

    Args:
        task_id: Airflow task ID
        dataset_id: BigQuery dataset ID
        location: GCP location for the dataset
        gcp_conn_id: Airflow connection ID for GCP

    Returns:
        BigQueryCreateEmptyDatasetOperator
    """
    return BigQueryCreateEmptyDatasetOperator(
        task_id=task_id, dataset_id=dataset_id, gcp_conn_id=gcp_conn_id, location=location, exists_ok=True
    )


def create_source_tasks(
    source_name: str,
    file_path: str,
    bucket_name: str,
    dataset_id: str,
    schema_fields: List[Dict],
    gcp_conn_id: str = "google_cloud_default",
) -> Dict:
    """
    Create tasks for a specific data source

    Args:
        source_name: Name of the data source (e.g., 'UNI', 'Uniswap_Universal_Router')
        file_path: Path to the source file
        bucket_name: The GCS bucket name
        dataset_id: The BigQuery dataset ID
        schema_fields: The schema for the BigQuery table
        gcp_conn_id: Airflow connection ID for GCP

    Returns:
        Dict containing the uploader and loader operators for this source
    """
    logger.info(f"Creating tasks for source {source_name} with file path: {file_path}")

    # Get GCS object name, table name, and friendly name from centralized config
    # Fall back to sensible defaults if the source is not in our standard list
    gcs_object_name = GCS_OBJECTS.get(source_name, f"transactions_{source_name}.json")
    table_name = TABLE_NAMES.get(source_name, f"{source_name.lower()}_transactions")
    friendly_name = FRIENDLY_NAMES.get(source_name, source_name.lower())

    # Create uploader task
    try:
        uploader_task = LocalFilesystemToGCSOperator(
            task_id=TASK_IDS["upload_template"].format(friendly_name),
            src=file_path,
            dst=gcs_object_name,
            bucket=bucket_name,
            gcp_conn_id=gcp_conn_id,
        )
        logger.info(f"Created uploader task for {source_name}")
    except Exception as e:
        logger.error(f"Error creating uploader task for {source_name}: {e}", exc_info=True)
        raise

    # Create loader task
    try:
        loader_task = GCSToBigQueryOperator(
            task_id=TASK_IDS["load_template"].format(friendly_name),
            gcp_conn_id=gcp_conn_id,
            bucket=bucket_name,
            source_objects=[gcs_object_name],
            destination_project_dataset_table=f"{dataset_id}.{table_name}",
            schema_fields=schema_fields,
            write_disposition="WRITE_TRUNCATE",
            source_format="NEWLINE_DELIMITED_JSON",
            autodetect=True,
        )
        logger.info(f"Created loader task for {source_name}")
    except Exception as e:
        logger.error(f"Error creating loader task for {source_name}: {e}", exc_info=True)
        raise

    return {"uploader": uploader_task, "loader": loader_task, "table_name": table_name}


def create_loading_tasks(
    extraction_results: Dict[str, str],
    dataset_id: Optional[str] = None,
    bucket_name: Optional[str] = None,
    gcp_conn_id: str = "google_cloud_default",
) -> Dict:
    """
    Create all necessary loading tasks and return them in a dictionary.
    This function can be used in a task group.

    Args:
        extraction_results: Dictionary containing paths to extracted data files
        dataset_id: The BigQuery dataset ID (required)
        bucket_name: The GCS bucket name (required)
        gcp_conn_id: Airflow connection ID for GCP

    Returns:
        Dict containing all created operators
    """
    # Initialize result structure
    result: Dict[str, Any] = {"tables": {}, "upload_load_pairs": []}

    # Validate required parameters
    if not dataset_id:
        raise ValueError("dataset_id is required")
    if not bucket_name:
        raise ValueError("bucket_name is required")

    # Create dataset first (always needed)
    dataset_creator = create_dataset(
        task_id=TASK_IDS["create_dataset"], dataset_id=dataset_id, location=DEFAULT_LOCATION, gcp_conn_id=gcp_conn_id
    )
    result["dataset_creator"] = dataset_creator

    # Log initial configuration
    logger.info(f"Creating loading tasks with dataset_id={dataset_id}, bucket_name={bucket_name}")
    logger.info(f"Received extraction_results of type: {type(extraction_results)}")

    # Handle XCom objects
    if hasattr(extraction_results, "key"):
        logger.info("Detected XCom object, will be resolved by Airflow at runtime")
        return result

    # Get schema for transactions
    transaction_schema = create_transaction_schema()

    # Process each source in extraction_results
    for source_name, file_path in extraction_results.items():
        try:
            # Create tasks for this source
            source_tasks = create_source_tasks(
                source_name=source_name,
                file_path=file_path,
                bucket_name=bucket_name,
                dataset_id=dataset_id,
                schema_fields=transaction_schema,
                gcp_conn_id=gcp_conn_id,
            )

            # Use a simplified key from our constants
            key_prefix = FRIENDLY_NAMES.get(source_name, source_name.lower().replace("_", ""))

            # Add tasks to result
            result[f"{key_prefix}_uploader"] = source_tasks["uploader"]
            result[f"{key_prefix}_loader"] = source_tasks["loader"]

            # Store dependency information
            result["upload_load_pairs"].append({"uploader": source_tasks["uploader"], "loader": source_tasks["loader"]})

            # Add table name to tables dict
            result["tables"][f"{key_prefix}_table"] = source_tasks["table_name"]

            logger.info(f"Created tasks for source {source_name}")

        except Exception as e:
            logger.error(f"Error creating tasks for source {source_name}: {e}", exc_info=True)

    logger.info(f"Created loading tasks for {len(result['upload_load_pairs'])} sources")
    return result


def set_task_dependencies(loading_tasks: Dict[str, Any]) -> None:
    """
    Set dependencies between loader tasks

    Args:
        loading_tasks: Dictionary of tasks returned by create_loading_tasks

    Returns:
        None, dependencies are set in place
    """
    if not loading_tasks:
        logger.warning("No loading tasks to set dependencies for")
        return

    # Get the dataset creator task
    dataset_creator = loading_tasks.get("dataset_creator")
    if not dataset_creator:
        logger.warning("No dataset creator task found")
        return

    # Create standard task dependencies - each loader depends on its uploader
    for pair in loading_tasks.get("upload_load_pairs", []):
        try:
            uploader = pair.get("uploader")
            loader = pair.get("loader")

            if uploader and loader:
                # Then ensure the uploads happen before loads
                uploader >> loader
                logger.info(f"Set dependency: {uploader.task_id} >> {loader.task_id}")

                # Also ensure explicit dependency from dataset creation to BigQuery loading
                dataset_creator >> loader
                logger.info(f"Set dependency: {dataset_creator.task_id} >> {loader.task_id}")
        except Exception as e:
            logger.error(f"Error setting task dependencies: {e}", exc_info=True)
