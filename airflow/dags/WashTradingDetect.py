import logging
import os  # Add os import for environment variables
from dataclasses import dataclass
from logging.handlers import RotatingFileHandler
from typing import List, Optional, Tuple

import numpy as np
import pandas as pd
from google.cloud import bigquery
from sklearn.cluster import DBSCAN
from sklearn.preprocessing import StandardScaler

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/opt/airflow/config/application_credentials.json"


@dataclass
class WashTradingDetect:
    big_query_connect_details: Optional[bigquery.Client]
    eps: float
    min_samples: int
    features_sublist: List[str]
    table_id_list: List[str]
    logger: logging.Logger

    def big_query_connector(self: "WashTradingDetect") -> pd.DataFrame:
        """Connect to BigQuery and fetch data from specified tables."""
        client = self.big_query_connect_details

        if not isinstance(client, bigquery.Client):
            error_message = (
                f"Invalid connection type: received {type(client).__name__}, " f"expected {bigquery.Client.__name__}."
            )
            self.logger.error(error_message)
            raise TypeError(error_message)
        else:
            self.logger.info("Connection is valid.")

        all_dataframes = []
        for table_id in self.table_id_list:
            # Use f-string to insert table name directly into the query
            # Ensure table_id is properly validated if it comes from external sources
            # In this case, it's controlled internally, so direct formatting is safe.
            query = f"SELECT * FROM `{table_id}`"  # noqa: S608
            # Remove job_config as parameterization is no longer needed for the table name
            sql_request_to_dataframe = client.query(query).to_dataframe()
            all_dataframes.append(sql_request_to_dataframe)

        self.logger.info("SQL connection and script are OK.")

        merged_data_table = pd.concat(all_dataframes, ignore_index=True)
        return merged_data_table

    def select_features(self: "WashTradingDetect", merged_data_table: pd.DataFrame) -> pd.DataFrame:
        """Select relevant features for analysis."""
        raw_data_inputs = merged_data_table[self.features_sublist]
        return pd.DataFrame(raw_data_inputs)

    def process_features(self: "WashTradingDetect", raw_data_inputs: pd.DataFrame) -> np.ndarray:
        """Process and transform features for analysis."""
        raw_data_inputs["timeStamp"] = pd.to_datetime(raw_data_inputs["timeStamp"], unit="s")

        # Convert timestamp to numeric feature
        raw_data_inputs["time_delta"] = (
            raw_data_inputs["timeStamp"] - raw_data_inputs["timeStamp"].min()
        ).dt.total_seconds()

        self.logger.info(
            f"Time delta range: {raw_data_inputs['time_delta'].min()} - " f"{raw_data_inputs['time_delta'].max()}"
        )

        features = raw_data_inputs[["value", "time_delta"]].values
        return features

    def detect_wash_transaction(
        self: "WashTradingDetect", features: np.ndarray, raw_data_inputs: pd.DataFrame
    ) -> Tuple[pd.DataFrame, np.ndarray, np.ndarray]:
        """Detect potential wash trading using DBSCAN clustering."""
        scaler = StandardScaler()
        features_scaled = scaler.fit_transform(features)

        dbscan = DBSCAN(eps=self.eps, min_samples=self.min_samples)
        clusters = dbscan.fit_predict(features_scaled)

        raw_data_inputs["cluster"] = clusters

        # Generate flat files for plotting in Front-End
        raw_data_inputs.to_csv("full_data.csv")
        # x (float)
        np.savetxt("x_value_column.csv", features_scaled[:, 0], delimiter=",", fmt="%f")
        # y (integer)
        raw_data_inputs["time_delta"].to_csv("y_time_delta_column.csv")
        # Cluster label (integer)
        np.savetxt("cluster_number.csv", clusters, delimiter=",", fmt="%d")

        return raw_data_inputs, features_scaled, clusters


def runner() -> None:
    """Main function to run the wash trading detection analysis."""
    # Create logger
    logger = logging.getLogger("connection_logger")
    logger.setLevel(logging.INFO)

    file_handler = RotatingFileHandler("logfile.log", maxBytes=500 * 1024, backupCount=5)

    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(console_handler)

    big_query_connect_details = bigquery.Client()
    eps = 0.90
    min_samples = 300
    features_sublist = ["blockNumber", "timeStamp", "nonce", "transactionIndex", "from", "to", "value"]

    # Split long table IDs for better readability
    base_path = "etherium-on-chain-intelligence.ethereum_data_onchain_intelligence"
    table_id_list = [
        f"{base_path}.uni_transactions_cleaned",
        f"{base_path}.uniswap_universal_router_transactions_cleaned",
        f"{base_path}.uniswap_v2_router_transactions_cleaned",
    ]

    logger.info("Starting data extraction...")

    wash_class_object = WashTradingDetect(
        big_query_connect_details=big_query_connect_details,
        eps=eps,
        min_samples=min_samples,
        features_sublist=features_sublist,
        table_id_list=table_id_list,
        logger=logger,
    )

    merged_data_table = wash_class_object.big_query_connector()
    raw_data_inputs = wash_class_object.select_features(merged_data_table)
    features = wash_class_object.process_features(raw_data_inputs)

    raw_data_inputs, features_scaled, clusters = wash_class_object.detect_wash_transaction(features, raw_data_inputs)

    logger.info("Ending Data computations.")


if __name__ == "__main__":
    runner()
