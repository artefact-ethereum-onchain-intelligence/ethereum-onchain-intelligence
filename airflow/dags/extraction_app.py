import json

# from loguru import logger
import logging
import os
from dataclasses import dataclass
from logging.handlers import RotatingFileHandler
from typing import Any, Dict

import requests
from dotenv import load_dotenv


@dataclass
class FetchJSONData:
    credentials_dict: dict
    logger: logging.Logger

    def generate_json_file(self: "FetchJSONData", data: Dict[str, Any], filename: str) -> str:
        # Save the response to a file
        output_dir = os.getenv("EXTRACTION_DIR")
        if output_dir is None:
            output_dir = "."
        file_path = os.path.join(output_dir, filename)

        # Check if the 'result' key exists in the data
        if "result" in data and isinstance(data["result"], list):
            # Write as JSONL (JSON Lines) format for BigQuery compatibility
            with open(file_path, "w") as f:
                for transaction in data["result"]:
                    json_line = json.dumps(transaction)
                    f.write(json_line + "\n")
        else:
            # Fallback to regular JSON if not expected format
            with open(file_path, "w") as f:
                json.dump(data, f, indent=4)

        self.logger.info(f"JSON file generated: {file_path}")

        return file_path

    def call_api_data(self: "FetchJSONData") -> dict:
        file_paths = {}
        for idx, address in enumerate(self.credentials_dict["addresses"]):
            params = {
                "module": "account",
                "action": "txlist",
                "address": address,
                "startblock": 0,  # TODO: increment this with every runnning
                "endblock": 99999999,  # TODO: increment this with every runnning
                "page": 1,
                "offset": 10000,  # Number of results to return
                "sort": "asc",
                "apikey": self.credentials_dict["api_key"],
            }

            # Requête pour obtenir la liste des transactions
            response = requests.get(self.credentials_dict["transactions_url"], params=params, timeout=30)

            if response.status_code != 200:
                self.logger.error(f"Bad response status: {response.status_code}")
                raise ValueError(f"API call failed with status: {response.status_code}")
            else:
                self.logger.info("Status code assertion passed with success.")

            data = response.json()
            # Validate that the data is a dict (JSON object)
            if not isinstance(data, dict):
                self.logger.error(f"Expected dict, got {type(data)}")
                raise TypeError("Returned data is not a JSON object")
            else:
                self.logger.info("Data type validation passed.")

            # Pretty print the response
            # print(json.dumps(data, indent=4))

            # Short name for filename
            # short_addr = f"{credentials_dict['addresses_names'][idx]}"
            data_name = self.credentials_dict["addresses_names"][idx]
            filename = f"transactions_{data_name}.json"

            output_file_path = self.generate_json_file(data, filename)
            file_paths[data_name] = output_file_path

        return file_paths


# Function to be called by Airflow
def extract_data() -> Dict[str, str]:
    # Create logger
    logger = logging.getLogger("etherscan_logger")
    logger.setLevel(logging.INFO)

    # Avoid adding multiple handlers if extract_data is called multiple times
    if not logger.handlers:
        # Rotating file handler
        file_handler = RotatingFileHandler("logfile.log", maxBytes=500 * 1024, backupCount=5)
        file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
        logger.addHandler(file_handler)

        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
        logger.addHandler(console_handler)

    logger.info("Starting data extraction...")

    load_dotenv()
    key = os.getenv("ETHERSCAN_API_KEY")

    if not key:
        logger.error("ETHERSCAN_API_KEY not found in environment variables")
        raise EnvironmentError("ETHERSCAN_API_KEY not found in environment variables")

    addresses_list = [
        "0x1f9840a85d5aF5bf1D1762F925BDADdC4201F984",
        "0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D",
        "0xEf1c6E67703c7BD7107eed8303Fbe6EC2554BF6B",
    ]

    names = ["UNI", "Uniswap_V2_Router", "Uniswap_Universal_Router"]

    # Class attributes
    credentials_dict = {
        "api_key": key,
        "addresses": addresses_list,
        "transactions_url": "https://api.etherscan.io/api",
        "addresses_names": names,
    }

    # Object
    fetcher = FetchJSONData(credentials_dict, logger)
    # Method call
    output_file_paths_dict = fetcher.call_api_data()
    logger.info(f"File paths6: {output_file_paths_dict}")  # TODO: remove this

    logger.info("Data extraction finished!")

    return output_file_paths_dict


if __name__ == "__main__":
    # Run
    extract_data()
