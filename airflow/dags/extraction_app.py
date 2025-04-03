import requests
from dotenv import load_dotenv
import os
from dataclasses import dataclass
#from loguru import logger
import logging
import json
from logging.handlers import RotatingFileHandler


@dataclass
class FetchJSONData():
    
    
    credentials_dict: dict
    logger : logging.Logger    
    
    
    def generate_json_file(self, data, filename):
        
        # Save the response to a file
        output_dir = os.getenv("EXTRACTION_DIR")
        file_path = os.path.join(output_dir, filename)
        with open(file_path, 'w') as f:
            json.dump(data, f, indent=4)
        
        self.logger.info(f'JSON file generated: {file_path}')
    
    
    def call_api_data(self)->json:
        
        for idx, address in enumerate(self.credentials_dict['addresses']):
            
            params = {
                'module': 'account',
                'action': 'txlist',
                'address': address,
                'startblock': 0,
                'endblock': 99999999,
                'page': 1,
                'offset': 10000,  # Number of results to return
                'sort': 'asc',
                'apikey': self.credentials_dict['api_key']
            }

            # Requête pour obtenir la liste des transactions
            response = requests.get(self.credentials_dict['transactions_url'], params=params)
            
            assert response.status_code == 200
            if response.status_code != 200:
                self.logger.error(f"Bad response status: {response.status_code}")
                raise ValueError(f"API call failed with status: {response.status_code}")
            else:
                self.logger.info("Status code assertion passed with success.")
            
            data = response.json()
            # Assert that the data is a dict (JSON object)
            assert isinstance(data, dict)        
            if not isinstance(data, dict):
                self.logger.error(f"Expected dict, got {type(data)}")
                raise TypeError("Returned data is not a JSON object")        
            else:
                self.logger.info("Data type assertion passed.")
            
            # Pretty print the response
            #print(json.dumps(data, indent=4))
            
            # Short name for filename
            #short_addr = f"{credentials_dict['addresses_names'][idx]}"
            short_addr = self.credentials_dict['addresses_names'][idx]
            filename = f"transactions_{short_addr}.json"
            
            self.generate_json_file(data, filename)     
            
        return data
    

# Function to be called by Airflow
def extract_data():
    
    # Create logger
    logger = logging.getLogger("etherscan_logger")
    logger.setLevel(logging.INFO)

    # Avoid adding multiple handlers if extract_data is called multiple times
    if not logger.handlers:
        # Rotating file handler
        file_handler = RotatingFileHandler(
            "logfile.log",
            maxBytes=500 * 1024,
            backupCount=5
        )
        file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logger.addHandler(file_handler)

        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logger.addHandler(console_handler)

    logger.info("Starting data extraction...")

    load_dotenv()
    key = os.getenv("ETHERSCAN_API_KEY")

    if not key:
        logger.error("ETHERSCAN_API_KEY not found in environment variables")
        raise EnvironmentError("ETHERSCAN_API_KEY not found in environment variables")

    addresses_list = [
        '0x1f9840a85d5aF5bf1D1762F925BDADdC4201F984',
        '0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D',
        '0xEf1c6E67703c7BD7107eed8303Fbe6EC2554BF6B'
    ]
    
    names = ['UNI', 'Uniswap_V2_Router', 'Uniswap_Universal_Router']

    # Class attributes
    credentials_dict = {
        'api_key': key,
        'addresses': addresses_list,
        'transactions_url': 'https://api.etherscan.io/api',
        'addresses_names': names
    }

    # Object
    fetcher = FetchJSONData(credentials_dict, logger)
    # Method call
    fetcher.call_api_data()

    logger.info("Data extraction finished!")

if __name__ == "__main__":
    # Run
    extract_data()