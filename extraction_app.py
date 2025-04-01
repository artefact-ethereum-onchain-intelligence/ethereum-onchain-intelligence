import requests
from dotenv import load_dotenv
import os
from dataclasses import dataclass
from loguru import logger
import json
import sys


@dataclass
class FetchJSONData():
    
    
    credentials_dict: dict    
    
    
    def generate_json_file(self, data, filename):
        
        # Save the response to a file
        with open(filename, 'w') as f:
            json.dump(data, f, indent=4)
        
        logger.info(f'JSON file generated: {filename}')
    
    
    def call_api_data(self)->json:
        
        for idx, address in enumerate(credentials_dict['addresses']):
            
            params = {
                'module': 'account',
                'action': 'txlist',
                'address': address,
                'startblock': 0,
                'endblock': 99999999,
                'page': 1,
                'offset': 10000,  # Number of results to return
                'sort': 'asc',
                'apikey': credentials_dict['api_key']
            }

            # Requête pour obtenir la liste des transactions
            response = requests.get(credentials_dict['transactions_url'], params=params)
            
            assert response.status_code == 200
            if response.status_code != 200:
                logger.error(f"Bad response status: {response.status_code}")
                raise ValueError(f"API call failed with status: {response.status_code}")
            else:
                logger.info("Status code assertion passed.")
            
            data = response.json()
            # Assert that the data is a dict (JSON object)
            assert isinstance(data, dict)        
            if not isinstance(data, dict):
                logger.error(f"Expected dict, got {type(data)}")
                raise TypeError("Returned data is not a JSON object")        
            else:
                logger.info("Data type assertion passed.")
            
            # Pretty print the response
            #print(json.dumps(data, indent=4))
            
            # Short name for filename (e.g., first 6 and last 4 characters)
            short_addr = f"{credentials_dict['addresses_names'][idx]}"
            filename = f"transactions_{short_addr}.json"
            
            self.generate_json_file(data, filename)     
            
        return data


if __name__ == "__main__":
    
    logger.add("logfile.log", rotation="500 KB")
        
    logger.info("Running the script...")
    
    load_dotenv()

    key = os.getenv("ETHERSCAN_API_KEY")
    
    addresses_list = ['0x1f9840a85d5aF5bf1D1762F925BDADdC4201F984', '0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D', '0xEf1c6E67703c7BD7107eed8303Fbe6EC2554BF6B']
    names = ['UNI', 'Uniswap V2 Router', 'Uniswap Universal Router']
    credentials_dict = {
                        'api_key': key, \
                        'addresses': addresses_list, \
                        'transactions_url' : 'https://api.etherscan.io/api', \
                        'addresses_names' : names
                        }
        
    data_fetch_object = FetchJSONData(credentials_dict)
    
    api_data = data_fetch_object.call_api_data()
 
    logger.info("... Ending the script!")
    
    
