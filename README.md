# Ethereum On-Chain Intelligence

A tool for analyzing and deriving insights from Ethereum blockchain data.
detecting wash trading on AMM exchanges withinEthereum-like systems, based on the understanding that colluding addresses (perceived as the same entity).

## Overview

This project provides tools and utilities for gathering, analyzing, and visualizing on-chain Ethereum data to derive meaningful intelligence and insights.

## Features

- On-chain data collection and processing
- Analytics and metrics calculation
- Data visualization capabilities
- Real-time monitoring options

## Getting Started
- Install Node.js (if not already installed)
- Install Python (if not already installed)
- Install Docker (if not already installed)
- Tools needed : Git, PostgreSQL, Redis




### Prerequisites

- memory : 1234
- storage : 1234
- Node.js (v16 or higher)
- Access to an Ethereum node (local or remote)

### Installation
-Use terraform option 
- 


## Run Airflow

### Environment Variables

Before running Airflow, make sure to set up the required environment variables:

1. Create or update the `.env` file in the `airflow` directory:
   ```bash
   cd airflow
   touch .env
   ```

2. Add your Etherscan API key to the `.env` file:
   ```
   ETHERSCAN_API_KEY="your_etherscan_api_key_here"
   ```
   
   This API key is required for fetching data from Etherscan's API services.

3. Alternatively, you can set the environment variable directly:
   ```bash
   export ETHERSCAN_API_KEY="your_etherscan_api_key_here"
   ```

Note: Never commit your API keys to version control. The `.env` file is included in `.gitignore`.

To run Apache Airflow for workflow orchestration:

1. Navigate to the Airflow directory:
   ```bash
   cd airflow
   ```

2. Start Airflow using Docker Compose:
   ```bash
   docker-compose up -d
   ```

3. Access the Airflow web interface:
   - Open your browser and go to: http://localhost:8080
   - Default login credentials:
     - Username: airflow
     - Password: airflow

4. DAGs are stored in the `airflow/dags` directory
   - Place your workflow DAGs in this directory to have them automatically detected

5. To stop Airflow:
   ```bash
   docker-compose down
   ```

Note: Logs are stored in the `airflow/logs` directory and data in `airflow/data`. Both directories are gitignored.


