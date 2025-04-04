"""
Constants used across the Ethereum On-Chain Intelligence project.
This module centralizes constants to avoid duplication and make maintenance easier.
"""

# Ethereum contract addresses constants (all caps for constants following Python convention)
UNI = "UNI"
UNISWAP_UNIVERSAL_ROUTER = "Uniswap_Universal_Router"
UNISWAP_V2_ROUTER = "Uniswap_V2_Router"

# Contract address mapping - maps constants to actual addresses
CONTRACT_ADDRESSES = {
    UNI: "0x1f9840a85d5aF5bf1D1762F925BDADdC4201F984",
    UNISWAP_UNIVERSAL_ROUTER: "0xEf1c6E67703c7BD7107eed8303Fbe6EC2554BF6B",
    UNISWAP_V2_ROUTER: "0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D"
}

# Friendly name mapping for consistent key naming throughout the codebase
FRIENDLY_NAMES = {
    UNI: "uni",
    UNISWAP_UNIVERSAL_ROUTER: "universal_router",
    UNISWAP_V2_ROUTER: "v2_router"
}

# BigQuery table name constants
UNI_TABLE = "uni_transactions"
UNIVERSAL_ROUTER_TABLE = "uniswap_universal_router_transactions"
V2_ROUTER_TABLE = "uniswap_v2_router_transactions"

# Default GCS object names
UNI_GCS_OBJECT = "transactions_UNI.json"
UNIVERSAL_ROUTER_GCS_OBJECT = "transactions_Uniswap_Universal_Router.json"
V2_ROUTER_GCS_OBJECT = "transactions_Uniswap_V2_Router.json"

# Mapping source names to their GCS objects
GCS_OBJECTS = {
    UNI: UNI_GCS_OBJECT,
    UNISWAP_UNIVERSAL_ROUTER: UNIVERSAL_ROUTER_GCS_OBJECT,
    UNISWAP_V2_ROUTER: V2_ROUTER_GCS_OBJECT
}

# Mapping source names to their BigQuery table names
TABLE_NAMES = {
    UNI: UNI_TABLE,
    UNISWAP_UNIVERSAL_ROUTER: UNIVERSAL_ROUTER_TABLE,
    UNISWAP_V2_ROUTER: V2_ROUTER_TABLE
}

# Default values
DEFAULT_LOCATION = "europe-west1"

# Common task IDs to maintain consistency
TASK_IDS = {
    "create_dataset": "create_dataset",
    "upload_template": "upload_{}_to_gcs",
    "load_template": "load_{}_to_bigquery"
} 