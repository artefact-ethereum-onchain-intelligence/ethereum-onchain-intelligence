import time
from web3 import Web3
from web3.middleware import geth_poa_middleware
import networkx as nx
import argparse
import random
from requests.exceptions import HTTPError

def extract_uniswap_v2_data(web3, factory_address_v2, start_block, end_block, max_retries=5, retry_delay=1.0):
    """
    Extracts Uniswap V2 pool creation and swap data from the Ethereum blockchain.

    Args:
        web3 (Web3): A Web3 instance connected to an Ethereum node.
        factory_address_v2 (str): Address of the Uniswap V2 factory contract.
        start_block (int): The starting block number for data extraction.
        end_block (int): The ending block number for data extraction.
        max_retries (int): Maximum number of retries for API requests.
        retry_delay (float): Base delay between retries in seconds (will be exponentially increased).

    Returns:
        tuple: A tuple containing two lists:
            - swap_records (list): A list of swap transactions.
            - liquidity_events (list): A list of liquidity providing/removing events.
    """
    # Uniswap V2 ABI (Application Binary Interface)
    uniswap_factory_abi_v2 = [
        {
            "anonymous": False,
            "inputs": [
                {"indexed": True, "name": "token0", "type": "address"},
                {"indexed": True, "name": "token1", "type": "address"},
                {"indexed": False, "name": "pair", "type": "address"},
                {"indexed": False, "name": "arg4", "type": "uint256"},
            ],
            "name": "PairCreated",
            "type": "event",
        }
    ]

    uniswap_pool_abi_v2 = [
        {
            "anonymous": False,
            "inputs": [
                {"indexed": True, "name": "sender", "type": "address"},
                {"indexed": False, "name": "amount0In", "type": "uint256"},
                {"indexed": False, "name": "amount1In", "type": "uint256"},
                {"indexed": False, "name": "amount0Out", "type": "uint256"},
                {"indexed": False, "name": "amount1Out", "type": "uint256"},
                {"indexed": True, "name": "to", "type": "address"},
            ],
            "name": "Swap",
            "type": "event",
        },
        {
            "anonymous": False,
            "inputs": [
                {"indexed": True, "name": "sender", "type": "address"},
                {"indexed": False, "name": "amount0", "type": "uint256"},
                {"indexed": False, "name": "amount1", "type": "uint256"},
            ],
            "name": "Mint",  # Liquidity providing
            "type": "event",
        },
        {
            "anonymous": False,
            "inputs": [
                {"indexed": True, "name": "sender", "type": "address"},
                {"indexed": False, "name": "amount0", "type": "uint256"},
                {"indexed": False, "name": "amount1", "type": "uint256"},
            ],
            "name": "Burn",  # Liquidity removing
            "type": "event",
        },
    ]

    # Initialize contracts
    factory_contract_v2 = web3.eth.contract(
        address=factory_address_v2, abi=uniswap_factory_abi_v2
    )

    # Get all pool creation events
    pool_creation_events_v2 = []
    for retry in range(max_retries):
        try:
            pool_creation_events_v2 = factory_contract_v2.events.PairCreated.get_logs(
                fromBlock=start_block, toBlock=end_block
            )
            break
        except Exception as e:
            if retry < max_retries - 1:
                # Exponential backoff with jitter
                sleep_time = retry_delay * (2 ** retry) + random.uniform(0, 0.5)
                print(f"Rate limit hit, retrying in {sleep_time:.2f} seconds...")
                time.sleep(sleep_time)
            else:
                print(f"Failed to get pool creation events after {max_retries} retries: {e}")
                return [], []

    # Store pool addresses
    pool_addresses_v2 = [event.args.pair for event in pool_creation_events_v2]

    swap_records = []
    liquidity_events = []

    # Loop through each pool address
    for pool_address in pool_addresses_v2:
        try:
            pool_contract = web3.eth.contract(address=pool_address, abi=uniswap_pool_abi_v2)

            # Get swap events for the current pool
            swap_events = []
            for retry in range(max_retries):
                try:
                    swap_events = pool_contract.events.Swap.get_logs(
                        fromBlock=start_block, toBlock=end_block
                    )
                    break
                except Exception as e:
                    if retry < max_retries - 1:
                        sleep_time = retry_delay * (2 ** retry) + random.uniform(0, 0.5)
                        print(f"Rate limit hit for pool {pool_address}, retrying in {sleep_time:.2f} seconds...")
                        time.sleep(sleep_time)
                    else:
                        raise e

            # Get Mint and Burn events (liquidity changes)
            mint_events = []
            for retry in range(max_retries):
                try:
                    mint_events = pool_contract.events.Mint.get_logs(
                        fromBlock=start_block, toBlock=end_block
                    )
                    break
                except Exception as e:
                    if retry < max_retries - 1:
                        sleep_time = retry_delay * (2 ** retry) + random.uniform(0, 0.5)
                        print(f"Rate limit hit for pool {pool_address}, retrying in {sleep_time:.2f} seconds...")
                        time.sleep(sleep_time)
                    else:
                        raise e

            burn_events = []
            for retry in range(max_retries):
                try:
                    burn_events = pool_contract.events.Burn.get_logs(
                        fromBlock=start_block, toBlock=end_block
                    )
                    break
                except Exception as e:
                    if retry < max_retries - 1:
                        sleep_time = retry_delay * (2 ** retry) + random.uniform(0, 0.5)
                        print(f"Rate limit hit for pool {pool_address}, retrying in {sleep_time:.2f} seconds...")
                        time.sleep(sleep_time)
                    else:
                        raise e

            # Process Swap Events
            for event in swap_events:
                block_number = event.blockNumber
                amount0In = event.args.amount0In
                amount1In = event.args.amount1In
                amount0Out = event.args.amount0Out
                amount1Out = event.args.amount1Out
                sender = event.args.sender
                # Need to determine asset_in and asset_out.  Assume if amount0In > 0, then asset_in is token0
                if amount0In > 0:
                    asset_in = amount0In
                    asset_out = -amount1Out
                else:
                    asset_in = amount1In
                    asset_out = -amount0Out

                volume = amount0In if amount0In > amount1In else amount1In  # Use the larger of the two as volume
                swap_records.append(
                    (block_number, asset_in, asset_out, sender, volume, pool_address)
                )

            # Process Liquidity Events (Mint and Burn)
            for event in mint_events:
                block_number = event.blockNumber
                sender = event.args.sender
                amount0 = event.args.amount0
                amount1 = event.args.amount1
                liquidity_events.append(
                    {
                        "block_number": block_number,
                        "pool_address": pool_address,
                        "type": "Mint",
                        "sender": sender,
                        "amount0": amount0,
                        "amount1": amount1,
                    }
                )
            for event in burn_events:
                block_number = event.blockNumber
                sender = event.args.sender
                amount0 = event.args.amount0
                amount1 = event.args.amount1
                liquidity_events.append(
                    {
                        "block_number": block_number,
                        "pool_address": pool_address,
                        "type": "Burn",
                        "sender": sender,
                        "amount0": amount0,
                        "amount1": amount1,
                    }
                )
        except Exception as e:
            print(f"Error processing pool {pool_address}: {e}")
            # Add a small delay before processing the next pool
            time.sleep(0.2)
            continue

    return swap_records, liquidity_events


def extract_eth_transfers(web3, start_block, end_block, max_retries=20, retry_delay=1.0):
    """
    Extracts ETH transfer records from the Ethereum blockchain.

    Args:
        web3 (Web3): A Web3 instance connected to an Ethereum node.
        start_block (int): The starting block number for data extraction.
        end_block (int): The ending block number for data extraction.
        max_retries (int): Maximum number of retries for API requests.
        retry_delay (float): Base delay between retries in seconds.

    Returns:
        list: A list of ETH transfer records.
    """
    eth_transfers = []
    for block_number in range(start_block, end_block + 1):
        for retry in range(max_retries):
            try:
                block = web3.eth.get_block(block_number, full_transactions=True)
                for tx in block.transactions:
                    # Check if the transaction is an ETH transfer (value > 0)
                    if tx.value > 0 and tx.to is not None and tx["from"] is not None:
                        sender = tx["from"]
                        receiver = tx.to
                        amount = tx.value
                        eth_transfers.append((sender, receiver, "ETH", amount))
                # Small delay to avoid hitting rate limits
                time.sleep(0.05)
                break
            except Exception as e:
                if retry < max_retries - 1:
                    sleep_time = retry_delay * (2 ** retry) + random.uniform(0, 0.5)
                    print(f"Rate limit hit for block {block_number}, retrying in {sleep_time:.2f} seconds...")
                    time.sleep(sleep_time)
                else:
                    print(f"Error processing block {block_number} after {max_retries} retries: {e}")
                    break
    return eth_transfers


def create_eth_transfer_graph(eth_transfers):
    """
    Creates a directed graph representing ETH transfers between addresses.

    Args:
        eth_transfers (list): A list of ETH transfer records.

    Returns:
        networkx.Graph: A NetworkX graph representing the ETH transfer network.
    """
    eth_transfer_graph = nx.Graph()
    for sender, receiver, _, _ in eth_transfers:
        eth_transfer_graph.add_edge(sender, receiver)
    return eth_transfer_graph



def filter_public_service_addresses(eth_transfer_graph, public_service_addresses):
    """
    Removes public service addresses and their associated edges from the ETH transfer graph.

    Args:
        eth_transfer_graph (networkx.Graph): A NetworkX graph representing the ETH transfer network.
        public_service_addresses (list): A list of public service addresses.
    """
    for address in public_service_addresses:
        if address in eth_transfer_graph:
            eth_transfer_graph.remove_node(address)



def extract_flash_loan_transactions(web3, start_block, end_block):
    """
    Extracts flash loan transactions from the Ethereum blockchain.

    Args:
        web3 (Web3): A Web3 instance connected to an Ethereum node.
        start_block (int): The starting block number.
        end_block (int): The ending block number.

    Returns:
        list: A list of flash loan transactions.
    """
    flash_loan_transactions = []
    # Implementation will depend on the specific flash loan contracts.
    #  You'll need the ABI of the flash loan contracts you're interested in
    #  (e.g., dYdX, Aave, Uniswap FlashLoan).  This is highly non-trivial
    #  and контракт-specific.
    #  The paper mentions Wang et al. [54] for identifying Flash-loan transactions.
    #  That paper would need to be consulted and its methodology implemented.
    #  For now, return an empty list.
    return flash_loan_transactions

def get_public_service_addresses(web3):
    """
    Returns a list of DeFi and CeFi public service addresses.  In a real-world
    scenario, this would likely come from a database or a configuration file,
    and be kept up-to-date.  This is hardcoded for the example.

    Args:
        web3 (Web3): A Web3 instance.

    Returns:
        list: A list of public service addresses.
    """
    return [
        "0x1f98431c8ad98523631ae4a59f267346ea31f984",  # Uniswap V3 Router
        "0x7a250d5630b4cf539739df2c5acb4c659f2488d9",  # Uniswap V2 Router
        "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",  # WETH
        "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",  # USDC
        "0xdac17f958d2ee523a2206206994597c13d831ec7",  # USDT
        "0x6b175474e89094c44da98b954eedeac495271d0f",  # DAI
        "0x2260fac5e5542a773aa44fbc68c9970c04472e8f",  # WBTC
    ]


if __name__ == "__main__":
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Extract Ethereum blockchain data')
    parser.add_argument('--start-block', type=int, default=10000835,
                        help='Starting block number for data extraction')
    parser.add_argument('--end-block', type=int, default=15400000,
                        help='Ending block number for data extraction')
    parser.add_argument('--node-url', type=str, 
                        default="https://mainnet.infura.io/v3/feab6362e4134b2fa4b61022d82d2b97",
                        help='Ethereum node URL')
    parser.add_argument('--max-retries', type=int, default=5,
                        help='Maximum number of retries for API requests')
    parser.add_argument('--retry-delay', type=float, default=1.0,
                        help='Base delay between retries in seconds')
    args = parser.parse_args()

    # 1. Connect to Ethereum Node
    ethereum_node_url = args.node_url
    web3 = Web3(Web3.HTTPProvider(ethereum_node_url))
    
    # Optional: If you are connecting to a PoA network, inject the PoA middleware
    # if ethereum_node_url.startswith("http://localhost") or "YOUR_POA_NETWORK":  # Adjust the condition as needed
    #     web3.middleware_onion.inject(geth_poa_middleware, layer=0)

    # Check if connected
    if not web3.is_connected():
        print("Failed to connect to Ethereum node!")
        exit()
    else:
        print("Successfully connected to Ethereum node.")

    # 2. Define Uniswap V2 Factory Address
    uniswap_factory_address_v2 = "0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f"  # Uniswap V2 Factory

    # 3. Specify Block Range
    start_block = args.start_block
    end_block = args.end_block

    # 4. Extract Data
    start_time = time.time()
    swap_records, liquidity_events = extract_uniswap_v2_data(
        web3, 
        uniswap_factory_address_v2, 
        start_block, 
        end_block,
        max_retries=args.max_retries,
        retry_delay=args.retry_delay
    )
    eth_transfers = extract_eth_transfers(
        web3, 
        start_block, 
        end_block,
        max_retries=args.max_retries,
        retry_delay=args.retry_delay
    )
    end_time = time.time()

    print(f"Data extraction completed in {end_time - start_time:.2f} seconds.")

    # 5. (Optional) Process and Use the Data
    #  The extracted data can now be used for further analysis, such as:
    #  - Creating the ETH transfer graph
    #  - Filtering public service addresses
    #  - Detecting wash trading

    print(f"Number of swap records: {len(swap_records)}")
    print(f"Number of liquidity events: {len(liquidity_events)}")
    print(f"Number of ETH transfers: {len(eth_transfers)}")

    # Example of creating the ETH transfer graph:
    eth_transfer_graph = create_eth_transfer_graph(eth_transfers)
    print(f"Number of nodes in ETH transfer graph: {len(eth_transfer_graph.nodes)}")
    print(f"Number of edges in ETH transfer graph: {len(eth_transfer_graph.edges)}")

    # Get public service addresses and filter them from the eth transfer graph.
    public_service_addresses = get_public_service_addresses(web3)
    filter_public_service_addresses(eth_transfer_graph, public_service_addresses)
    print(f"Number of nodes in ETH transfer graph after filtering public service addresses: {len(eth_transfer_graph.nodes)}")
    print(f"Number of edges in ETH transfer graph after filtering: {len(eth_transfer_graph.edges)}")

    #  The next step would be to use the  'detect_wash_trading'  function
    #  (from the previous code) with the extracted data.  You'll need to 
    #  adapt it to work with the data structures returned by this script.
    #  For example:
    #  wash_trades = detect_wash_trading(swap_records, eth_transfer_graph, ...)
    #  print(f"Detected wash trades: {wash_trades}")
