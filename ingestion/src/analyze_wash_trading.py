import argparse
import time
from web3 import Web3
from main import (
    extract_uniswap_v2_data,
    extract_eth_transfers,
    create_eth_transfer_graph,
    get_public_service_addresses,
    filter_public_service_addresses,
)
from detect_wash_trading2 import detect_wash_trading

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Analyze wash trading in Uniswap V2')
    parser.add_argument('--start-block', type=int, default=10000835,
                        help='Starting block number for data extraction')
    parser.add_argument('--end-block', type=int, default=15400000,
                        help='Ending block number for data extraction')
    parser.add_argument('--node-url', type=str, 
                        # default="https://mainnet.infura.io/v3/feab6362e4134b2fa4b61022d82d2b97",
                        # default="https://eth-mainnet.g.alchemy.com/v2/uw4njq5KtUotzky45gwUCx_5gTnCvBsd",
                        default="https://eth-mainnet.g.alchemy.com/v2/uw4njq5KtUotzky45gwUCx_5gTnCvBsd",
                        help='Ethereum node URL')
    parser.add_argument('--max-time-interval', type=int, default=2,
                        help='Maximum time interval (in blocks) for wash trading detection')
    parser.add_argument('--max-position-change', type=float, default=2,
                        help='Maximum position change ratio for wash trading detection')
    parser.add_argument('--max-distance', type=int, default=20,
                        help='Maximum distance in ETH transfer graph for entity recognition')
    args = parser.parse_args()

    # Connect to Ethereum node
    web3 = Web3(Web3.HTTPProvider(args.node_url))
    if not web3.is_connected():
        print("Failed to connect to Ethereum node!")
        exit()
    print("Successfully connected to Ethereum node.")

    # Define Uniswap V2 Factory Address
    uniswap_factory_address_v2 = "0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f"

    # Extract data
    print("Extracting Uniswap V2 data...")
    start_time = time.time()
    swap_records, _ = extract_uniswap_v2_data(
        web3, 
        uniswap_factory_address_v2, 
        args.start_block, 
        args.end_block
    )
    print(f"Extracted {len(swap_records)} swap records")

    print("Extracting ETH transfers...")
    eth_transfers = extract_eth_transfers(
        web3, 
        args.start_block, 
        args.end_block
    )
    print(f"Extracted {len(eth_transfers)} ETH transfers")

    # Create and filter ETH transfer graph
    print("Creating ETH transfer graph...")
    eth_transfer_graph = create_eth_transfer_graph(eth_transfers)
    print(f"Initial graph: {len(eth_transfer_graph.nodes)} nodes, {len(eth_transfer_graph.edges)} edges")

    public_service_addresses = get_public_service_addresses(web3)
    filter_public_service_addresses(eth_transfer_graph, public_service_addresses)
    print(f"After filtering: {len(eth_transfer_graph.nodes)} nodes, {len(eth_transfer_graph.edges)} edges")

    # Detect wash trading
    print("Detecting wash trading...")
    wash_trades = detect_wash_trading(
        swap_records,
        eth_transfer_graph,
        args.max_time_interval,
        args.max_position_change,
        args.max_distance
    )

    # Output results
    print("\nWash Trading Analysis Results:")
    print(f"Analysis took {time.time() - start_time:.2f} seconds")
    print(f"Found {len(wash_trades)} potential wash trading groups")

    if wash_trades:
        print("\nDetected Wash Trading Groups:")
        for i, group in enumerate(wash_trades):
            print(f"\nGroup {i + 1}:")
            for swap in group:
                print(f"  Block: {swap[0]}, Sender: {swap[3]}, Asset In/Out: {swap[1]:+}, Volume: {swap[4]}, Pool: {swap[5]}")

if __name__ == "__main__":
    main() 