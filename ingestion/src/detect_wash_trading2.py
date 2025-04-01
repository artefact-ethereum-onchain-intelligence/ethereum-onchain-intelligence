import networkx as nx
import time

def coarse_filtering(swap_sequence, max_time_interval, max_position_change):
    """
    Filters swap sequences to identify potential wash trading groups based on time interval
    and position change.

    Args:
        swap_sequence (list): A list of swaps, where each swap is a tuple:
            (timestamp, asset_in, asset_out, sender, volume)
        max_time_interval (int): Maximum time interval (in blocks) for swaps in a group.
        max_position_change (float): Maximum allowed change in asset position (as a fraction
            of total volume).

    Returns:
        list: A list of suspicious swap groups. Each group is a list of swaps.
    """
    suspicious_groups = []
    n = len(swap_sequence)

    for i in range(n):
        for j in range(i + 1, min(n, i + 10)):  # Check up to 10 swaps ahead
            swap_group = swap_sequence[i:j+1]
            
            # 1. Tiny Time Interval
            time_difference = abs(swap_group[-1][0] - swap_group[0][0])
            if time_difference > max_time_interval:
                continue  # Skip this group if time interval is too large

            # 2. Unchanged AMM Status
            net_position_change_x = 0
            net_position_change_y = 0
            total_volume = 0
            
            for swap in swap_group:
                #  asset_in, asset_out,  sender, volume = swap
                if swap[1] > 0: # Buy X, Sell Y
                    net_position_change_x += swap[4]  # Increase in X
                    net_position_change_y -= swap[4]  # Decrease in Y
                else:
                    net_position_change_x -= swap[4]
                    net_position_change_y += swap[4]
                total_volume += swap[4]

            # Check if the change in position is small relative to the total volume.
            if total_volume == 0:
                continue
            
            position_change_x_ratio = abs(net_position_change_x) / total_volume
            position_change_y_ratio = abs(net_position_change_y) / total_volume
            
            if position_change_x_ratio <= max_position_change or position_change_y_ratio <= max_position_change:
                suspicious_groups.append(swap_group)

    return suspicious_groups


def entity_recognition(suspicious_groups, eth_transfer_graph, max_distance=2):
    """
    Filters suspicious swap groups by checking if the sender addresses belong to the same entity
    based on ETH transfer links.

    Args:
        suspicious_groups (list): A list of suspicious swap groups.
        eth_transfer_graph (networkx.Graph): A graph representing ETH transfers between addresses.
        max_distance (int): Maximum distance (in hops) between addresses to be considered
            the same entity.

    Returns:
        list: A list of swap groups where the senders are likely the same entity.
    """
    filtered_groups = []
    for group in suspicious_groups:
        senders = list(set(swap[3] for swap in group))  # Extract unique senders
        
        if len(senders) == 1:
            filtered_groups.append(group)  # Single sender, same entity
            continue
        
        # Create a subgraph containing only the senders in the current group.
        sender_graph = nx.Graph()
        sender_graph.add_nodes_from(senders)

        # Add edges if the distance between senders in the ETH transfer graph is within the threshold.
        for i in range(len(senders)):
            for j in range(i + 1, len(senders)):
                sender_a = senders[i]
                sender_b = senders[j]
                
                if sender_a in eth_transfer_graph and sender_b in eth_transfer_graph:
                    try:
                        path_length = nx.shortest_path_length(eth_transfer_graph, source=sender_a, target=sender_b)
                        if path_length <= max_distance:
                            sender_graph.add_edge(sender_a, sender_b)
                    except nx.NetworkXNoPath:
                        pass  # No path between the senders

        # If the sender graph is connected, consider them the same entity.
        if nx.is_connected(sender_graph):
            filtered_groups.append(group)
            
    return filtered_groups



def eliminate_mev_transactions(potential_wash_trades):
    """
    Filters out MEV transactions from potential wash trades, focusing on sandwich attacks,
    arbitrage, and rebase token arbitrage.

    Args:
        potential_wash_trades (list): List of potential wash trade groups.

    Returns:
        list: List of filtered swap groups, likely to be wash trades.
    """
    
    filtered_trades = []
    
    for trade_group in potential_wash_trades:
        is_mev = False
        
        # 1. Eliminate Sandwich Attack Transactions
        if len(trade_group) >= 3:
            first_tx = trade_group[0]
            middle_tx = trade_group[1]
            last_tx = trade_group[2]
            
            if (
                first_tx[0] == middle_tx[0] == last_tx[0] # Same Block
                and (first_tx[1] > 0 and middle_tx[1] > 0 and last_tx[1] < 0) # first and middle buy, last sell
                and 0.9 <= abs(last_tx[4]) / abs(first_tx[4]) <= 1.1 # Similar amounts
            ):
                is_mev = True
        
        # 2. Eliminate Ordinary Arbitrage Transactions
        
        pool_set = set()
        for tx in trade_group:
            pool_set.add(tx[5])  # tx[5] is the pool
        if len(pool_set) > 1:
            is_mev = True
        
        # 3. Eliminate Rebase Token Arbitrage Transactions
        if len(trade_group) == 2:
            tx1, tx2 = trade_group
            if (tx1[1] > 0 and tx2[1] < 0 and tx1[4] == abs(tx2[4]) and abs(tx2[2]) > abs(tx1[2])):
                is_mev = True
        
        if not is_mev:
            filtered_trades.append(trade_group)
    return filtered_trades



def detect_wash_trading(swap_records, eth_transfer_graph, max_time_interval, max_position_change, max_distance):
    """
    Detects wash trading in AMM exchanges using coarse filtering, entity recognition, and MEV transaction removal.

    Args:
        swap_records (list): A list of swap transactions.  Each swap record is a tuple:
            (block_number, asset_in, asset_out, sender_address, volume, pool_address)
        eth_transfer_graph (networkx.Graph): A graph representing ETH transfers between addresses.
        max_time_interval (int): Maximum time interval (in blocks) for swaps in a group.
        max_position_change (float): Maximum allowed change in asset position.
        max_distance (int): Maximum distance for entity recognition.

    Returns:
        list: A list of lists, where each inner list represents a group of transactions
              identified as wash trades.
    """
    # 1. Coarse Filtering
    potential_wash_trades = coarse_filtering(swap_records, max_time_interval, max_position_change)

    print(f"Potential wash trades: {len(potential_wash_trades)}")
    
    # 2. Entity Recognition
    potential_wash_trades = entity_recognition(potential_wash_trades, eth_transfer_graph, max_distance)
    print(f"Potential wash trades after entity recognition: {len(potential_wash_trades)}")
    wash_trades = potential_wash_trades
    # # 3. Eliminate MEV Transactions
    # wash_trades = eliminate_mev_transactions(potential_wash_trades)
    # print(f"Potential wash trades after MEV elimination: {len(wash_trades)}")
    return wash_trades



if __name__ == "__main__":
    # 1. Example Data (replace with your actual data)
    # Each swap is represented as a tuple: (block_number, asset_in, asset_out, sender_address, volume, pool_address)
    swap_records = [
        (100, 10, -5, 'A1', 10, 'Pool1'),  # Buy 10 X, Sell 5 Y  at block 100
        (101, -5, 10, 'A2', 5, 'Pool1'),  # Sell 5 X, Buy 10 Y at block 101
        (102, 20, -10, 'A1', 20, 'Pool1'),
        (103, -10, 20, 'A2', 10, 'Pool1'),
        (105, 5, -5, 'A3', 5, 'Pool2'), # Irrelevant transaction in another pool
        (110, 100, -50, 'B1', 100, 'Pool3'),
        (111, -50, 100, 'B2', 50, 'Pool3'),
        (112, 20, -10, 'B1', 20, 'Pool3'),
        (113, -10, 20, 'B2', 10, 'Pool3')
    ]
    
    # 2. Example ETH Transfer Graph (replace with your actual graph)
    # Create a simple graph for demonstration.  In a real scenario, this would be 
    # constructed from actual blockchain data.
    eth_transfer_graph = nx.Graph()
    eth_transfer_graph.add_edge('A1', 'A2')
    eth_transfer_graph.add_edge('A2', 'A3')  # A1 and A3 are connected through A2
    eth_transfer_graph.add_edge('B1','B2')
    

    # 3. Set Parameters (adjust based on your needs and the characteristics of the data)
    max_time_interval = 2  # Example: 2 blocks
    max_position_change = 0.1  # Example: 10%
    max_distance = 2  # Example:  addresses within 2 hops are considered the same entity

    # 4. Detect Wash Trading
    start_time = time.time()
    wash_trades = detect_wash_trading(swap_records, eth_transfer_graph, max_time_interval, max_position_change, max_distance)
    end_time = time.time()
    
    print(f"Wash trading detection took {end_time - start_time:.2f} seconds.")

    # 5. Output Results
    if wash_trades:
        print("Detected Wash Trading Groups:")
        for i, group in enumerate(wash_trades):
            print(f"Group {i + 1}:")
            for swap in group:
                print(f"  Block: {swap[0]}, Sender: {swap[3]}, Asset In/Out: {swap[1]:+}, Volume: {swap[4]}, Pool: {swap[5]}")
    else:
        print("No wash trading detected.")
