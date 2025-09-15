import pickle
import pprint

def dump_initial_policy_data():
    """Dump initial_policy_data.pkl to txt file"""
    with open('/ssd_root/yrayhan/balsa/data/initial_policy_data.pkl', 'rb') as f:
        data = pickle.load(f)
    
    with open('initial_policy_data.txt', 'w') as f:
        f.write("=== INITIAL POLICY DATA ===\n")
        f.write(f"Type: {type(data)}\n")
        f.write(f"Object: {data}\n\n")
        
        if hasattr(data, '__dict__'):
            f.write("Attributes:\n")
            for attr_name in dir(data):
                if not attr_name.startswith('_'):
                    try:
                        attr_value = getattr(data, attr_name)
                        f.write(f"\n{attr_name}:\n")
                        f.write(f"  Type: {type(attr_value)}\n")
                        if hasattr(attr_value, '__len__') and not isinstance(attr_value, str):
                            f.write(f"  Length: {len(attr_value)}\n")
                        f.write(f"  Value: {repr(attr_value)}\n")
                    except Exception as e:
                        f.write(f"  Error accessing {attr_name}: {e}\n")

def dump_sim_data():
    """Dump sim-data-6b437297.pkl to txt file"""
    with open('/ssd_root/yrayhan/balsa/data/sim-data-6b437297.pkl', 'rb') as f:
        data = pickle.load(f)
    
    with open('sim_data.txt', 'w') as f:
        f.write("=== SIMULATION DATA ===\n")
        f.write(f"Type: {type(data)}\n")
        f.write(f"Total items: {len(data)}\n\n")
        
        # Write first 100 items in detail
        f.write("First 100 items:\n")
        for i, item in enumerate(data[:100]):
            f.write(f"\nItem {i}:\n")
            f.write(f"  Type: {type(item)}\n")
            f.write(f"  String representation: {item}\n")
            if hasattr(item, 'subplan'):
                f.write(f"  subplan: {item.subplan}\n")
            if hasattr(item, 'goal'):
                f.write(f"  goal: {item.goal}\n")
            if hasattr(item, 'cost'):
                f.write(f"  cost: {item.cost}\n")
        
        # Write summary of all items
        f.write(f"\n\nSummary of all {len(data)} items:\n")
        unique_subplans = set()
        unique_goals = set()
        costs = []
        
        for item in data:
            if hasattr(item, 'subplan'):
                unique_subplans.add(item.subplan)
            if hasattr(item, 'goal'):
                unique_goals.add(item.goal)
            if hasattr(item, 'cost'):
                costs.append(item.cost)
        
        f.write(f"Unique subplans: {len(unique_subplans)}\n")
        f.write(f"Unique goals: {len(unique_goals)}\n")
        if costs:
            f.write(f"Cost range: {min(costs)} - {max(costs)}\n")
            f.write(f"Average cost: {sum(costs) / len(costs):.2f}\n")

def dump_featurized_data():
    """Dump sim-featurized-77797395.pkl to txt file"""
    with open('/ssd_root/yrayhan/balsa/data/sim-featurized-77797395.pkl', 'rb') as f:
        data = pickle.load(f)
    
    with open('featurized_data.txt', 'w') as f:
        f.write("=== FEATURIZED DATA ===\n")
        f.write(f"Type: {type(data)}\n")
        f.write(f"Value: {data}\n")
        f.write(f"String representation: {repr(data)}\n")
        f.write(f"Hex representation: {hex(data) if isinstance(data, int) else 'N/A'}\n")

if __name__ == "__main__":
    print("Dumping initial_policy_data.pkl...")
    dump_initial_policy_data()
    
    print("Dumping sim-data-6b437297.pkl...")
    dump_sim_data()
    
    print("Dumping sim-featurized-77797395.pkl...")
    dump_featurized_data()
    
    print("Done! Created initial_policy_data.txt, sim_data.txt, and featurized_data.txt")