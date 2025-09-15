import pickle
import pandas as pd
import numpy as np

def explore_pickle_file(filepath):
    """Explore the structure of a pickle file"""
    print(f"\n=== Exploring {filepath} ===")
    
    try:
        with open(filepath, 'rb') as f:
            data = pickle.load(f)
        
        print(f"Type: {type(data)}")
        
        if hasattr(data, '__len__'):
            print(f"Length/Size: {len(data)}")
        
        if isinstance(data, dict):
            print(f"Keys: {list(data.keys())[:10]}")  # Show first 10 keys
            for key in list(data.keys())[:3]:  # Show first 3 items
                item = data[key]
                if hasattr(item, 'shape'):
                    print(f"  Key '{key}': type={type(item)}, shape={item.shape}")
                elif hasattr(item, '__len__'):
                    print(f"  Key '{key}': type={type(item)}, len={len(item)}")
                else:
                    print(f"  Key '{key}': type={type(item)}, value={item}")
                    
        elif isinstance(data, (list, tuple)):
            print(f"First few item types: {[type(item) for item in data[:5]]}")
            if len(data) > 0:
                first_item = data[0]
                print(f"First item: {first_item}")
                if hasattr(first_item, '__dict__'):
                    print(f"First item attributes: {list(first_item.__dict__.keys())}")
                if hasattr(first_item, 'shape'):
                    print(f"First item shape: {first_item.shape}")
        
        elif isinstance(data, (np.ndarray, pd.DataFrame)):
            print(f"Shape: {data.shape}")
            if isinstance(data, pd.DataFrame):
                print(f"Columns: {list(data.columns)[:10]}")
        
        else:
            print(f"Data preview: {str(data)[:200]}")
            if hasattr(data, '__dict__'):
                print(f"Object attributes: {list(data.__dict__.keys())}")
            
    except Exception as e:
        print(f"Error loading {filepath}: {e}")

def load_and_iterate_data():
    """Show how to load and iterate through the data files"""
    print("\n=== How to work with the data ===")
    
    # Example 1: Initial policy data (JoinOrderBenchmark object)
    print("\n1. Loading initial_policy_data.pkl:")
    with open('/ssd_root/yrayhan/balsa/data/initial_policy_data.pkl', 'rb') as f:
        initial_data = pickle.load(f)
    print(f"   Loaded: {type(initial_data)}")
    if hasattr(initial_data, '__dict__'):
        print(f"   Attributes: {list(initial_data.__dict__.keys())}")
    
    # Example 2: Simulation data (list of SubplanGoalCost objects)
    print("\n2. Loading sim-data-6b437297.pkl:")
    with open('/ssd_root/yrayhan/balsa/data/sim-data-6b437297.pkl', 'rb') as f:
        sim_data = pickle.load(f)
    print(f"   Loaded: {len(sim_data)} items of type {type(sim_data[0])}")
    
    # Show how to iterate through first few items
    print("   First 3 items:")
    for i, item in enumerate(sim_data[:3]):
        print(f"     Item {i}: {item}")
        if hasattr(item, '__dict__'):
            print(f"       Attributes: {list(item.__dict__.keys())}")
    
    # Example 3: Featurized data 
    print("\n3. Loading sim-featurized-77797395.pkl:")
    with open('/ssd_root/yrayhan/balsa/data/sim-featurized-77797395.pkl', 'rb') as f:
        featurized_data = pickle.load(f)
    print(f"   Loaded: {featurized_data} (type: {type(featurized_data)})")
    
    return initial_data, sim_data, featurized_data

def main():
    # List of pickle files to explore
    pickle_files = [
        '/ssd_root/yrayhan/balsa/data/initial_policy_data.pkl',
        '/ssd_root/yrayhan/balsa/data/sim-data-6b437297.pkl',
        '/ssd_root/yrayhan/balsa/data/sim-featurized-77797395.pkl'
    ]
    
    for filepath in pickle_files:
        explore_pickle_file(filepath)
    
    # Show how to work with the data
    load_and_iterate_data()

if __name__ == "__main__":
    main()