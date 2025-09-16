#!/usr/bin/env python3
"""
Clean workload management for NTP-QO.

Provides a simple, clean interface for loading and managing query workloads
without the complexity of Balsa's original workload infrastructure.
"""

import os
import sys
import glob
from typing import List, Dict, Tuple, Optional
from dataclasses import dataclass

# Add paths for Balsa imports
sys.path.append('..')
sys.path.append('../..')
from balsa.util import postgres, plans_lib


@dataclass
class WorkloadConfig:
    """Configuration for workload creation."""
    name: str
    query_dir: str
    train_ratio: float = 0.8
    test_ratio: float = 0.2
    seed: int = 42


class NTPWorkload:
    """Clean workload manager for NTP-QO."""
    
    def __init__(self, config: WorkloadConfig):
        self.config = config
        self.name = config.name
        
        # Query collections
        self._all_queries: List[plans_lib.Node] = []
        self._train_queries: List[plans_lib.Node] = []
        self._test_queries: List[plans_lib.Node] = []
        
        # Metadata
        self.loaded = False
        
    def load(self) -> None:
        """Load queries from the configured directory."""
        print(f"Loading workload: {self.name}")
        print(f"Query directory: {self.config.query_dir}")
        
        if not os.path.exists(self.config.query_dir):
            raise FileNotFoundError(f"Query directory not found: {self.config.query_dir}")
        
        # Find all SQL files
        sql_files = glob.glob(os.path.join(self.config.query_dir, "*.sql"))
        if not sql_files:
            raise ValueError(f"No SQL files found in {self.config.query_dir}")
        
        print(f"Found {len(sql_files)} SQL files")
        
        # Load queries
        successful_loads = 0
        for sql_file in sorted(sql_files):
            try:
                query_node = self._load_query_file(sql_file)
                self._all_queries.append(query_node)
                successful_loads += 1
            except Exception as e:
                print(f"Warning: Failed to load {os.path.basename(sql_file)}: {e}")
        
        print(f"Successfully loaded {successful_loads} queries")
        
        # Split into train/test
        self._split_queries()
        self.loaded = True
        
        self._print_summary()
    
    def _load_query_file(self, sql_file: str) -> plans_lib.Node:
        """Load a single SQL file and return Balsa Node."""
        with open(sql_file, 'r') as f:
            sql_text = f.read().strip()
        
        # Use Balsa's PostgreSQL infrastructure to get the plan
        balsa_node, _ = postgres.SqlToPlanNode(sql_text)
        
        # Add metadata to the node's info dict
        if not hasattr(balsa_node, 'info'):
            balsa_node.info = {}
        
        balsa_node.info['query_name'] = os.path.basename(sql_file)
        balsa_node.info['source_file'] = sql_file
        balsa_node.info['sql_str'] = sql_text
        
        return balsa_node
    
    
    def _split_queries(self) -> None:
        """Split queries into train/test sets."""
        import random
        
        # Set seed for reproducible splits
        random.seed(self.config.seed)
        
        # Shuffle and split
        queries = self._all_queries.copy()
        random.shuffle(queries)
        
        train_size = int(len(queries) * self.config.train_ratio)
        
        self._train_queries = queries[:train_size]
        self._test_queries = queries[train_size:]
    
    def _print_summary(self) -> None:
        """Print workload summary."""
        print("\n" + "="*50)
        print(f"Workload: {self.name}")
        print("="*50)
        print(f"Total queries: {len(self._all_queries)}")
        print(f"Train queries: {len(self._train_queries)}")
        print(f"Test queries: {len(self._test_queries)}")
        
        if self._train_queries:
            print(f"Train files: {[q.info['query_name'] for q in self._train_queries[:5]]}")
            if len(self._train_queries) > 5:
                print(f"             ... and {len(self._train_queries) - 5} more")
        
        if self._test_queries:
            print(f"Test files: {[q.info['query_name'] for q in self._test_queries[:5]]}")
            if len(self._test_queries) > 5:
                print(f"            ... and {len(self._test_queries) - 5} more")
        
        print("="*50)
    
    # Query access methods
    def all_queries(self) -> List[plans_lib.Node]:
        """Get all queries."""
        if not self.loaded:
            raise RuntimeError("Workload not loaded. Call load() first.")
        return self._all_queries
    
    def train_queries(self) -> List[plans_lib.Node]:
        """Get training queries."""
        if not self.loaded:
            raise RuntimeError("Workload not loaded. Call load() first.")
        return self._train_queries
    
    def test_queries(self) -> List[plans_lib.Node]:
        """Get test queries."""
        if not self.loaded:
            raise RuntimeError("Workload not loaded. Call load() first.")
        return self._test_queries
    
    def get_query(self, name: str) -> Optional[plans_lib.Node]:
        """Get a specific query by name."""
        for query in self._all_queries:
            if query.info.get('query_name') == name:
                return query
        return None


def create_job_workload(
    query_dir: str = "../queries/join-order-benchmark",
    train_ratio: float = 0.8
) -> NTPWorkload:
    """Create a JOB (Join Order Benchmark) workload."""
    
    # Try different possible paths
    possible_paths = [
        query_dir,
        "../../queries/join-order-benchmark",
        "../../../queries/join-order-benchmark"
    ]
    
    actual_path = None
    for path in possible_paths:
        if os.path.exists(path):
            actual_path = path
            break
    
    if actual_path is None:
        raise FileNotFoundError(f"JOB queries not found in any of: {possible_paths}")
    
    config = WorkloadConfig(
        name="JOB",
        query_dir=actual_path,
        train_ratio=train_ratio,
        test_ratio=1.0 - train_ratio
    )
    
    return NTPWorkload(config)


def create_custom_workload(
    name: str,
    query_dir: str,
    train_ratio: float = 0.8
) -> NTPWorkload:
    """Create a custom workload from a directory of SQL files."""
    
    config = WorkloadConfig(
        name=name,
        query_dir=query_dir,
        train_ratio=train_ratio,
        test_ratio=1.0 - train_ratio
    )
    
    return NTPWorkload(config)