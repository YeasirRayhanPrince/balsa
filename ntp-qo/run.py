#!/usr/bin/env python3
"""
Test script for the refactored workloads.py with dataclass-based configuration.
"""

import sys
import os
import logging
import argparse

# Supported database engines
SUPPORTED_ENGINES = ['postgres', 'duckdb', 'mysql']

# Add paths for imports
sys.path.append('.')
sys.path.append('..')
sys.path.append('../..')
sys.path.append('ntp_qo')

def validate_engine(engine):
    """Validate that the engine is supported."""
    if engine not in SUPPORTED_ENGINES:
        raise ValueError(f"Unsupported engine: {engine}. Supported engines: {', '.join(SUPPORTED_ENGINES)}")
    return True

def test_workload_params():
    """Test the new WorkloadParams dataclass."""
    print("="*60)
    print("Testing WorkloadParams dataclass")
    print("="*60)
    
    try:
        from sql_parse import WorkloadParams
        
        # Test default initialization
        print("1. Testing default WorkloadParams...")
        params = WorkloadParams()
        print(f"   query_dir: {params.query_dir}")
        print(f"   query_glob: {params.query_glob}")
        print(f"   loop_through_queries: {params.loop_through_queries}")
        print(f"   test_query_glob: {params.test_query_glob}")
        print(f"   search_space_join_ops: {params.search_space_join_ops}")
        print(f"   search_space_scan_ops: {params.search_space_scan_ops}")
        
        # Test custom initialization
        print("\n2. Testing custom WorkloadParams...")
        custom_params = WorkloadParams(
            query_dir="/custom/path",
            query_glob="test_*.sql",
            loop_through_queries=True
        )
        print(f"   query_dir: {custom_params.query_dir}")
        print(f"   query_glob: {custom_params.query_glob}")
        print(f"   loop_through_queries: {custom_params.loop_through_queries}")
        print(f"   search_space_join_ops: {custom_params.search_space_join_ops}")
        
        print("\nWorkloadParams tests passed!")
        return True
        
    except Exception as e:
        print(f"\nWorkloadParams test failed: {e}")
        return False

def test_workload_creation():
    """Test workload creation with new structure."""
    print("\n" + "="*60)
    print("Testing Workload Creation")
    print("="*60)
    
    try:
        from sql_parse import WorkloadParams, Workload
        
        print("1. Testing basic Workload creation...")
        params = WorkloadParams(query_dir="/tmp/test")
        workload = Workload(params)
        
        print(f"   Workload params type: {type(workload.params)}")
        print(f"   Query dir: {workload.params.query_dir}")
        print(f"   Query glob: {workload.params.query_glob}")
        
        print("\nBasic Workload creation passed!")
        return True
        
    except Exception as e:
        print(f"\nWorkload creation test failed: {e}")
        return False

def test_join_order_benchmark():
    """Test JoinOrderBenchmark initialization."""
    print("\n" + "="*60)
    print("Testing JoinOrderBenchmark")
    print("="*60)
    
    try:
        from sql_parse import JoinOrderBenchmark, WorkloadParams
        
        print("1. Testing JOB with required params...")
        # Now requires explicit query_dir parameter
        try:
            params = WorkloadParams(query_dir="/ssd_root/yrayhan/balsa/queries/join-order-benchmark")
            job = JoinOrderBenchmark(params)
            print("JOB created successfully with explicit query_dir!")
            print(job.workload_info)
            print(job.query_nodes[:2])
        except Exception as e:
            raise e
        
        # print("\n2. Testing JOB with custom params...")
        # from sql_parse import WorkloadParams
        # custom_params = WorkloadParams(
        #     query_dir="/custom/job/path",
        #     query_glob="*.sql"
        # )
        
        # try:
        #     job = JoinOrderBenchmark(custom_params)
        #     print("   JOB with custom params created!")
        # except NameError as ne:
        #     if "balsa" in str(ne):
        #         print("   Expected error: balsa module not found (this is normal)")
        #         print("   JOB custom initialization structure is correct")
        #     else:
        #         raise ne
        
        # print("\nJoinOrderBenchmark tests passed!")
        return True
        
    except Exception as e:
        print(f"\nJoinOrderBenchmark test failed: {e}")
        return False

def test_join_order_benchmark_w_diff_engine(engine='postgres', true_card=None):
    """Test a different engine for JoinOrderBenchmark initialization."""
    print("\n" + "="*60)
    print("Testing Different Engine: JoinOrderBenchmark")
    print("="*60)

    try:
        from sql_parse import JoinOrderBenchmark, WorkloadParams
        
        print(f"1. Configure WorkloadParams with {engine} engine...")
        
        # Validate engine
        validate_engine(engine)
        
        try:
            print(f"\n   Creating WorkloadParams with {engine} engine...")
            params = WorkloadParams(
                query_dir="/ssd_root/yrayhan/balsa/queries/join-order-benchmark",
                engine=engine,
                true_card=true_card
            )
            print(f"   WorkloadParams created:")
            print(f"     engine: {params.engine}")
            print(f"     query_dir: {params.query_dir}")
            
            print(f"\n   Initializing JoinOrderBenchmark with {engine} engine...")
            
            job = JoinOrderBenchmark(params)
            
            print(f"✅ JOB created successfully with {engine} engine!")
            print(f"\nWorkload info:")
            print(job.workload_info)
            print(f"\nFirst 2 query nodes:")
            for _ in job.query_nodes[:2]:
                # print(_)
                print(_.print_tree(true_card=True))
        except Exception as e:
            import traceback
            print(f"\n❌ Detailed error in JOB initialization:")
            print(f"Error type: {type(e).__name__}")
            print(f"Error message: {str(e)}")
            print("\nFull traceback:")
            traceback.print_exc()
            raise e
        return True
        
    except Exception as e:
        import traceback
        print(f"\nJoinOrderBenchmark test failed: {e}")
        print("Full traceback:")
        traceback.print_exc()
        return False

def main():
    """Run all tests with command-line argument support."""
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Test JoinOrderBenchmark with different engines')
    parser.add_argument('--engine', 
                       choices=SUPPORTED_ENGINES, 
                       default='duckdb',
                       help=f'Database engine to use (choices: {", ".join(SUPPORTED_ENGINES)}, default: duckdb)')
    parser.add_argument('--true_card', 
                       action='store_true',
                       help='Enable true cardinality (default: False)')
    
    args = parser.parse_args()
    
    # Configure logging to show INFO level messages
    logging.basicConfig(level=logging.INFO, format='%(filename)s:%(lineno)d: %(levelname)s: %(message)s')
    
    print(f"Running tests with engine: {args.engine}, {'true_card' if args.true_card else 'no true_card'}")
    
    success = True
    
    # # Test 1: WorkloadParams dataclass
    # success &= test_workload_params()
   
    
    # # Test 2: Basic Workload creation
    # success &= test_workload_creation()
    
    # Test 3: JoinOrderBenchmark
    # success &= test_join_order_benchmark()

    # Test 4: JoinOrderBenchmark with different engine
    success &= test_join_order_benchmark_w_diff_engine(
        engine=args.engine, 
        true_card=args.true_card
        )
    
    # Summary
    print("\n" + "="*80)
    # if success:
    #     print("ALL TESTS PASSED!")
    #     print("Dataclass configuration is working correctly")
    #     print("No more complex hyperparams.InstantiableParams needed")
    #     print("Clean, type-safe parameter management")
    # else:
    #     print("SOME TESTS FAILED")
    #     print("Check the error messages above")
    
    print("="*80)
    
    return 0 if success else 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)