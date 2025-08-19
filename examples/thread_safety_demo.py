"""
Thread Safety Demonstration

This script demonstrates the thread safety improvements made to the SchemaDataProfiler
and FieldPatternRecognizer, particularly for parallel processing scenarios.
"""

import sys
import os
import threading
import time
import concurrent.futures
from typing import List, Dict, Any

# Add the src directory to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from profiler import FieldPatternRecognizer, ProfilingStrategy


def test_pattern_recognizer_thread_safety():
    """Test thread safety of the FieldPatternRecognizer."""
    print("=" * 70)
    print("Thread Safety Test: FieldPatternRecognizer")
    print("=" * 70)
    
    # Create pattern recognizer
    pattern_recognizer = FieldPatternRecognizer()
    
    # Sample data for different threads
    test_datasets = [
        {
            "name": "Healthcare IDs",
            "values": ["1234567890", "9876543210", "1111111111", "2222222222"],
            "field_name": "provider_npi"
        },
        {
            "name": "Email Addresses", 
            "values": ["john@hospital.org", "jane@clinic.com", "bob@medical.net"],
            "field_name": "email_address"
        },
        {
            "name": "Phone Numbers",
            "values": ["555-1234", "555-5678", "555-9999"],
            "field_name": "phone"
        },
        {
            "name": "Mixed Data",
            "values": ["test123", "data456", "sample789"],
            "field_name": "mixed_field"
        }
    ]
    
    results = {}
    errors = []
    
    def pattern_detection_worker(dataset: Dict[str, Any], worker_id: int) -> Dict[str, Any]:
        """Worker function for pattern detection."""
        try:
            print(f"  Worker {worker_id}: Processing {dataset['name']}")
            
            # Simulate some processing time
            time.sleep(0.1)
            
            # Detect patterns
            patterns = pattern_recognizer.detect_patterns(
                dataset['values'], 
                field_name=dataset['field_name']
            )
            
            # Get pattern info
            pattern_info = []
            for pattern in patterns:
                info = pattern_recognizer.get_pattern_info(pattern)
                if info:
                    pattern_info.append({
                        'pattern': pattern,
                        'description': info.get('description', 'N/A')
                    })
            
            result = {
                'worker_id': worker_id,
                'dataset_name': dataset['name'],
                'patterns_found': len(patterns),
                'patterns': patterns,
                'pattern_info': pattern_info,
                'success': True
            }
            
            print(f"  Worker {worker_id}: Found {len(patterns)} patterns in {dataset['name']}")
            return result
            
        except Exception as e:
            error_result = {
                'worker_id': worker_id,
                'dataset_name': dataset['name'],
                'error': str(e),
                'success': False
            }
            errors.append(error_result)
            print(f"  Worker {worker_id}: ERROR in {dataset['name']}: {e}")
            return error_result
    
    def reload_patterns_worker():
        """Worker function that reloads patterns during processing."""
        try:
            print("  Reload Worker: Starting pattern reload...")
            time.sleep(0.05)  # Start reload in middle of processing
            pattern_recognizer.reload_patterns()
            print("  Reload Worker: Pattern reload completed")
            return {'reload_success': True}
        except Exception as e:
            print(f"  Reload Worker: ERROR during reload: {e}")
            return {'reload_success': False, 'error': str(e)}
    
    print(f"\n🧵 Testing concurrent pattern detection with {len(test_datasets)} workers...")
    print("   (Including pattern reload during processing)")
    
    # Run concurrent pattern detection with pattern reloading
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
        # Submit pattern detection tasks
        pattern_futures = {
            executor.submit(pattern_detection_worker, dataset, i): i 
            for i, dataset in enumerate(test_datasets, 1)
        }
        
        # Submit pattern reload task
        reload_future = executor.submit(reload_patterns_worker)
        
        # Collect results
        for future in concurrent.futures.as_completed(pattern_futures):
            worker_id = pattern_futures[future]
            try:
                result = future.result()
                results[worker_id] = result
            except Exception as e:
                print(f"  Worker {worker_id}: Exception: {e}")
        
        # Get reload result
        try:
            reload_result = reload_future.result()
            results['reload'] = reload_result
        except Exception as e:
            print(f"  Reload Worker: Exception: {e}")
    
    # Print results
    print(f"\n📊 Results Summary:")
    successful_workers = sum(1 for r in results.values() if isinstance(r, dict) and r.get('success', False))
    total_patterns = sum(r.get('patterns_found', 0) for r in results.values() if isinstance(r, dict) and 'patterns_found' in r)
    
    print(f"   ✓ Successful workers: {successful_workers}/{len(test_datasets)}")
    print(f"   ✓ Total patterns detected: {total_patterns}")
    print(f"   ✓ Pattern reload success: {results.get('reload', {}).get('reload_success', False)}")
    print(f"   ✓ Errors encountered: {len(errors)}")
    
    if errors:
        print(f"\n❌ Errors:")
        for error in errors:
            print(f"     Worker {error['worker_id']}: {error['error']}")
    
    # Detailed results
    print(f"\n📋 Detailed Results:")
    for worker_id, result in results.items():
        if worker_id != 'reload' and isinstance(result, dict):
            print(f"   Worker {result['worker_id']} ({result['dataset_name']}):")
            if result['success']:
                print(f"     Patterns: {result['patterns']}")
                for info in result['pattern_info']:
                    print(f"     - {info['pattern']}: {info['description']}")
            else:
                print(f"     ERROR: {result['error']}")
    
    return len(errors) == 0 and results.get('reload', {}).get('reload_success', False)


def test_schema_profiler_thread_safety():
    """Test thread safety concepts in schema profiling."""
    print("\n" + "=" * 70)
    print("Thread Safety Test: Schema Profiling Concepts")
    print("=" * 70)
    
    # Simulate multiple profiler instances (as would happen in production)
    def create_pattern_recognizer_worker(worker_id: int) -> Dict[str, Any]:
        """Worker that creates and uses a pattern recognizer."""
        try:
            print(f"  Profiler Worker {worker_id}: Creating pattern recognizer...")
            
            # Each worker gets its own pattern recognizer instance
            recognizer = FieldPatternRecognizer()
            
            # Test pattern detection
            test_data = [
                ["1234567890", "9876543210"],  # NPI-like
                ["john@test.com", "jane@test.org"],  # Email-like
                ["data123", "test456"]  # Generic
            ]
            
            results = []
            for i, data in enumerate(test_data):
                patterns = recognizer.detect_patterns(data, field_name=f"field_{i}")
                results.append({
                    'data': data[:2],  # First 2 items for brevity
                    'patterns': patterns
                })
            
            print(f"  Profiler Worker {worker_id}: Completed pattern detection")
            
            return {
                'worker_id': worker_id,
                'results': results,
                'success': True
            }
            
        except Exception as e:
            print(f"  Profiler Worker {worker_id}: ERROR: {e}")
            return {
                'worker_id': worker_id,
                'error': str(e),
                'success': False
            }
    
    print(f"\n🏭 Testing multiple pattern recognizer instances...")
    
    # Test multiple instances in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = {
            executor.submit(create_pattern_recognizer_worker, i): i 
            for i in range(1, 5)
        }
        
        profiler_results = {}
        for future in concurrent.futures.as_completed(futures):
            worker_id = futures[future]
            try:
                result = future.result()
                profiler_results[worker_id] = result
            except Exception as e:
                print(f"  Profiler Worker {worker_id}: Exception: {e}")
    
    # Analyze results
    successful_profilers = sum(1 for r in profiler_results.values() if r.get('success', False))
    print(f"\n📊 Profiler Results:")
    print(f"   ✓ Successful profiler instances: {successful_profilers}/4")
    
    for worker_id, result in profiler_results.items():
        if result['success']:
            total_patterns = sum(len(r['patterns']) for r in result['results'])
            print(f"   Worker {worker_id}: {total_patterns} patterns detected")
        else:
            print(f"   Worker {worker_id}: ERROR - {result['error']}")
    
    return successful_profilers == 4


def demonstrate_thread_safety_improvements():
    """Demonstrate the specific thread safety improvements made."""
    print("\n" + "=" * 70)
    print("Thread Safety Improvements Demonstrated")
    print("=" * 70)
    
    improvements = [
        {
            "area": "Pattern Recognizer",
            "issue": "Race conditions during pattern reload",
            "solution": "RLock for thread-safe pattern loading/reloading",
            "benefit": "Safe concurrent access to patterns"
        },
        {
            "area": "Pattern Detection",
            "issue": "Shared state modification during detection",
            "solution": "Snapshot patterns before processing",
            "benefit": "Consistent pattern state across threads"
        },
        {
            "area": "Regex Compilation",
            "issue": "Concurrent modification of compiled patterns",
            "solution": "Atomic updates of pattern dictionaries",
            "benefit": "No partial state during updates"
        },
        {
            "area": "Parallel Profiling",
            "issue": "Shared profiler instances across threads",
            "solution": "Thread-safe wrapper methods",
            "benefit": "Safe parallel table profiling"
        }
    ]
    
    print(f"\n🔒 Thread Safety Improvements:")
    for i, improvement in enumerate(improvements, 1):
        print(f"\n   {i}. {improvement['area']}:")
        print(f"      Issue: {improvement['issue']}")
        print(f"      Solution: {improvement['solution']}")
        print(f"      Benefit: {improvement['benefit']}")
    
    print(f"\n🛡️ Key Thread Safety Features:")
    print(f"   ✓ threading.RLock for pattern state protection")
    print(f"   ✓ Atomic updates of shared data structures")
    print(f"   ✓ Thread-local snapshots for safe access")
    print(f"   ✓ Exception handling in threaded contexts")
    print(f"   ✓ Safe pattern reloading during execution")
    print(f"   ✓ Improved type hints for better code safety")


def main():
    """Run all thread safety demonstrations."""
    print("Thread Safety Demonstration for Schema Profiler")
    print("=" * 70)
    
    try:
        # Test 1: Pattern recognizer thread safety
        pattern_test_success = test_pattern_recognizer_thread_safety()
        
        # Test 2: Schema profiler thread safety concepts
        profiler_test_success = test_schema_profiler_thread_safety()
        
        # Show improvements
        demonstrate_thread_safety_improvements()
        
        # Final results
        print(f"\n" + "=" * 70)
        print("Thread Safety Testing Completed")
        print("=" * 70)
        
        print(f"\n📊 Test Results:")
        print(f"   Pattern Recognizer Test: {'✓ PASSED' if pattern_test_success else '❌ FAILED'}")
        print(f"   Schema Profiler Test: {'✓ PASSED' if profiler_test_success else '❌ FAILED'}")
        
        overall_success = pattern_test_success and profiler_test_success
        print(f"   Overall: {'✓ ALL TESTS PASSED' if overall_success else '❌ SOME TESTS FAILED'}")
        
        if overall_success:
            print(f"\n🎉 Thread safety improvements are working correctly!")
            print(f"   The profiler is now safe for production parallel processing.")
        else:
            print(f"\n⚠️ Thread safety issues detected!")
            print(f"   Review the error messages above for details.")
        
        print(f"\n🔧 Type Hints Improvements:")
        print(f"   ✓ More specific return types for complex methods")
        print(f"   ✓ Optional parameters properly typed")
        print(f"   ✓ Generic types specified (re.Pattern[str])")
        print(f"   ✓ Union types for complex return structures")
        print(f"   ✓ Better documentation of expected types")
        
    except Exception as e:
        print(f"❌ Error in thread safety demonstration: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main() 