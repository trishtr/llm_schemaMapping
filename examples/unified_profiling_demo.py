"""
Unified Schema Profiling Demo

This example demonstrates the new unified profile_schema() method with
strategy parameter, replacing the previous three separate methods.
"""

import sys
import os
import logging

# Add the src directory to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from connectors.config_loader import ConfigLoader
from profiler import SchemaDataProfiler, ProfilingStrategy


def setup_logging():
    """Setup logging configuration."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )


def demo_unified_profiling():
    """Demonstrate the unified profiling API with different strategies."""
    print("Unified Schema Profiling API Demo")
    print("=" * 50)
    
    setup_logging()
    
    try:
        # 1. Create config loader and get a connector
        print("\n1. Loading database configurations...")
        config_loader = ConfigLoader()
        
        if not config_loader.validate_config():
            print("✗ Configuration validation failed")
            return
        
        clients = config_loader.list_available_clients()
        if not clients:
            print("✗ No clients available")
            return
        
        # Use the first available client for demo
        client = clients[0]
        print(f"Using client: {client}")
        
        # Get client info and create connector
        client_info = config_loader.get_client_info(client)
        connector = config_loader.create_connector_from_config(client)
        
        # Create profiler
        profiler = SchemaDataProfiler(
            connector=connector,
            database_name=client_info['database'],
            schema_name=client_info.get('schema')
        )
        
        print(f"\n2. Demonstrating Different Profiling Strategies:")
        print(f"   Database: {client_info['database']}")
        print(f"   Type: {client_info['db_type']}")
        
        # Strategy 1: Sequential Processing
        print(f"\n{'='*60}")
        print("Strategy 1: SEQUENTIAL Processing")
        print(f"{'='*60}")
        
        schema_profile_seq = profiler.profile_schema(
            strategy=ProfilingStrategy.SEQUENTIAL
        )
        
        print(f"Sequential Results:")
        print(f"  - Tables: {schema_profile_seq.total_tables}")
        print(f"  - Columns: {schema_profile_seq.total_columns}")
        print(f"  - Relationships: {len(schema_profile_seq.cross_table_relationships)}")
        
        # Strategy 2: Parallel Processing
        print(f"\n{'='*60}")
        print("Strategy 2: PARALLEL Processing")
        print(f"{'='*60}")
        
        schema_profile_par = profiler.profile_schema(
            strategy=ProfilingStrategy.PARALLEL,
            max_workers=4
        )
        
        print(f"Parallel Results:")
        print(f"  - Tables: {schema_profile_par.total_tables}")
        print(f"  - Columns: {schema_profile_par.total_columns}")
        print(f"  - Relationships: {len(schema_profile_par.cross_table_relationships)}")
        
        # Strategy 3: Adaptive Processing
        print(f"\n{'='*60}")
        print("Strategy 3: ADAPTIVE Processing")
        print(f"{'='*60}")
        
        schema_profile_adapt = profiler.profile_schema(
            strategy=ProfilingStrategy.ADAPTIVE,
            parallel_threshold=5,
            max_workers=6
        )
        
        print(f"Adaptive Results:")
        print(f"  - Tables: {schema_profile_adapt.total_tables}")
        print(f"  - Columns: {schema_profile_adapt.total_columns}")
        print(f"  - Relationships: {len(schema_profile_adapt.cross_table_relationships)}")
        
        # Strategy 4: Default (Adaptive)
        print(f"\n{'='*60}")
        print("Strategy 4: DEFAULT (Adaptive)")
        print(f"{'='*60}")
        
        schema_profile_default = profiler.profile_schema()  # Uses default ADAPTIVE strategy
        
        print(f"Default Results:")
        print(f"  - Tables: {schema_profile_default.total_tables}")
        print(f"  - Columns: {schema_profile_default.total_columns}")
        print(f"  - Relationships: {len(schema_profile_default.cross_table_relationships)}")
        
        # Demonstrate backward compatibility (with deprecation warnings)
        print(f"\n{'='*60}")
        print("Backward Compatibility Demo (with deprecation warnings)")
        print(f"{'='*60}")
        
        print("Testing deprecated methods (warnings expected):")
        
        # This will show deprecation warnings
        import warnings
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            
            # Call deprecated methods
            schema_old_seq = profiler.profile_schema_sequential()
            schema_old_par = profiler.profile_schema_parallel(max_workers=2)
            schema_old_adapt = profiler.profile_schema_adaptive(parallel_threshold=8)
            
            print(f"  - Deprecated methods still work")
            print(f"  - Generated {len(w)} deprecation warnings")
            for warning in w:
                print(f"    Warning: {warning.message}")
        
        # Disconnect
        connector.disconnect()
        print(f"\n✓ Unified profiling demo completed successfully!")
        
    except Exception as e:
        print(f"✗ Error in unified profiling demo: {e}")
        import traceback
        traceback.print_exc()


def demo_strategy_comparison():
    """Compare different strategies and their use cases."""
    print(f"\n{'='*70}")
    print("Strategy Comparison and Use Cases")
    print(f"{'='*70}")
    
    strategies = [
        {
            "name": "SEQUENTIAL",
            "enum": ProfilingStrategy.SEQUENTIAL,
            "use_cases": [
                "Small databases (< 10 tables)",
                "Limited system resources",
                "Single-threaded environments",
                "Debugging and development"
            ],
            "pros": ["Simple", "Predictable", "Low memory usage"],
            "cons": ["Slower for large schemas", "No parallelization benefit"]
        },
        {
            "name": "PARALLEL",
            "enum": ProfilingStrategy.PARALLEL,
            "use_cases": [
                "Large databases (> 20 tables)",
                "Multi-core systems with good resources",
                "Production environments",
                "Time-critical profiling"
            ],
            "pros": ["Faster for large schemas", "Better resource utilization"],
            "cons": ["Higher memory usage", "More complex error handling"]
        },
        {
            "name": "ADAPTIVE",
            "enum": ProfilingStrategy.ADAPTIVE,
            "use_cases": [
                "Unknown database sizes",
                "General-purpose profiling",
                "Automated systems",
                "Default choice for most cases"
            ],
            "pros": ["Automatic optimization", "Best of both worlds", "Safe default"],
            "cons": ["Slight overhead for decision making"]
        }
    ]
    
    for strategy in strategies:
        print(f"\n{strategy['name']} Strategy:")
        print(f"  Enum: ProfilingStrategy.{strategy['name']}")
        print(f"  Use Cases:")
        for use_case in strategy['use_cases']:
            print(f"    - {use_case}")
        print(f"  Pros: {', '.join(strategy['pros'])}")
        print(f"  Cons: {', '.join(strategy['cons'])}")
    
    print(f"\nAPI Examples:")
    print(f"  # Sequential")
    print(f"  profiler.profile_schema(strategy=ProfilingStrategy.SEQUENTIAL)")
    print(f"  ")
    print(f"  # Parallel with 8 workers")
    print(f"  profiler.profile_schema(strategy=ProfilingStrategy.PARALLEL, max_workers=8)")
    print(f"  ")
    print(f"  # Adaptive with custom threshold")
    print(f"  profiler.profile_schema(strategy=ProfilingStrategy.ADAPTIVE, parallel_threshold=15)")
    print(f"  ")
    print(f"  # Default (Adaptive)")
    print(f"  profiler.profile_schema()")


def main():
    """Run the unified profiling demonstration."""
    try:
        demo_unified_profiling()
        demo_strategy_comparison()
        
        print(f"\n{'='*70}")
        print("Unified Profiling Demo Completed!")
        print(f"{'='*70}")
        
        print(f"\nKey Improvements:")
        print(f"✓ Single unified method: profile_schema()")
        print(f"✓ Strategy-based approach with enum")
        print(f"✓ Cleaner, more flexible API")
        print(f"✓ Backward compatibility maintained")
        print(f"✓ Better parameter organization")
        print(f"✓ Consistent error handling across strategies")
        print(f"✓ Improved logging and monitoring")
        print(f"✓ Strategy-specific optimizations")
        
    except Exception as e:
        print(f"✗ Error running unified profiling demo: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main() 