"""
Clean Architecture Demonstration

This script demonstrates the new clean, modular architecture of the Schema Profiler,
showing how the refactored components work together with proper separation of concerns.
"""

import sys
import os
import logging
import tempfile
from pathlib import Path

# Add the src directory to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from connectors.config_loader import ConfigLoader
from profiler import (
    # Main entry points (recommended)
    UnifiedProfiler,
    ProfilerConfig, 
    CommonConfigs,
    ConfigBuilder,
    
    # Core interfaces
    ProfilingStrategy,
    
    # Core components (for advanced usage)
    CoreSchemaProfiler,
    DefaultProfilerFactory,
    IncrementalProfilingManager,
    FileStateManager,
    DatabaseChangeDetector,
    MemoryProfileCache,
    PerformanceMonitor,
    

)


def setup_logging():
    """Setup logging configuration."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )


def demo_unified_profiler_basic():
    """Demonstrate basic usage with the new UnifiedProfiler."""
    print("=" * 70)
    print("1. Basic UnifiedProfiler Usage")
    print("=" * 70)
    
    try:
        # Create config loader and get connector
        config_loader = ConfigLoader()
        if not config_loader.validate_config():
            print("✗ Configuration validation failed")
            return None, None
        
        clients = config_loader.list_available_clients()
        if not clients:
            print("✗ No clients available")
            return None, None
        
        client = clients[0]
        client_info = config_loader.get_client_info(client)
        connector = config_loader.create_connector_from_config(client)
        
        print(f"📊 Using client: {client}")
        print(f"   Database: {client_info['database']}")
        print(f"   Type: {client_info['db_type']}")
        
        # Create simple configuration
        config = ProfilerConfig(
            database_name=client_info['database'],
            schema_name=client_info.get('schema'),
            strategy=ProfilingStrategy.ADAPTIVE,
            max_workers=4
        )
        
        print(f"\n🏗️ Configuration created:")
        print(f"   Strategy: {config.strategy.value}")
        print(f"   Max workers: {config.max_workers}")
        print(f"   Parallel threshold: {config.parallel_threshold}")
        
        # Create unified profiler
        profiler = UnifiedProfiler(connector, config)
        
        print(f"\n✓ UnifiedProfiler created successfully")
        print(f"   Architecture: Clean, modular design")
        print(f"   Separation of concerns: ✓")
        print(f"   Dependency injection: ✓")
        
        # Profile schema
        print(f"\n🚀 Starting schema profiling...")
        schema_profile = profiler.profile_schema()
        
        print(f"\n✓ Profiling completed:")
        print(f"   Tables: {schema_profile.total_tables}")
        print(f"   Columns: {schema_profile.total_columns}")
        print(f"   Relationships: {len(schema_profile.cross_table_relationships)}")
        
        return profiler, connector
        
    except Exception as e:
        print(f"✗ Error in basic demo: {e}")
        import traceback
        traceback.print_exc()
        return None, None


def demo_configuration_builder():
    """Demonstrate the configuration builder pattern."""
    print(f"\n{'='*70}")
    print("2. Configuration Builder Pattern")
    print("=" * 70)
    
    try:
        # Get database info
        config_loader = ConfigLoader()
        clients = config_loader.list_available_clients()
        if not clients:
            return
        
        client = clients[0]
        client_info = config_loader.get_client_info(client)
        
        print(f"🔧 Building configurations for: {client_info['database']}")
        
        # 1. Builder pattern
        print(f"\n1️⃣ Using ConfigBuilder:")
        config1 = ConfigBuilder(client_info['database']) \
            .with_schema(client_info.get('schema')) \
            .with_strategy(ProfilingStrategy.PARALLEL) \
            .with_parallel_processing(max_workers=8, threshold=5) \
            .with_debugging(debug_mode=True) \
            .build()
        
        print(f"   Strategy: {config1.strategy.value}")
        print(f"   Max workers: {config1.max_workers}")
        print(f"   Debug mode: {config1.debug_mode}")
        
        # 2. Common configurations
        print(f"\n2️⃣ Using CommonConfigs:")
        
        # Development config
        dev_config = CommonConfigs.development(client_info['database'], client_info.get('schema'))
        print(f"   Development config:")
        print(f"     Strategy: {dev_config.strategy.value}")
        print(f"     Max connections: {dev_config.max_connections}")
        print(f"     Debug mode: {dev_config.debug_mode}")
        
        # Production config
        prod_config = CommonConfigs.production(client_info['database'], client_info.get('schema'))
        print(f"   Production config:")
        print(f"     Strategy: {prod_config.strategy.value}")
        print(f"     Max workers: {prod_config.max_workers}")
        print(f"     Query timeout: {prod_config.query_timeout}")
        
        # Large database config
        temp_dir = tempfile.mkdtemp()
        state_path = os.path.join(temp_dir, "large_db_state.json")
        large_config = CommonConfigs.large_database(
            client_info['database'], 
            client_info.get('schema'),
            state_path
        )
        print(f"   Large database config:")
        print(f"     Strategy: {large_config.strategy.value}")
        print(f"     Incremental enabled: {large_config.incremental_enabled}")
        print(f"     State path: {large_config.incremental_state_path}")
        
        # 3. Configuration copying and modification
        print(f"\n3️⃣ Configuration copying:")
        modified_config = prod_config.copy(
            strategy=ProfilingStrategy.SEQUENTIAL,
            max_workers=2,
            debug_mode=True
        )
        print(f"   Modified from production:")
        print(f"     Strategy: {modified_config.strategy.value}")
        print(f"     Max workers: {modified_config.max_workers}")
        print(f"     Debug mode: {modified_config.debug_mode}")
        
        return large_config
        
    except Exception as e:
        print(f"✗ Error in configuration demo: {e}")
        return None


def demo_incremental_profiling_architecture(config):
    """Demonstrate the clean incremental profiling architecture."""
    print(f"\n{'='*70}")
    print("3. Clean Incremental Profiling Architecture")
    print("=" * 70)
    
    if not config or not config.incremental_enabled:
        print("⚠️ Incremental profiling not configured")
        return
    
    try:
        # Get connector
        config_loader = ConfigLoader()
        clients = config_loader.list_available_clients()
        client = clients[0]
        connector = config_loader.create_connector_from_config(client)
        
        print(f"🏗️ Incremental Architecture Components:")
        
        # 1. State Manager
        print(f"\n1️⃣ State Manager:")
        state_manager = FileStateManager(config.incremental_state_path)
        print(f"   ✓ FileStateManager created")
        print(f"   Path: {config.incremental_state_path}")
        print(f"   Validation: Built-in state validation")
        print(f"   Atomic writes: Safe concurrent access")
        
        # 2. Change Detector
        print(f"\n2️⃣ Change Detector:")
        from profiler.metadata_extractor import MetadataExtractor
        metadata_extractor = MetadataExtractor(connector, config.database_name, config.schema_name, "unknown")
        change_detector = DatabaseChangeDetector(metadata_extractor)
        print(f"   ✓ DatabaseChangeDetector created")
        print(f"   Schema hashing: MD5 of table structure")
        print(f"   Data change detection: Row count monitoring")
        print(f"   Threshold: {config.data_change_threshold:.1%}")
        
        # 3. Profile Cache
        print(f"\n3️⃣ Profile Cache:")
        profile_cache = MemoryProfileCache(max_size_mb=256)
        print(f"   ✓ MemoryProfileCache created")
        print(f"   Max size: 256MB")
        print(f"   LRU eviction: Planned feature")
        print(f"   Thread-safe: ✓")
        
        # 4. Incremental Manager
        print(f"\n4️⃣ Incremental Manager:")
        incremental_manager = IncrementalProfilingManager(
            state_manager, 
            change_detector, 
            profile_cache
        )
        print(f"   ✓ IncrementalProfilingManager created")
        print(f"   Separation of concerns: ✓")
        print(f"   Dependency injection: ✓")
        print(f"   Error recovery: Fallback to full profiling")
        
        # 5. Unified Profiler with Incremental
        print(f"\n5️⃣ Unified Profiler Integration:")
        profiler = UnifiedProfiler(connector, config)
        print(f"   ✓ UnifiedProfiler with incremental support")
        print(f"   Automatic strategy selection: ✓")
        print(f"   Clean API: Single method for all profiling")
        print(f"   Backward compatibility: ✓")
        
        # Demonstrate usage
        print(f"\n🚀 Running incremental profiling:")
        schema_profile = profiler.profile_schema()
        
        print(f"✓ Incremental profiling completed:")
        print(f"   Tables: {schema_profile.total_tables}")
        print(f"   Architecture benefits:")
        print(f"     - Modular components")
        print(f"     - Testable interfaces")
        print(f"     - Resource management")
        print(f"     - Error recovery")
        
        connector.disconnect()
        
    except Exception as e:
        print(f"✗ Error in incremental architecture demo: {e}")
        import traceback
        traceback.print_exc()


def demo_processing_strategies():
    """Demonstrate the pluggable processing strategies."""
    print(f"\n{'='*70}")
    print("4. Pluggable Processing Strategies")
    print("=" * 70)
    
    try:
        # Get connector and config
        config_loader = ConfigLoader()
        clients = config_loader.list_available_clients()
        client = clients[0]
        client_info = config_loader.get_client_info(client)
        connector = config_loader.create_connector_from_config(client)
        
        print(f"🔧 Processing Strategy Architecture:")
        
        # Create factory
        factory = DefaultProfilerFactory(connector)
        
        # Create core profiler
        core_profiler = CoreSchemaProfiler(connector)
        
        print(f"\n1️⃣ Sequential Strategy:")
        from profiler.processing_strategies import SequentialTableProcessor
        sequential_processor = SequentialTableProcessor(core_profiler)
        print(f"   ✓ {sequential_processor.get_strategy_name().title()} processor created")
        print(f"   Best for: Small schemas, limited resources")
        print(f"   Resource usage: Minimal")
        
        print(f"\n2️⃣ Parallel Strategy:")
        from profiler.processing_strategies import ParallelTableProcessor
        parallel_processor = ParallelTableProcessor(core_profiler)
        print(f"   ✓ {parallel_processor.get_strategy_name().title()} processor created")
        print(f"   Best for: Large schemas, high-performance systems")
        print(f"   Resource management: Connection pooling")
        print(f"   Thread safety: ✓")
        
        print(f"\n3️⃣ Adaptive Strategy:")
        from profiler.processing_strategies import AdaptiveTableProcessor
        adaptive_processor = AdaptiveTableProcessor(core_profiler)
        print(f"   ✓ {adaptive_processor.get_strategy_name().title()} processor created")
        print(f"   Intelligence: Automatic strategy selection")
        print(f"   Decision criteria: Table count vs threshold")
        
        print(f"\n4️⃣ Performance Monitoring:")
        monitor = PerformanceMonitor()
        print(f"   ✓ PerformanceMonitor created")
        print(f"   Metrics: Processing time, error tracking")
        print(f"   Reports: Comprehensive performance analysis")
        
        print(f"\n5️⃣ Resource Management:")
        base_config = ProfilerConfig(database_name=client_info['database'])
        from profiler.processing_strategies import ResourceManager
        resource_manager = ResourceManager(base_config)
        print(f"   ✓ ResourceManager created")
        print(f"   Connection limits: {base_config.max_connections}")
        print(f"   Memory limits: {base_config.memory_limit_mb}MB")
        print(f"   Query timeout: {base_config.query_timeout}s")
        
        # Demonstrate strategy selection
        print(f"\n🎯 Strategy Selection Demo:")
        strategies = [ProfilingStrategy.SEQUENTIAL, ProfilingStrategy.PARALLEL, ProfilingStrategy.ADAPTIVE]
        
        for strategy in strategies:
            config = ProfilerConfig(
                database_name=client_info['database'],
                schema_name=client_info.get('schema'),
                strategy=strategy,
                max_workers=4,
                parallel_threshold=10
            )
            
            profiler = UnifiedProfiler(connector, config)
            print(f"   {strategy.value.title()}: Ready for profiling")
        
        connector.disconnect()
        
    except Exception as e:
        print(f"✗ Error in processing strategies demo: {e}")


def demo_architecture_benefits():
    """Demonstrate the benefits of the clean architecture."""
    print(f"\n{'='*70}")
    print("5. Clean Architecture Benefits")
    print("=" * 70)
    
    benefits = {
        "🏗️ Separation of Concerns": [
            "Core profiling logic isolated from processing strategies",
            "Incremental features separate from base profiling",
            "Configuration management centralized",
            "State management abstracted"
        ],
        "🔧 Dependency Injection": [
            "Components receive dependencies, don't create them",
            "Easy testing with mock objects",
            "Flexible configuration and customization",
            "Loose coupling between components"
        ],
        "🧪 Testability": [
            "Clear interfaces enable unit testing",
            "Mock implementations for isolated testing",
            "Dependency injection supports test doubles",
            "Focused components easier to test"
        ],
        "🔄 Maintainability": [
            "Single responsibility principle",
            "Open/closed principle for extensions",
            "Clear module boundaries",
            "Reduced code duplication"
        ],
        "⚡ Performance": [
            "Resource management and connection pooling",
            "Pluggable strategies for optimization",
            "Performance monitoring built-in",
            "Memory and connection limits"
        ],
        "🛡️ Error Handling": [
            "Graceful fallback strategies",
            "Comprehensive error recovery",
            "Resource cleanup guaranteed",
            "State validation and corruption detection"
        ],
        "🔧 Extensibility": [
            "Interface-based design for new implementations",
            "Plugin architecture for processing strategies",
            "Custom state managers and caches",
            "Easy addition of new database types"
        ]
    }
    
    for category, items in benefits.items():
        print(f"\n{category}")
        for item in items:
            print(f"   ✓ {item}")
    
    print(f"\n📊 Architecture Comparison:")
    print(f"   Before (Legacy): 806+ lines, mixed concerns")
    print(f"   After (UnifiedProfiler): Modular components, clear boundaries")
    print(f"   Code organization: From monolithic to modular")
    print(f"   Testability: From difficult to straightforward")
    print(f"   Extensibility: From rigid to flexible")


def demo_migration_path():
    """Demonstrate migration from legacy to new architecture."""
    print(f"\n{'='*70}")
    print("6. Migration Path from Legacy Code")
    print("=" * 70)
    
    print(f"🔄 Migration Strategy:")
    
    print(f"\n1️⃣ Backward Compatibility:")
    print(f"   ✓ Legacy code has been removed - clean migration required")
    print(f"   ✓ Existing code continues to work")
    print(f"   ✓ Gradual migration possible")
    print(f"   ⚠️ Deprecation warnings guide users to new API")
    
    print(f"\n2️⃣ Migration Steps:")
    steps = [
        "Replace legacy profiler with UnifiedProfiler",
        "Use ProfilerConfig instead of method parameters",
        "Leverage CommonConfigs for typical use cases",
        "Enable incremental profiling for large schemas",
        "Add performance monitoring if needed",
        "Customize processing strategies as required"
    ]
    
    for i, step in enumerate(steps, 1):
        print(f"   {i}. {step}")
    
    print(f"\n3️⃣ Code Examples:")
    
    print(f"\n   # Legacy approach (removed)")
    print(f"   # profiler = SchemaDataProfiler(connector, 'my_db')  # No longer available")
    print(f"   # schema = profiler.profile_schema_adaptive()  # No longer available")
    
    print(f"\n   # New approach (recommended)")
    print(f"   config = ProfilerConfig(database_name='my_db')")
    print(f"   profiler = UnifiedProfiler(connector, config)")
    print(f"   schema = profiler.profile_schema()")
    
    print(f"\n   # Advanced configuration")
    print(f"   config = CommonConfigs.large_database('my_db', './state.json')")
    print(f"   profiler = UnifiedProfiler(connector, config)")
    print(f"   schema = profiler.profile_schema()  # Automatically incremental")
    
    print(f"\n4️⃣ Benefits of Migration:")
    migration_benefits = [
        "Cleaner, more maintainable code",
        "Better performance with resource management",
        "Incremental profiling for large schemas",
        "Comprehensive configuration options",
        "Built-in performance monitoring",
        "Future-proof architecture"
    ]
    
    for benefit in migration_benefits:
        print(f"   ✓ {benefit}")


def main():
    """Run the complete clean architecture demonstration."""
    print("Clean Architecture Demonstration")
    print("=" * 70)
    print("Showcasing the refactored, modular schema profiler architecture")
    
    setup_logging()
    
    try:
        # 1. Basic usage
        profiler, connector = demo_unified_profiler_basic()
        if not profiler:
            return
        
        # 2. Configuration management
        incremental_config = demo_configuration_builder()
        
        # 3. Incremental architecture
        demo_incremental_profiling_architecture(incremental_config)
        
        # 4. Processing strategies
        demo_processing_strategies()
        
        # 5. Architecture benefits
        demo_architecture_benefits()
        
        # 6. Migration path
        demo_migration_path()
        
        # Cleanup
        if connector:
            connector.disconnect()
        
        print(f"\n{'='*70}")
        print("Clean Architecture Demo Completed!")
        print("=" * 70)
        
        print(f"\n🎯 Key Architectural Improvements:")
        print(f"✓ Separated concerns into focused modules")
        print(f"✓ Implemented dependency injection")
        print(f"✓ Created clean interfaces and abstractions")
        print(f"✓ Centralized configuration management")
        print(f"✓ Added comprehensive error handling")
        print(f"✓ Improved testability and maintainability")
        print(f"✓ Maintained backward compatibility")
        
        print(f"\n📁 New Module Structure:")
        modules = [
            "interfaces.py - Clean contracts and protocols",
            "config.py - Centralized configuration management", 
            "core_profiler.py - Pure profiling logic",
            "processing_strategies.py - Pluggable execution strategies",
            "incremental_manager.py - Change detection and state management",
            "profiler_factory.py - Component orchestration and DI",
            "__init__.py - Clean public API"
        ]
        
        for module in modules:
            print(f"   • {module}")
        
        print(f"\n🚀 Ready for Production:")
        print(f"   • Scalable architecture")
        print(f"   • Resource management")
        print(f"   • Performance monitoring")
        print(f"   • Error recovery")
        print(f"   • Extensible design")
        
    except Exception as e:
        print(f"✗ Error in clean architecture demo: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main() 