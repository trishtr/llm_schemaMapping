"""
Database Schema Profiler Package

This package provides a clean, modular architecture for comprehensive database schema profiling.

## Core Architecture

### Main Entry Points:
- UnifiedProfiler: Primary interface for all profiling operations
- ProfilerConfig: Centralized configuration management

### Core Components:
- CoreSchemaProfiler: Pure schema profiling logic
- ProcessingStrategies: Pluggable sequential/parallel execution
- IncrementalManager: Change detection and state management
- MetadataExtractor: Database metadata extraction

### Data Models:
- ColumnProfile, TableProfile, SchemaProfile: Structured schema information
- ProfilerConfig: Comprehensive configuration system

### Interfaces:
- Clean abstractions for testability and extensibility
- Dependency injection for loose coupling

## Usage Examples

### Basic Profiling:
```python
from profiler import UnifiedProfiler, ProfilerConfig

config = ProfilerConfig(database_name="my_db")
profiler = UnifiedProfiler(connector, config)
schema = profiler.profile_schema()
```

### Incremental Profiling:
```python
config = ProfilerConfig(
    database_name="my_db",
    incremental_enabled=True,
    incremental_state_path="./state.json"
)
profiler = UnifiedProfiler(connector, config)
schema = profiler.profile_schema()  # Automatically incremental
```

### Custom Configuration:
```python
from profiler.config import CommonConfigs

config = CommonConfigs.large_database("my_db", state_path="./state.json")
profiler = UnifiedProfiler(connector, config)
```
"""

# Main entry points (recommended for users)
from .profiler_factory import UnifiedProfiler, DefaultProfilerFactory
from .config import ProfilerConfig, CommonConfigs, ConfigBuilder

# Core interfaces and components
from .interfaces import ProfilingStrategy, SchemaProfiler, IncrementalProfiler
from .core_profiler import CoreSchemaProfiler
from .processing_strategies import (
    SequentialTableProcessor, 
    ParallelTableProcessor, 
    AdaptiveTableProcessor,
    PerformanceMonitor,
    ResourceManager
)
from .incremental_manager import (
    IncrementalProfilingManager,
    FileStateManager,
    DatabaseChangeDetector,
    MemoryProfileCache
)

# Data models and utilities
from .schema_models import ColumnProfile, TableProfile, SchemaProfile
from .simple_pattern_recognizer import SimplePatternRecognizer as FieldPatternRecognizer
from .database_dialect import DatabaseDialect
from .metadata_extractor import MetadataExtractor

# Legacy compatibility removed - SchemaDataProfiler has been replaced by UnifiedProfiler
# For migration guidance, see examples/clean_architecture_demo.py

__all__ = [
    # Main entry points (recommended)
    'UnifiedProfiler',
    'ProfilerConfig', 
    'CommonConfigs',
    'ConfigBuilder',
    
    # Core interfaces
    'ProfilingStrategy',
    'SchemaProfiler',
    'IncrementalProfiler',
    
    # Core components
    'CoreSchemaProfiler',
    'DefaultProfilerFactory',
    'SequentialTableProcessor',
    'ParallelTableProcessor', 
    'AdaptiveTableProcessor',
    'IncrementalProfilingManager',
    'FileStateManager',
    'DatabaseChangeDetector',
    'MemoryProfileCache',
    'PerformanceMonitor',
    'ResourceManager',
    
    # Data models
    'ColumnProfile',
    'TableProfile', 
    'SchemaProfile',
    
    # Utilities
    'FieldPatternRecognizer',
    'DatabaseDialect',
    'MetadataExtractor'
]

# Version info
__version__ = "2.0.0"
__architecture__ = "clean_modular" 