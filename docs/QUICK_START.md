# Schema Profiler Quick Start Guide

## Installation & Setup

### Prerequisites

- Python 3.8+
- Database connectors (mysql-connector-python, psycopg2, pyodbc)
- Access to target databases

### Basic Setup

```python
from connectors.config_loader import ConfigLoader
from profiler import UnifiedProfiler, ProfilerConfig

# Load database configuration
config_loader = ConfigLoader()
connector = config_loader.create_connector_from_config("your_client")

# Create profiler configuration
config = ProfilerConfig(database_name="your_database")

# Initialize profiler
profiler = UnifiedProfiler(connector, config)
```

## 🚀 5-Minute Quick Start

### 1. Basic Schema Profiling

```python
from profiler import UnifiedProfiler, ProfilerConfig

# Minimal configuration
config = ProfilerConfig(database_name="healthcare_db")
profiler = UnifiedProfiler(connector, config)

# Profile the schema
schema = profiler.profile_schema()

# View results
print(f"Database: {schema.database_name}")
print(f"Tables: {schema.total_tables}")
print(f"Columns: {schema.total_columns}")
print(f"Relationships: {len(schema.cross_table_relationships)}")

# Access table details
for table in schema.tables[:3]:  # First 3 tables
    print(f"\nTable: {table.name}")
    print(f"  Columns: {len(table.columns)}")
    print(f"  Primary Keys: {table.primary_keys}")
    print(f"  Foreign Keys: {len(table.foreign_keys)}")
    print(f"  Row Count: {table.estimated_row_count:,}")
```

### 2. Performance Optimization

```python
from profiler import ProfilerConfig, ProfilingStrategy

# Optimized for large databases
config = ProfilerConfig(
    database_name="large_db",
    strategy=ProfilingStrategy.PARALLEL,  # Use parallel processing
    max_workers=8,                        # 8 concurrent threads
    parallel_threshold=5                  # Parallel if 5+ tables
)

profiler = UnifiedProfiler(connector, config)
schema = profiler.profile_schema()
```

### 3. Incremental Profiling (Recommended for Production)

```python
from profiler import CommonConfigs

# Use preset configuration for large databases
config = CommonConfigs.large_database(
    database_name="enterprise_db",
    state_path="./profiling_state.json"
)

profiler = UnifiedProfiler(connector, config)

# First run - profiles all tables (may take several minutes)
schema = profiler.profile_schema()

# Subsequent runs - only changed tables (typically 30-60 seconds)
schema = profiler.profile_schema()  # Automatically incremental!
```

## 📊 Common Use Cases

### Development Environment

```python
from profiler import CommonConfigs

# Optimized for development
config = CommonConfigs.development("dev_db", schema_name="public")
profiler = UnifiedProfiler(connector, config)
schema = profiler.profile_schema()
```

### Production Monitoring

```python
from profiler import ConfigBuilder

config = ConfigBuilder("production_db") \
    .with_parallel_processing(max_workers=12, threshold=3) \
    .with_incremental("./prod_state.json", change_threshold=0.02) \
    .with_resource_limits(max_connections=15, memory_limit_mb=2048) \
    .with_debugging(debug_mode=False, log_level="INFO") \
    .build()

profiler = UnifiedProfiler(connector, config)
schema = profiler.profile_schema()
```

### CI/CD Pipeline

```python
from profiler import CommonConfigs

# Fast profiling for CI/CD
config = CommonConfigs.ci_cd("test_db")
profiler = UnifiedProfiler(connector, config)
schema = profiler.profile_schema()

# Validate schema structure
assert schema.total_tables > 0, "No tables found"
assert len(schema.cross_table_relationships) > 0, "No relationships found"
```

## 🎯 Configuration Presets

### Quick Configurations

```python
from profiler import CommonConfigs

# Development (fast, debug enabled)
dev_config = CommonConfigs.development("my_db")

# Production (optimized, incremental enabled)
prod_config = CommonConfigs.production("my_db", state_path="./state.json")

# Large Database (parallel, incremental, high limits)
large_config = CommonConfigs.large_database("big_db", "./big_state.json")

# CI/CD (fast, limited resources)
ci_config = CommonConfigs.ci_cd("test_db")
```

### Custom Configuration

```python
from profiler import ProfilerConfig, ProfilingStrategy

config = ProfilerConfig(
    # Basic settings
    database_name="my_database",
    schema_name="public",

    # Processing strategy
    strategy=ProfilingStrategy.ADAPTIVE,
    max_workers=4,
    parallel_threshold=10,

    # Incremental profiling
    incremental_enabled=True,
    incremental_state_path="./state.json",
    data_change_threshold=0.1,  # 10% change threshold

    # Resource limits
    max_connections=10,
    query_timeout=300,  # 5 minutes
    memory_limit_mb=1024,  # 1GB

    # Features
    pattern_recognition_enabled=True,
    include_sample_data=True,
    sample_data_limit=5,

    # Debugging
    debug_mode=False,
    log_level="INFO"
)
```

## 📈 Performance Tips

### For Small Databases (< 20 tables)

```python
config = ProfilerConfig(
    database_name="small_db",
    strategy=ProfilingStrategy.SEQUENTIAL,  # No overhead
    max_workers=1
)
```

### For Medium Databases (20-100 tables)

```python
config = ProfilerConfig(
    database_name="medium_db",
    strategy=ProfilingStrategy.ADAPTIVE,    # Auto-select
    max_workers=4,
    parallel_threshold=10
)
```

### For Large Databases (100+ tables)

```python
config = CommonConfigs.large_database(
    database_name="large_db",
    state_path="./large_state.json"
)
# Uses: PARALLEL strategy, 12 workers, incremental enabled
```

### For Very Large Databases (1000+ tables)

```python
config = ProfilerConfig(
    database_name="huge_db",
    strategy=ProfilingStrategy.PARALLEL,
    max_workers=20,
    parallel_threshold=3,
    max_connections=25,
    memory_limit_mb=4096,  # 4GB
    incremental_enabled=True,
    incremental_state_path="./huge_state.json",
    data_change_threshold=0.01  # 1% threshold
)
```

## 🔍 Analyzing Results

### Schema Overview

```python
schema = profiler.profile_schema()

print(f"📊 Schema Overview:")
print(f"  Database: {schema.database_name}")
print(f"  Type: {schema.database_type}")
print(f"  Tables: {schema.total_tables}")
print(f"  Columns: {schema.total_columns}")
print(f"  Relationships: {len(schema.cross_table_relationships)}")
print(f"  Profiled: {schema.profiling_timestamp}")
```

### Table Analysis

```python
# Find largest tables
largest_tables = sorted(schema.tables,
                       key=lambda t: t.estimated_row_count,
                       reverse=True)[:5]

print("\n🏆 Largest Tables:")
for table in largest_tables:
    print(f"  {table.name}: {table.estimated_row_count:,} rows")
```

### Relationship Analysis

```python
# Analyze relationships
print(f"\n🔗 Relationships:")
for rel in schema.cross_table_relationships:
    print(f"  {rel['from_table']}.{rel['from_column']} -> {rel['to_table']}.{rel['to_column']}")

# Find tables with no relationships
isolated_tables = [t for t in schema.tables if not t.foreign_keys]
print(f"\n🏝️  Isolated Tables: {len(isolated_tables)}")
```

### Pattern Analysis

```python
# Pattern summary
if schema.pattern_summary:
    print(f"\n🔍 Detected Patterns:")
    for pattern, count in schema.pattern_summary.items():
        print(f"  {pattern}: {count} columns")
```

### Export Results

```python
# Export to JSON
profiler.export_profile(schema, "./schema_profile.json")

# Export to dictionary
schema_dict = asdict(schema)

# Pretty print specific table
import json
table_data = asdict(schema.tables[0])
print(json.dumps(table_data, indent=2, default=str))
```

## 🛠️ Troubleshooting

### Common Issues

#### Connection Problems

```python
# Test connection first
try:
    connector.connect()
    print("✓ Connection successful")
except Exception as e:
    print(f"✗ Connection failed: {e}")
```

#### Performance Issues

```python
# Enable performance monitoring
config = ProfilerConfig(
    database_name="my_db",
    profile_performance=True,
    debug_mode=True
)

profiler = UnifiedProfiler(connector, config)
schema = profiler.profile_schema()

# Check performance stats
stats = profiler.get_performance_stats()
if stats:
    print(f"Duration: {stats['total_duration_seconds']}s")
    print(f"Slowest table: {stats['slowest_table']}")
```

#### Memory Issues

```python
# Reduce memory usage
config = ProfilerConfig(
    database_name="my_db",
    memory_limit_mb=512,        # Lower limit
    include_sample_data=False,  # Skip sample data
    sample_data_limit=1         # Minimal samples
)
```

#### Incremental State Issues

```python
# Reset incremental state
import os
if os.path.exists("./state.json"):
    os.remove("./state.json")
    print("Incremental state reset")

# Force full profiling
schema = profiler.profile_schema_full()
```

### Debug Mode

```python
import logging

# Enable debug logging
logging.basicConfig(level=logging.DEBUG)

config = ProfilerConfig(
    database_name="my_db",
    debug_mode=True,
    log_level="DEBUG"
)

profiler = UnifiedProfiler(connector, config)
schema = profiler.profile_schema()
```

## 📚 Next Steps

### Advanced Features

- [Incremental Profiling Guide](./INCREMENTAL_PROFILING.md)
- [Performance Optimization](./PERFORMANCE_GUIDE.md)
- [Custom Extensions](./EXTENSIONS.md)

### Examples

- [Complete Examples](../examples/)
- [Clean Architecture Demo](../examples/clean_architecture_demo.py)
- [Production Usage](../examples/production_example.py)

### API Documentation

- [Full Architecture Guide](./PROFILER_ARCHITECTURE.md)
- [Configuration Reference](./CONFIGURATION.md)
- [API Reference](./API_REFERENCE.md)

## 💡 Pro Tips

1. **Start Simple**: Use `CommonConfigs` presets for quick setup
2. **Enable Incremental**: For databases with 50+ tables, always use incremental profiling
3. **Monitor Performance**: Enable performance monitoring in production
4. **Resource Limits**: Set appropriate connection and memory limits
5. **Debug Mode**: Use debug mode for troubleshooting, disable for production
6. **State Management**: Keep incremental state files backed up
7. **Parallel Processing**: Use parallel processing for databases with 10+ tables
8. **Pattern Recognition**: Enable pattern recognition for data governance

Happy profiling! 🚀
