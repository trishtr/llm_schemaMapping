# Schema Profiler API Reference

## Core Classes

### UnifiedProfiler

The main entry point for all profiling operations.

```python
class UnifiedProfiler:
    def __init__(self, connector, config: ProfilerConfig)
    def profile_schema(self, custom_config: Optional[ProfilerConfig] = None) -> SchemaProfile
    def profile_schema_full(self, custom_config: Optional[ProfilerConfig] = None) -> SchemaProfile
    def profile_table(self, table_name: str, custom_config: Optional[ProfilerConfig] = None) -> TableProfile
    def get_tables_info(self) -> List[Dict[str, Any]]
    def export_profile(self, schema_profile: SchemaProfile, output_path: str) -> Dict[str, Any]
    def get_performance_stats(self) -> Optional[Dict[str, Any]]
    def get_resource_stats(self) -> Optional[Dict[str, Any]]
```

#### Methods

##### `__init__(connector, config: ProfilerConfig)`

Initialize the unified profiler.

**Parameters:**

- `connector`: Database connector instance
- `config`: Profiler configuration

**Example:**

```python
config = ProfilerConfig(database_name="my_db")
profiler = UnifiedProfiler(connector, config)
```

##### `profile_schema(custom_config: Optional[ProfilerConfig] = None) -> SchemaProfile`

Profile the complete database schema.

**Parameters:**

- `custom_config`: Optional custom configuration (uses instance config if None)

**Returns:**

- `SchemaProfile`: Complete schema information

**Example:**

```python
schema = profiler.profile_schema()
print(f"Tables: {schema.total_tables}")
```

##### `profile_schema_full(custom_config: Optional[ProfilerConfig] = None) -> SchemaProfile`

Force full schema profiling (bypass incremental).

**Parameters:**

- `custom_config`: Optional custom configuration

**Returns:**

- `SchemaProfile`: Complete schema information

**Example:**

```python
# Force full profiling even with incremental enabled
schema = profiler.profile_schema_full()
```

##### `profile_table(table_name: str, custom_config: Optional[ProfilerConfig] = None) -> TableProfile`

Profile a single table.

**Parameters:**

- `table_name`: Name of the table to profile
- `custom_config`: Optional custom configuration

**Returns:**

- `TableProfile`: Complete table information

**Example:**

```python
table = profiler.profile_table("users")
print(f"Columns: {len(table.columns)}")
```

### ProfilerConfig

Centralized configuration for all profiling operations.

```python
@dataclass
class ProfilerConfig:
    # Core settings
    database_name: str
    schema_name: Optional[str] = None

    # Processing strategy
    strategy: ProfilingStrategy = ProfilingStrategy.ADAPTIVE
    max_workers: int = 4
    parallel_threshold: int = 10

    # Incremental profiling
    incremental_enabled: bool = False
    incremental_state_path: Optional[str] = None
    data_change_threshold: float = 0.1
    force_full_profile: bool = False

    # Resource limits
    max_connections: int = 10
    query_timeout: int = 300
    memory_limit_mb: int = 1024

    # Pattern recognition
    pattern_recognition_enabled: bool = True
    patterns_config_path: Optional[str] = None

    # Logging and debugging
    log_level: str = "INFO"
    debug_mode: bool = False
    profile_performance: bool = False

    # Output settings
    export_format: str = "json"
    output_path: Optional[str] = None
    include_sample_data: bool = True
    sample_data_limit: int = 5

    # Validation settings
    validate_relationships: bool = True
    validate_patterns: bool = True
    strict_mode: bool = False
```

#### Methods

##### `to_dict() -> Dict[str, Any]`

Convert configuration to dictionary.

##### `from_dict(cls, data: Dict[str, Any]) -> ProfilerConfig`

Create configuration from dictionary.

##### `from_file(cls, file_path: str) -> ProfilerConfig`

Load configuration from JSON file.

##### `save_to_file(file_path: str) -> None`

Save configuration to JSON file.

##### `copy(**changes) -> ProfilerConfig`

Create a copy with optional changes.

**Example:**

```python
config = ProfilerConfig(database_name="test")
prod_config = config.copy(
    strategy=ProfilingStrategy.PARALLEL,
    max_workers=8
)
```

### ConfigBuilder

Builder pattern for creating configurations.

```python
class ConfigBuilder:
    def __init__(self, database_name: str)
    def with_schema(self, schema_name: str) -> ConfigBuilder
    def with_strategy(self, strategy: ProfilingStrategy) -> ConfigBuilder
    def with_parallel_processing(self, max_workers: int = 4, threshold: int = 10) -> ConfigBuilder
    def with_incremental(self, state_path: str, change_threshold: float = 0.1) -> ConfigBuilder
    def with_patterns(self, config_path: Optional[str] = None, enabled: bool = True) -> ConfigBuilder
    def with_resource_limits(self, max_connections: int = 10, query_timeout: int = 300, memory_limit_mb: int = 1024) -> ConfigBuilder
    def with_debugging(self, debug_mode: bool = True, log_level: str = "DEBUG", profile_performance: bool = True) -> ConfigBuilder
    def build() -> ProfilerConfig
```

**Example:**

```python
config = ConfigBuilder("my_db") \
    .with_schema("public") \
    .with_parallel_processing(max_workers=8) \
    .with_incremental("./state.json") \
    .build()
```

### CommonConfigs

Predefined configurations for common use cases.

```python
class CommonConfigs:
    @staticmethod
    def development(database_name: str, schema_name: Optional[str] = None) -> ProfilerConfig

    @staticmethod
    def production(database_name: str, schema_name: Optional[str] = None, state_path: Optional[str] = None) -> ProfilerConfig

    @staticmethod
    def large_database(database_name: str, schema_name: Optional[str] = None, state_path: str = "./incremental_state.json") -> ProfilerConfig

    @staticmethod
    def ci_cd(database_name: str, schema_name: Optional[str] = None) -> ProfilerConfig
```

**Example:**

```python
# Development configuration
dev_config = CommonConfigs.development("my_db")

# Production with incremental profiling
prod_config = CommonConfigs.production("prod_db", state_path="./prod_state.json")

# Large database optimization
large_config = CommonConfigs.large_database("big_db")
```

## Data Models

### SchemaProfile

Complete database schema information.

```python
@dataclass
class SchemaProfile:
    database_name: str
    schema_name: Optional[str]
    database_type: str
    total_tables: int = 0
    total_columns: int = 0
    tables: List[TableProfile] = field(default_factory=list)
    cross_table_relationships: List[Dict[str, Any]] = field(default_factory=list)
    potential_relationships: List[Dict[str, Any]] = field(default_factory=list)
    pattern_summary: Dict[str, int] = field(default_factory=dict)
    profiling_timestamp: datetime = field(default_factory=datetime.now)
```

#### Attributes

- `database_name`: Name of the profiled database
- `schema_name`: Schema name (if applicable)
- `database_type`: Database type (mysql, postgresql, mssql)
- `total_tables`: Total number of tables profiled
- `total_columns`: Total number of columns across all tables
- `tables`: List of individual table profiles
- `cross_table_relationships`: Foreign key relationships between tables
- `potential_relationships`: Suspected relationships based on naming patterns
- `pattern_summary`: Summary of detected patterns across all columns
- `profiling_timestamp`: When the profiling was performed

### TableProfile

Complete table information.

```python
@dataclass
class TableProfile:
    name: str
    schema: Optional[str]
    columns: List[ColumnProfile]
    primary_keys: List[str]
    foreign_keys: List[Dict[str, str]]
    indexes: List[Dict[str, Any]]
    sample_data: List[Dict[str, Any]]
    estimated_row_count: int
    self_referencing_columns: List[str]
    potential_fk_candidates: List[Dict[str, str]]
    table_comment: Optional[str] = None
    creation_time: Optional[datetime] = None
    last_modified: Optional[datetime] = None
```

#### Attributes

- `name`: Table name
- `schema`: Schema name (if applicable)
- `columns`: List of column profiles
- `primary_keys`: List of primary key column names
- `foreign_keys`: List of foreign key definitions
- `indexes`: List of index definitions
- `sample_data`: Sample rows from the table
- `estimated_row_count`: Approximate number of rows
- `self_referencing_columns`: Columns that reference the same table
- `potential_fk_candidates`: Suspected foreign key relationships

### ColumnProfile

Complete column information.

```python
@dataclass
class ColumnProfile:
    name: str
    data_type: str
    is_nullable: bool
    is_primary_key: bool = False
    is_foreign_key: bool = False
    is_unique: bool = False
    is_indexed: bool = False
    max_length: Optional[int] = None
    default_value: Optional[str] = None
    column_comment: Optional[str] = None
    ordinal_position: int = 0
    detected_patterns: List[str] = field(default_factory=list)
    sample_values: List[Any] = field(default_factory=list)
    foreign_key_reference: Optional[Dict[str, str]] = None
```

#### Attributes

- `name`: Column name
- `data_type`: Database-specific data type
- `is_nullable`: Whether column allows NULL values
- `is_primary_key`: Whether column is part of primary key
- `is_foreign_key`: Whether column is a foreign key
- `is_unique`: Whether column has unique constraint
- `is_indexed`: Whether column is indexed
- `max_length`: Maximum character length (for string types)
- `default_value`: Default value for the column
- `column_comment`: Database comment/description
- `ordinal_position`: Column position in table
- `detected_patterns`: List of detected data patterns
- `sample_values`: Sample values from the column
- `foreign_key_reference`: Foreign key target information

## Enums

### ProfilingStrategy

Available processing strategies.

```python
class ProfilingStrategy(Enum):
    SEQUENTIAL = "sequential"  # Process tables one by one
    PARALLEL = "parallel"      # Process tables concurrently
    ADAPTIVE = "adaptive"      # Automatically choose based on table count
```

**Usage:**

```python
config = ProfilerConfig(
    database_name="my_db",
    strategy=ProfilingStrategy.PARALLEL
)
```

## Processing Strategies

### SequentialTableProcessor

Processes tables one by one.

```python
class SequentialTableProcessor(TableProcessor):
    def __init__(self, core_profiler: CoreSchemaProfiler)
    def process_tables(self, tables_info: List[Dict[str, Any]], config: ProfilerConfig) -> List[TableProfile]
    def get_strategy_name(self) -> str
```

**Best for:**

- Small databases (< 20 tables)
- Limited system resources
- Debugging and development

### ParallelTableProcessor

Processes tables concurrently using thread pools.

```python
class ParallelTableProcessor(TableProcessor):
    def __init__(self, core_profiler: CoreSchemaProfiler)
    def process_tables(self, tables_info: List[Dict[str, Any]], config: ProfilerConfig) -> List[TableProfile]
    def get_strategy_name(self) -> str
```

**Best for:**

- Large databases (50+ tables)
- High-performance systems
- Production environments

**Features:**

- Connection pooling
- Resource management
- Thread safety
- Error isolation

### AdaptiveTableProcessor

Automatically chooses between sequential and parallel processing.

```python
class AdaptiveTableProcessor(TableProcessor):
    def __init__(self, core_profiler: CoreSchemaProfiler)
    def process_tables(self, tables_info: List[Dict[str, Any]], config: ProfilerConfig) -> List[TableProfile]
    def get_strategy_name(self) -> str
```

**Decision Logic:**

- Uses parallel if `table_count >= parallel_threshold`
- Uses sequential otherwise
- Considers `max_workers` configuration

## Incremental Profiling

### IncrementalProfilingManager

Main orchestrator for incremental profiling.

```python
class IncrementalProfilingManager(IncrementalProfiler):
    def __init__(self, state_manager: StateManager, change_detector: ChangeDetector, profile_cache: Optional[ProfileCache] = None)
    def profile_incremental(self, base_profiler: SchemaProfiler, config: ProfilerConfig) -> SchemaProfile
```

### FileStateManager

File-based state persistence.

```python
class FileStateManager(StateManager):
    def __init__(self, file_path: str)
    def load_state(self) -> Optional[Dict[str, Any]]
    def save_state(self, state: Dict[str, Any]) -> None
    def validate_state(self, state: Dict[str, Any]) -> bool
```

**Features:**

- Atomic writes
- State validation
- JSON format
- Error recovery

### DatabaseChangeDetector

Detects changes in database schema and data.

```python
class DatabaseChangeDetector(ChangeDetector):
    def __init__(self, metadata_extractor)
    def identify_changed_tables(self, current_tables: List[Dict[str, Any]], previous_state: Optional[Dict[str, Any]], config: ProfilerConfig) -> List[Dict[str, Any]]
```

**Detection Methods:**

- **Schema Changes**: MD5 hash of table structure
- **Data Changes**: Row count monitoring with configurable threshold
- **New Tables**: Tables not in previous state
- **Removed Tables**: Tables no longer present

### MemoryProfileCache

In-memory caching for table profiles.

```python
class MemoryProfileCache(ProfileCache):
    def __init__(self, max_size_mb: int = 256)
    def get_cached_profile(self, table_name: str) -> Optional[TableProfile]
    def cache_profile(self, table_name: str, profile: TableProfile) -> None
    def clear_cache(self) -> None
    def get_cache_stats(self) -> Dict[str, Any]
```

## Utilities

### MetadataExtractor

Extracts comprehensive metadata from databases.

```python
class MetadataExtractor(DatabaseQuery, MetadataQueryMixin):
    def __init__(self, connector, database_name: str, schema_name: Optional[str], db_type: str)
    def get_tables_info(self) -> List[Dict[str, Any]]
    def get_column_profiles(self, table_name: str) -> List[ColumnProfile]
    def get_primary_keys(self, table_name: str) -> List[str]
    def get_foreign_keys(self, table_name: str) -> List[Dict[str, str]]
    def get_indexes(self, table_name: str) -> List[Dict[str, Any]]
    def get_sample_data(self, table_name: str, limit: int = 5) -> List[Dict[str, Any]]
    def get_row_count(self, table_name: str) -> int
    def get_complete_table_metadata(self, table_name: str) -> Dict[str, Any]
```

### FieldPatternRecognizer

Recognizes patterns in field data.

```python
class FieldPatternRecognizer:
    def __init__(self, patterns_config_path: Optional[Union[str, Path]] = None)
    def detect_patterns(self, values: List[Any], field_name: Optional[str] = None) -> List[str]
    def detect_patterns_with_confidence(self, values: List[Any], field_name: Optional[str] = None) -> List[Tuple[str, float]]
    def reload_patterns(self) -> None
```

**Supported Patterns:**

- Healthcare identifiers (NPI, DEA)
- Email addresses
- Phone numbers
- Social Security Numbers
- Credit card numbers
- Custom regex patterns

### DatabaseDialect

Database-specific SQL syntax and operations.

```python
class DatabaseDialect:
    def __init__(self, db_type: str)
    def get_tables_query(self, schema_name: Optional[str] = None) -> Tuple[str, tuple]
    def get_columns_query(self, table_name: str, schema_name: Optional[str] = None) -> Tuple[str, tuple]
    def get_primary_keys_query(self, table_name: str, schema_name: Optional[str] = None) -> Tuple[str, tuple]
    def get_foreign_keys_query(self, table_name: str, schema_name: Optional[str] = None) -> Tuple[str, tuple]
    def get_indexes_query(self, table_name: str, schema_name: Optional[str] = None) -> Tuple[str, tuple]
    def get_sample_data_query(self, table_name: str, schema_name: Optional[str] = None, limit: int = 5) -> Tuple[str, tuple]
    def get_row_count_query(self, table_name: str, schema_name: Optional[str] = None) -> Tuple[str, tuple]
```

**Supported Databases:**

- MySQL
- PostgreSQL
- Microsoft SQL Server

## Performance Monitoring

### PerformanceMonitor

Monitors processing performance and resource usage.

```python
class PerformanceMonitor:
    def __init__(self)
    def start_monitoring(self) -> None
    def end_monitoring(self) -> None
    def record_table_time(self, table_name: str, duration: float) -> None
    def record_error(self, table_name: str, error: str) -> None
    def get_performance_report(self) -> Dict[str, Any]
```

**Metrics Collected:**

- Total processing duration
- Per-table processing times
- Error counts and details
- Tables per second
- Slowest/fastest tables

### ResourceManager

Manages system resources during processing.

```python
class ResourceManager:
    def __init__(self, config: ProfilerConfig)
    def acquire_connection(self) -> bool
    def release_connection(self) -> None
    def check_memory_usage(self) -> bool
    def get_resource_status(self) -> Dict[str, Any]
```

**Resource Management:**

- Connection pooling
- Memory usage monitoring
- Query timeout enforcement
- Thread pool management

## Factory Pattern

### DefaultProfilerFactory

Creates and configures profiler components.

```python
class DefaultProfilerFactory(ProfilerFactory):
    def __init__(self, connector)
    def create_profiler(self, config: ProfilerConfig) -> SchemaProfiler
    def create_incremental_profiler(self, config: ProfilerConfig) -> IncrementalProfiler
    def create_table_processor(self, strategy: ProfilingStrategy) -> TableProcessor
```

**Dependency Injection:**

- Creates components with proper dependencies
- Handles configuration propagation
- Ensures consistent object creation

## Error Handling

### Custom Exceptions

```python
class ProfilerError(Exception):
    """Base exception for profiler errors"""

class ConfigurationError(ProfilerError):
    """Configuration validation errors"""

class DatabaseConnectionError(ProfilerError):
    """Database connection errors"""

class IncrementalStateError(ProfilerError):
    """Incremental state management errors"""
```

### Error Recovery Strategies

1. **Incremental Fallback**: Falls back to full profiling on incremental errors
2. **Partial Processing**: Continues with other tables on individual table errors
3. **State Recovery**: Validates and recovers from corrupted state files
4. **Resource Cleanup**: Ensures proper cleanup on errors

## Thread Safety

All components are designed to be thread-safe:

- **Pattern Recognition**: Uses `threading.RLock` for thread-safe access
- **State Management**: Atomic file operations
- **Connection Management**: Thread-safe connection pooling
- **Cache Operations**: Thread-safe cache access

## Example Usage Patterns

### Basic Usage

```python
from profiler import UnifiedProfiler, ProfilerConfig

config = ProfilerConfig(database_name="my_db")
profiler = UnifiedProfiler(connector, config)
schema = profiler.profile_schema()
```

### Advanced Configuration

```python
from profiler import ConfigBuilder, ProfilingStrategy

config = ConfigBuilder("enterprise_db") \
    .with_parallel_processing(max_workers=12) \
    .with_incremental("./enterprise_state.json") \
    .with_resource_limits(max_connections=20, memory_limit_mb=4096) \
    .build()

profiler = UnifiedProfiler(connector, config)
schema = profiler.profile_schema()
```

### Custom Components

```python
from profiler import DefaultProfilerFactory
from profiler.processing_strategies import ProcessingStrategyFactory

factory = DefaultProfilerFactory(connector)
core_profiler = factory.create_profiler(config)

# Custom strategy usage
strategy = ProcessingStrategyFactory.create_processor(
    ProfilingStrategy.PARALLEL,
    core_profiler
)
```

This API reference provides comprehensive documentation for all public interfaces and components in the Schema Profiler system.
