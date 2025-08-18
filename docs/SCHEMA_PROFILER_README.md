# Comprehensive Schema Data Profiler

A powerful database schema profiling system designed to collect essential information for LLM processing. The profiler analyzes database schemas comprehensively without generating full database statistics, keeping the output concise and focused on structural information.

## Features

### **Table Profiling**

- Table metadata (name, type, comments)
- Row count estimates
- Table relationships and dependencies
- Index information

### **Column Profiling**

- Data types and constraints
- Nullability and default values
- Primary key and foreign key identification
- Unique constraints and indexes
- Column comments and metadata

### **Schema Profiling**

- Overall database structure
- Cross-table relationships
- Schema-level metadata
- Database type detection

### **Sample Data Extraction**

- 5 sample rows per table
- Representative data for pattern analysis
- Null-safe data extraction
- Formatted for LLM consumption

### **Field Pattern Recognition**

- Healthcare-specific patterns (from `field_patterns.json`)
- Regex-based pattern matching
- Field name pattern detection
- Compliance and PHI status identification

### **Structural Relationship Analysis**

- **Explicit Foreign Key Relationships**: Actual FK constraints
- **Primary Key Information**: PK columns and constraints
- **Potential Foreign Key Candidates**: Columns that could be FKs based on naming patterns
- **Self-Referencing Relationships**: Tables that reference themselves

## Architecture

### Core Classes

#### `ColumnProfile`

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
    detected_patterns: List[str] = None
    sample_values: List[Any] = None
    foreign_key_reference: Optional[Dict[str, str]] = None
```

#### `TableProfile`

```python
@dataclass
class TableProfile:
    name: str
    schema: Optional[str] = None
    table_type: str = "BASE TABLE"
    table_comment: Optional[str] = None
    estimated_row_count: int = 0
    columns: List[ColumnProfile] = None
    primary_keys: List[str] = None
    foreign_keys: List[Dict[str, str]] = None
    indexes: List[Dict[str, Any]] = None
    sample_data: List[Dict[str, Any]] = None
    self_referencing_columns: List[str] = None
    potential_fk_candidates: List[Dict[str, Any]] = None
```

#### `SchemaProfile`

```python
@dataclass
class SchemaProfile:
    database_name: str
    schema_name: Optional[str] = None
    database_type: str = "unknown"
    profiling_timestamp: str = None
    total_tables: int = 0
    total_columns: int = 0
    tables: List[TableProfile] = None
    cross_table_relationships: List[Dict[str, Any]] = None
    potential_relationships: List[Dict[str, Any]] = None
    pattern_summary: Dict[str, int] = None
```

### Supporting Classes

#### `DatabaseDialect`

- Handles database-specific SQL syntax
- Supports MySQL, PostgreSQL, and MSSQL
- Provides appropriate queries for each database type

#### `FieldPatternRecognizer`

- Loads patterns from `config/field_patterns.json`
- Matches field names and data patterns
- Supports healthcare-specific patterns
- Configurable pattern recognition thresholds

#### `SchemaDataProfiler`

- Main profiler class
- Orchestrates the profiling process
- Integrates all components
- Exports results for LLM processing

## Usage

### Basic Usage

```python
from connectors.config_loader import ConfigLoader
from profiler.schemaProfiler import SchemaDataProfiler

# Create database connector
config_loader = ConfigLoader()
connector = config_loader.create_connector_from_config('client_alpha')

# Create profiler
profiler = SchemaDataProfiler(
    connector=connector,
    database_name='alpha_db',
    schema_name='public'  # Optional
)

# Profile the schema
schema_profile = profiler.profile_schema()

# Export for LLM processing
profile_dict = profiler.export_profile(schema_profile, 'schema_profile.json')
```

### Advanced Usage

```python
# Profile specific aspects
table_profile = profiler._profile_table('users')
column_profiles = profiler._get_column_profiles('users')
sample_data = profiler._get_sample_data('users', limit=5)

# Analyze relationships
relationships = profiler._analyze_cross_table_relationships(schema_profile.tables)
potential_rels = profiler._find_potential_relationships(schema_profile.tables)

# Pattern recognition
pattern_recognizer = FieldPatternRecognizer()
patterns = pattern_recognizer.detect_patterns('npi_number', ['1234567890', '9876543210'])
```

## Database Support

### MySQL

- Uses `INFORMATION_SCHEMA` tables
- Supports character sets and collations
- Handles MySQL-specific column keys
- Backtick identifier quoting

### PostgreSQL

- Uses `information_schema` and `pg_*` system tables
- Supports schemas and namespaces
- Handles PostgreSQL-specific data types
- Double-quote identifier quoting

### MSSQL

- Uses `INFORMATION_SCHEMA` and `sys.*` tables
- Supports extended properties for comments
- Handles SQL Server-specific features
- Bracket identifier quoting

## Pattern Recognition

### Healthcare Patterns

The profiler recognizes healthcare-specific patterns from `field_patterns.json`:

- **Provider Identifiers**: NPI, DEA numbers, license numbers
- **Patient Identifiers**: MRN, SSN patterns
- **Clinical Codes**: ICD, CPT, SNOMED patterns
- **Contact Information**: Phone, email, address patterns
- **Demographics**: Gender, race, ethnicity patterns

### Pattern Detection Logic

1. **Field Name Matching**: Checks if column names match known field names
2. **Regex Pattern Matching**: Applies regex patterns to sample data
3. **Threshold-based Detection**: Requires 50%+ match rate for pattern confirmation
4. **Multi-pattern Support**: Columns can match multiple patterns

## Relationship Analysis

### Explicit Relationships

- Foreign key constraints from database metadata
- Primary key identification
- Unique constraints and indexes

### Potential Relationships

- Column naming pattern analysis
- Data type compatibility checking
- Cross-table reference suggestions

### Self-Referencing Detection

- Identifies tables that reference themselves
- Useful for hierarchical data structures
- Parent-child relationship identification

## Output Format

The profiler generates a comprehensive JSON structure suitable for LLM processing:

```json
{
  "database_name": "alpha_db",
  "database_type": "mysql",
  "profiling_timestamp": "2024-01-15T10:30:00",
  "total_tables": 15,
  "total_columns": 127,
  "tables": [
    {
      "name": "users",
      "columns": [
        {
          "name": "user_id",
          "data_type": "int",
          "is_primary_key": true,
          "sample_values": [1, 2, 3, 4, 5]
        }
      ],
      "sample_data": [
        { "user_id": 1, "name": "John Doe", "email": "john@example.com" }
      ],
      "foreign_keys": [],
      "self_referencing_columns": [],
      "potential_fk_candidates": []
    }
  ],
  "cross_table_relationships": [],
  "potential_relationships": [],
  "pattern_summary": {
    "provider_identifiers.npi": 3,
    "contact_information.email": 12
  }
}
```

## Configuration

### Pattern Configuration

Edit `config/field_patterns.json` to add custom patterns:

```json
{
  "healthcare_provider_patterns": {
    "pattern_categories": {
      "custom_category": {
        "patterns": {
          "custom_pattern": {
            "regex": "^CUSTOM\\d{4}$",
            "field_names": ["custom_id", "custom_code"],
            "description": "Custom pattern description"
          }
        }
      }
    }
  }
}
```

### Database Configuration

Use the existing `config/database_configs.json` for database connections.

## Performance Considerations

### Optimizations

- **Limited Sample Size**: Only 5 rows per table
- **Efficient Queries**: Uses database-specific optimized queries
- **Connection Pooling**: Leverages existing connector pooling
- **Lazy Loading**: Loads data only when needed

### Scalability

- **Large Databases**: Handles databases with hundreds of tables
- **Memory Efficient**: Processes tables one at a time
- **Configurable Limits**: Adjustable sample sizes and query limits

## Error Handling

### Robust Error Management

- Database connection failures
- Query execution errors
- Missing pattern files
- Invalid configuration handling

### Logging

- Comprehensive logging at multiple levels
- Error tracking and debugging support
- Performance monitoring capabilities

## Example Output

Run the example to see the profiler in action:

```bash
cd examples
python schema_profiling_example.py
```

This will:

1. Load database configurations
2. Profile each configured database
3. Display comprehensive schema information
4. Export JSON profiles for LLM processing
5. Show detected patterns and relationships

## Integration with LLM Systems

The profiler output is specifically designed for LLM consumption:

- **Structured Data**: Clean JSON format
- **Contextual Information**: Rich metadata and relationships
- **Pattern Recognition**: Pre-identified data patterns
- **Sample Data**: Representative examples
- **Relationship Mapping**: Clear table and column relationships

This enables LLMs to understand database schemas effectively for tasks like:

- Schema mapping and transformation
- Data migration planning
- Query generation
- Data quality assessment
- Compliance analysis
