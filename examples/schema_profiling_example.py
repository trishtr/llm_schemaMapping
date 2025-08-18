"""
Schema Profiling Example

This example demonstrates how to use the comprehensive SchemaDataProfiler
with the new modular architecture including:
- Separated data models (ColumnProfile, TableProfile, SchemaProfile)
- Enhanced pattern recognition (FieldPatternRecognizer)
- Database dialect support (DatabaseDialect)
- Advanced schema analysis for LLM processing
"""

import sys
import os
import json
import logging

# Add the src directory to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from connectors.config_loader import ConfigLoader
from profiler import (
    SchemaDataProfiler, 
    FieldPatternRecognizer, 
    DatabaseDialect,
    ColumnProfile,
    TableProfile,
    SchemaProfile
)


def setup_logging():
    """Setup logging configuration."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )


def demo_pattern_recognition():
    """Demonstrate the enhanced FieldPatternRecognizer capabilities."""
    print("\n" + "="*60)
    print("Pattern Recognition Demo")
    print("="*60)
    
    try:
        # Initialize pattern recognizer
        pattern_recognizer = FieldPatternRecognizer()
        
        # Get pattern statistics
        stats = pattern_recognizer.get_pattern_statistics()
        print(f"Pattern Recognizer Statistics:")
        print(f"  - Total patterns loaded: {stats['total_patterns']}")
        print(f"  - Categories: {stats['total_categories']}")
        print(f"  - Available categories: {', '.join(stats['categories'])}")
        print(f"  - Config path: {stats['config_path']}")
        
        # Demo pattern detection with sample data
        print(f"\nPattern Detection Examples:")
        
        # Example 1: NPI numbers
        npi_values = ["1234567890", "9876543210", "1111111111", "2222222222"]
        npi_patterns = pattern_recognizer.detect_patterns(npi_values, field_name="provider_npi")
        print(f"  NPI field patterns: {npi_patterns}")
        
        # Example 2: Email addresses (if pattern exists)
        email_values = ["john@example.com", "jane.doe@hospital.org", "admin@clinic.net"]
        email_patterns = pattern_recognizer.detect_patterns(email_values, field_name="email_address")
        print(f"  Email field patterns: {email_patterns}")
        
        # Example 3: Pattern detection with confidence scores
        if npi_patterns:
            confidence_results = pattern_recognizer.detect_patterns_with_confidence(
                npi_values, field_name="provider_npi"
            )
            print(f"  Pattern confidence analysis:")
            for result in confidence_results:
                print(f"    - {result['pattern_key']}: {result['confidence']:.1%} confidence")
                print(f"      Description: {result['description']}")
                print(f"      Examples: {result['examples'][:2]}")
        
        # Demo pattern validation
        print(f"\nPattern Validation Examples:")
        if npi_patterns:
            pattern_key = npi_patterns[0]
            test_values = ["1234567890", "invalid_npi", "12345"]
            for value in test_values:
                is_valid = pattern_recognizer.validate_value(value, pattern_key)
                print(f"  '{value}' matches {pattern_key}: {is_valid}")
        
        # Demo category exploration
        categories = pattern_recognizer.get_pattern_categories()
        print(f"\nAvailable Pattern Categories:")
        for category in categories:
            category_patterns = pattern_recognizer.get_patterns_by_category(category)
            print(f"  - {category}: {len(category_patterns)} patterns")
            
            # Show first pattern as example
            if category_patterns:
                first_pattern = list(category_patterns.keys())[0]
                pattern_info = pattern_recognizer.get_pattern_info(first_pattern)
                print(f"    Example: {first_pattern}")
                print(f"    Description: {pattern_info.get('description', 'N/A')}")
        
    except Exception as e:
        print(f"✗ Error in pattern recognition demo: {e}")


def demo_database_dialect():
    """Demonstrate DatabaseDialect capabilities."""
    print("\n" + "="*60)
    print("Database Dialect Demo")
    print("="*60)
    
    try:
        # Test different database dialects
        db_types = ['mysql', 'postgresql', 'mssql']
        
        for db_type in db_types:
            print(f"\n{db_type.upper()} Dialect:")
            dialect = DatabaseDialect(db_type)
            
            # Show sample queries
            print(f"  - Sample query: {dialect.get_sample_query('*', 'users', 5)}")
            print(f"  - Count query: {dialect.get_count_query('users')}")
            print(f"  - Quoted identifier: {dialect.quote_identifier('table_name')}")
            print(f"  - Supported: {dialect.is_supported(db_type)}")
        
        # Show supported databases
        dialect = DatabaseDialect('mysql')
        supported = dialect.get_supported_databases()
        print(f"\nSupported Database Types: {supported}")
        
    except Exception as e:
        print(f"✗ Error in database dialect demo: {e}")


def demo_data_models():
    """Demonstrate the enhanced data models."""
    print("\n" + "="*60)
    print("Data Models Demo")
    print("="*60)
    
    try:
        # Create sample column
        column = ColumnProfile(
            name="user_id",
            data_type="int",
            is_nullable=False,
            is_primary_key=True,
            is_indexed=True,
            detected_patterns=["provider_identifiers.npi"],
            sample_values=[1, 2, 3, 4, 5]
        )
        
        print(f"Column Model Features:")
        print(f"  - Name: {column.name}")
        print(f"  - Is key column: {column.is_key_column()}")
        print(f"  - Constraints: {column.get_constraints()}")
        print(f"  - Has NPI pattern: {column.has_pattern('provider_identifiers.npi')}")
        
        # Create sample table
        table = TableProfile(
            name="users",
            schema="public",
            columns=[column],
            primary_keys=["user_id"],
            estimated_row_count=1000
        )
        
        print(f"\nTable Model Features:")
        print(f"  - Full name: {table.get_full_name()}")
        print(f"  - Column count: {table.get_column_count()}")
        print(f"  - Primary key columns: {[col.name for col in table.get_primary_key_columns()]}")
        print(f"  - Relationships summary: {table.get_relationships_summary()}")
        
        # Create sample schema
        schema = SchemaProfile(
            database_name="test_db",
            database_type="postgresql",
            tables=[table],
            total_tables=1,
            total_columns=1,
            pattern_summary={"provider_identifiers.npi": 1}
        )
        
        print(f"\nSchema Model Features:")
        print(f"  - Table names: {schema.get_table_names()}")
        print(f"  - Pattern statistics: {schema.get_pattern_statistics()}")
        print(f"  - Schema summary: {schema.get_schema_summary()}")
        
    except Exception as e:
        print(f"✗ Error in data models demo: {e}")


def main():
    """Main function demonstrating schema profiling with new modular architecture."""
    print("Schema Data Profiling Example - Enhanced Modular Architecture")
    print("=" * 70)
    
    setup_logging()
    
    try:
        # Demo the new modular components
        demo_pattern_recognition()
        demo_database_dialect()
        demo_data_models()
        # 1. Create config loader and get connectors
        print("\n1. Loading database configurations...")
        config_loader = ConfigLoader()
        
        if not config_loader.validate_config():
            print("✗ Configuration validation failed")
            return
        
        clients = config_loader.list_available_clients()
        print(f"Available clients: {clients}")
        
        # 2. Profile each database
        for client in clients[:2]:  # Limit to first 2 clients for demo
            print(f"\n{'='*60}")
            print(f"Profiling Database: {client}")
            print(f"{'='*60}")
            
            try:
                # Get client info
                client_info = config_loader.get_client_info(client)
                print(f"Database Type: {client_info['db_type']}")
                print(f"Database Name: {client_info['database']}")
                print(f"Host: {client_info['host']}:{client_info['port']}")
                
                # Create connector
                connector = config_loader.create_connector_from_config(client)
                
                # Create profiler
                profiler = SchemaDataProfiler(
                    connector=connector,
                    database_name=client_info['database'],
                    schema_name=client_info.get('schema')
                )
                
                print(f"\n2. Starting comprehensive schema profiling...")
                
                # Profile the schema using adaptive processing (automatically chooses parallel for large schemas)
                schema_profile = profiler.profile_schema_adaptive(parallel_threshold=5, max_workers=4)
                
                # Alternative options:
                # schema_profile = profiler.profile_schema()  # Sequential processing
                # schema_profile = profiler.profile_schema_parallel(max_workers=8)  # Force parallel processing
                
                print(f"\n3. Profiling Results:")
                print(f"   - Database: {schema_profile.database_name}")
                print(f"   - Database Type: {schema_profile.database_type}")
                print(f"   - Schema: {schema_profile.schema_name or 'default'}")
                print(f"   - Total Tables: {schema_profile.total_tables}")
                print(f"   - Total Columns: {schema_profile.total_columns}")
                print(f"   - Cross-table Relationships: {len(schema_profile.cross_table_relationships)}")
                print(f"   - Potential Relationships: {len(schema_profile.potential_relationships)}")
                
                # Display table details
                print(f"\n4. Table Details:")
                for table in schema_profile.tables[:3]:  # Show first 3 tables
                    print(f"\n   Table: {table.name}")
                    print(f"   - Columns: {len(table.columns)}")
                    print(f"   - Primary Keys: {table.primary_keys}")
                    print(f"   - Foreign Keys: {len(table.foreign_keys)}")
                    print(f"   - Indexes: {len(table.indexes)}")
                    print(f"   - Estimated Rows: {table.estimated_row_count}")
                    print(f"   - Self-referencing: {table.self_referencing_columns}")
                    print(f"   - Potential FK Candidates: {len(table.potential_fk_candidates)}")
                    
                    # Show sample columns
                    print(f"   - Sample Columns:")
                    for col in table.columns[:5]:  # Show first 5 columns
                        flags = []
                        if col.is_primary_key: flags.append("PK")
                        if col.is_foreign_key: flags.append("FK")
                        if col.is_unique: flags.append("UNIQUE")
                        if col.is_indexed: flags.append("INDEXED")
                        
                        flag_str = f" [{', '.join(flags)}]" if flags else ""
                        print(f"     • {col.name} ({col.data_type}){flag_str}")
                        
                        if col.detected_patterns:
                            print(f"       Patterns: {', '.join(col.detected_patterns)}")
                        
                        if col.sample_values:
                            sample_str = ', '.join([str(v)[:20] for v in col.sample_values[:3] if v is not None])
                            print(f"       Sample: {sample_str}")
                    
                    # Show sample data
                    if table.sample_data:
                        print(f"   - Sample Data (first 2 rows):")
                        for i, row in enumerate(table.sample_data[:2]):
                            print(f"     Row {i+1}: {dict(list(row.items())[:3])}...")  # Show first 3 columns
                
                # Display relationships
                if schema_profile.cross_table_relationships:
                    print(f"\n5. Explicit Foreign Key Relationships:")
                    for rel in schema_profile.cross_table_relationships[:5]:  # Show first 5
                        print(f"   {rel['from_table']}.{rel['from_column']} -> {rel['to_table']}.{rel['to_column']}")
                
                if schema_profile.potential_relationships:
                    print(f"\n6. Potential Relationships:")
                    for rel in schema_profile.potential_relationships[:5]:  # Show first 5
                        print(f"   {rel['from_table']}.{rel['from_column']} -> {rel['to_table']}.{rel['to_column']} ({rel['reason']})")
                
                # Display pattern summary
                if schema_profile.pattern_summary:
                    print(f"\n7. Detected Patterns Summary:")
                    for pattern, count in list(schema_profile.pattern_summary.items())[:10]:
                        print(f"   {pattern}: {count} occurrences")
                
                # Export profile for LLM processing
                output_file = f"schema_profile_{client}.json"
                profile_dict = profiler.export_profile(schema_profile, output_file)
                print(f"\n8. Profile exported to: {output_file}")
                print(f"   Profile size: {len(json.dumps(profile_dict))} characters")
                
                # Disconnect
                connector.disconnect()
                print(f"✓ Profiling completed for {client}")
                
            except Exception as e:
                print(f"✗ Error profiling {client}: {e}")
                import traceback
                traceback.print_exc()
        
        print(f"\n{'='*60}")
        print("Schema Profiling Example Completed!")
        print(f"{'='*60}")
        
        print(f"\nKey Features Demonstrated:")
        print(f"✓ Enhanced Modular Architecture:")
        print(f"  - Separated data models (ColumnProfile, TableProfile, SchemaProfile)")
        print(f"  - Advanced pattern recognition (FieldPatternRecognizer)")
        print(f"  - Database dialect support (DatabaseDialect)")
        print(f"✓ Table profiling (metadata, constraints, relationships)")
        print(f"✓ Column profiling (types, patterns, sample data)")
        print(f"✓ Schema profiling (overall structure)")
        print(f"✓ Sample data extraction (5 rows per table)")
        print(f"✓ Enhanced field pattern recognition:")
        print(f"  - Pattern detection with confidence scores")
        print(f"  - Field name-based pattern matching")
        print(f"  - Pattern validation and category exploration")
        print(f"✓ Structural relationship analysis:")
        print(f"  - Explicit foreign key relationships")
        print(f"  - Primary key information")
        print(f"  - Potential foreign key candidates")
        print(f"  - Self-referencing relationships")
        print(f"✓ Rich data models with utility methods")
        print(f"✓ Parallel processing for large schemas")
        print(f"✓ Export for LLM processing")
        
    except Exception as e:
        print(f"✗ Error running schema profiling example: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main() 