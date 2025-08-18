"""
Test Database Dialect Functionality

This script tests the DatabaseDialect class to ensure it works correctly
after being separated into its own module.
"""

import sys
import os

# Add the src directory to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from profiler import DatabaseDialect


def test_database_dialect():
    """Test the DatabaseDialect class functionality."""
    print("Testing Database Dialect Functionality")
    print("=" * 50)
    
    # Test all supported database types
    db_types = ['mysql', 'postgresql', 'mssql']
    
    for db_type in db_types:
        print(f"\nTesting {db_type.upper()} dialect:")
        
        try:
            # Create dialect instance
            dialect = DatabaseDialect(db_type)
            
            # Test basic properties
            print(f"  ✓ Database type: {dialect.db_type}")
            print(f"  ✓ Supported: {dialect.is_supported(db_type)}")
            
            # Test identifier quoting
            table_name = "test_table"
            quoted = dialect.quote_identifier(table_name)
            print(f"  ✓ Quoted identifier: {quoted}")
            
            # Test sample query generation
            sample_query = dialect.get_sample_query("*", quoted, 5)
            print(f"  ✓ Sample query: {sample_query}")
            
            # Test count query generation
            count_query = dialect.get_count_query(quoted)
            print(f"  ✓ Count query: {count_query}")
            
            # Test query method availability
            queries = [
                'get_tables_query',
                'get_column_info_query',
                'get_foreign_keys_query',
                'get_indexes_query',
                'get_primary_keys_query'
            ]
            
            for query_method in queries:
                query = getattr(dialect, query_method)()
                print(f"  ✓ {query_method}: Available")
            
        except Exception as e:
            print(f"  ✗ Error testing {db_type}: {e}")
    
    # Test utility methods
    print(f"\nTesting utility methods:")
    dialect = DatabaseDialect('mysql')
    
    supported_dbs = dialect.get_supported_databases()
    print(f"  ✓ Supported databases: {supported_dbs}")
    
    print(f"  ✓ MySQL supported: {dialect.is_supported('mysql')}")
    print(f"  ✓ Oracle supported: {dialect.is_supported('oracle')}")
    
    # Test case insensitivity
    dialect_upper = DatabaseDialect('MYSQL')
    print(f"  ✓ Case insensitive: {dialect_upper.db_type}")
    
    # Test fallback to PostgreSQL for unknown database
    dialect_unknown = DatabaseDialect('unknown_db')
    print(f"  ✓ Unknown DB fallback: {dialect_unknown.db_type}")
    
    print(f"\n{'='*50}")
    print("Database Dialect Testing Completed!")
    print(f"{'='*50}")


if __name__ == "__main__":
    test_database_dialect() 