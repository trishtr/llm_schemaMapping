"""
Schema Models Demo

This example demonstrates the functionality of the separated schema data models
and their utility methods for analyzing database schema information.
"""

import sys
import os
from datetime import datetime

# Add the src directory to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from profiler import ColumnProfile, TableProfile, SchemaProfile


def demo_column_profile():
    """Demonstrate ColumnProfile functionality."""
    print("Column Profile Demo")
    print("-" * 30)
    
    # Create sample columns
    id_column = ColumnProfile(
        name="user_id",
        data_type="int",
        is_nullable=False,
        is_primary_key=True,
        is_indexed=True,
        sample_values=[1, 2, 3, 4, 5],
        detected_patterns=["provider_identifiers.npi"]
    )
    
    email_column = ColumnProfile(
        name="email",
        data_type="varchar",
        is_nullable=True,
        max_length=255,
        is_unique=True,
        sample_values=["john@example.com", "jane@example.com", "bob@test.com"],
        detected_patterns=["contact_information.email"]
    )
    
    # Demonstrate utility methods
    print(f"ID Column:")
    print(f"  Name: {id_column.name}")
    print(f"  Is Key Column: {id_column.is_key_column()}")
    print(f"  Constraints: {id_column.get_constraints()}")
    print(f"  Has NPI Pattern: {id_column.has_pattern('provider_identifiers.npi')}")
    
    print(f"\nEmail Column:")
    print(f"  Name: {email_column.name}")
    print(f"  Is Key Column: {email_column.is_key_column()}")
    print(f"  Constraints: {email_column.get_constraints()}")
    print(f"  Has Email Pattern: {email_column.has_pattern('contact_information.email')}")
    
    return [id_column, email_column]


def demo_table_profile():
    """Demonstrate TableProfile functionality."""
    print("\nTable Profile Demo")
    print("-" * 30)
    
    # Get columns from previous demo
    columns = demo_column_profile()
    
    # Create sample table
    users_table = TableProfile(
        name="users",
        schema="public",
        table_comment="User information table",
        estimated_row_count=10000,
        columns=columns,
        primary_keys=["user_id"],
        foreign_keys=[
            {
                "column_name": "department_id",
                "referenced_table": "departments",
                "referenced_column": "id",
                "constraint_name": "fk_users_department"
            }
        ],
        sample_data=[
            {"user_id": 1, "email": "john@example.com"},
            {"user_id": 2, "email": "jane@example.com"},
            {"user_id": 3, "email": "bob@test.com"}
        ]
    )
    
    # Demonstrate utility methods
    print(f"\nTable: {users_table.name}")
    print(f"  Full Name: {users_table.get_full_name()}")
    print(f"  Column Count: {users_table.get_column_count()}")
    print(f"  Has Foreign Keys: {users_table.has_foreign_keys()}")
    print(f"  Has Self References: {users_table.has_self_references()}")
    
    # Get specific column types
    pk_columns = users_table.get_primary_key_columns()
    print(f"  Primary Key Columns: {[col.name for col in pk_columns]}")
    
    indexed_columns = users_table.get_indexed_columns()
    print(f"  Indexed Columns: {[col.name for col in indexed_columns]}")
    
    # Get relationships summary
    rel_summary = users_table.get_relationships_summary()
    print(f"  Relationships Summary: {rel_summary}")
    
    # Test column lookup
    email_col = users_table.get_column_by_name("email")
    if email_col:
        print(f"  Found column 'email': {email_col.data_type}")
    
    return users_table


def demo_schema_profile():
    """Demonstrate SchemaProfile functionality."""
    print("\nSchema Profile Demo")
    print("-" * 30)
    
    # Get table from previous demo
    users_table = demo_table_profile()
    
    # Create additional sample table
    departments_table = TableProfile(
        name="departments",
        schema="public",
        columns=[
            ColumnProfile("id", "int", False, is_primary_key=True),
            ColumnProfile("name", "varchar", False, max_length=100),
            ColumnProfile("manager_id", "int", True, is_foreign_key=True)
        ],
        primary_keys=["id"],
        self_referencing_columns=["manager_id"],  # Self-referencing
        estimated_row_count=50
    )
    
    # Create schema profile
    schema = SchemaProfile(
        database_name="company_db",
        schema_name="public",
        database_type="postgresql",
        total_tables=2,
        total_columns=5,
        tables=[users_table, departments_table],
        cross_table_relationships=[
            {
                "type": "foreign_key",
                "from_table": "users",
                "from_column": "department_id",
                "to_table": "departments",
                "to_column": "id"
            }
        ],
        potential_relationships=[
            {
                "type": "potential_foreign_key",
                "from_table": "users",
                "from_column": "created_by_user_id",
                "to_table": "users",
                "to_column": "user_id",
                "confidence": "high"
            }
        ],
        pattern_summary={
            "provider_identifiers.npi": 1,
            "contact_information.email": 1
        }
    )
    
    # Demonstrate utility methods
    print(f"\nSchema: {schema.database_name}")
    print(f"  Database Type: {schema.database_type}")
    print(f"  Table Names: {schema.get_table_names()}")
    print(f"  Relationship Count: {schema.get_relationship_count()}")
    print(f"  Potential Relationships: {schema.get_potential_relationship_count()}")
    print(f"  Has Relationships: {schema.has_relationships()}")
    
    # Get filtered tables
    base_tables = schema.get_tables_by_type("BASE TABLE")
    print(f"  Base Tables: {[t.name for t in base_tables]}")
    
    fk_tables = schema.get_tables_with_foreign_keys()
    print(f"  Tables with Foreign Keys: {[t.name for t in fk_tables]}")
    
    self_ref_tables = schema.get_self_referencing_tables()
    print(f"  Self-Referencing Tables: {[t.name for t in self_ref_tables]}")
    
    # Get tables by column count
    small_tables = schema.get_tables_by_column_count(max_columns=3)
    print(f"  Small Tables (≤3 columns): {[t.name for t in small_tables]}")
    
    # Get pattern statistics
    pattern_stats = schema.get_pattern_statistics()
    print(f"  Pattern Statistics: {pattern_stats}")
    
    # Get comprehensive summary
    summary = schema.get_schema_summary()
    print(f"\n  Schema Summary:")
    print(f"    Database Info: {summary['database_info']}")
    print(f"    Structure: {summary['structure']}")
    print(f"    Relationships: {summary['relationships']}")
    print(f"    Patterns: {summary['patterns']}")
    
    return schema


def main():
    """Run all demos."""
    print("Schema Data Models Demonstration")
    print("=" * 50)
    
    try:
        # Run individual demos
        schema = demo_schema_profile()
        
        print(f"\n{'='*50}")
        print("Schema Models Demo Completed!")
        print(f"{'='*50}")
        
        print(f"\nKey Features Demonstrated:")
        print(f"✓ ColumnProfile with constraint checking and pattern detection")
        print(f"✓ TableProfile with relationship analysis and column lookup")
        print(f"✓ SchemaProfile with comprehensive schema analysis")
        print(f"✓ Utility methods for filtering and analyzing schema data")
        print(f"✓ Rich data models suitable for LLM processing")
        print(f"✓ Clean separation of data models from business logic")
        
    except Exception as e:
        print(f"✗ Error running demo: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main() 