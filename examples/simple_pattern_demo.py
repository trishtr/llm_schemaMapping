#!/usr/bin/env python3
"""
Simple Pattern Recognition Demo

This script demonstrates the simplified pattern recognizer that focuses
only on obvious, easily detectable patterns to eliminate complexity.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from profiler.simple_pattern_recognizer import SimplePatternRecognizer
from profiler.schema_models import ColumnProfile, TableProfile, SchemaProfile
from dataclasses import asdict
import json


def create_simple_test_data():
    """Create test data with obvious patterns and relationship examples."""
    
    # Add a provider_id column with foreign key reference
    provider_id_column = ColumnProfile(
        name="provider_id",
        data_type="int",
        is_nullable=True,
        is_foreign_key=True,
        is_indexed=True,
        sample_values=[1001, 1002, 1003, 1001, 1004],
        detected_patterns=[],
        foreign_key_reference={
            "referenced_table": "providers",
            "referenced_column": "provider_id",
            "constraint_name": "fk_healthcare_provider"
        }
    )
    
    # Add a department_id column as potential FK candidate
    department_id_column = ColumnProfile(
        name="department_id",
        data_type="int",
        is_nullable=True,
        is_indexed=True,
        sample_values=[101, 102, 101, 103, 102],
        detected_patterns=[]
    )
    
    # Healthcare table with clear patterns
    columns = [
        ColumnProfile(
            name="patient_id",
            data_type="bigint",
            is_nullable=False,
            is_primary_key=True,
            sample_values=[100001, 100002, 100003, 100004, 100005],
            detected_patterns=[]
        ),
        ColumnProfile(
            name="npi",
            data_type="varchar",
            max_length=10,
            is_nullable=False,
            is_unique=True,
            sample_values=["1234567890", "9876543210", "5555666677", "1111222233", "9999888877"],
            detected_patterns=[]
        ),
        ColumnProfile(
            name="email_address", 
            data_type="varchar",
            max_length=255,
            is_nullable=True,
            sample_values=["john@example.com", "jane@test.org", "bob@clinic.net", "alice@hospital.edu", "charlie@medical.com"],
            detected_patterns=[]
        ),
        ColumnProfile(
            name="phone_number",
            data_type="varchar", 
            max_length=20,
            is_nullable=True,
            sample_values=["555-123-4567", "555-987-6543", "555-456-7890", "555-234-5678", "555-345-6789"],
            detected_patterns=[]
        ),
        ColumnProfile(
            name="status",
            data_type="varchar",
            max_length=20,
            is_nullable=False,
            sample_values=["active", "inactive", "pending", "active", "completed"],
            detected_patterns=[]
        ),
        ColumnProfile(
            name="first_name",
            data_type="varchar",
            max_length=50,
            is_nullable=False,
            sample_values=["John", "Jane", "Bob", "Alice", "Charlie"],
            detected_patterns=[]
        ),
        ColumnProfile(
            name="created_at",
            data_type="timestamp",
            is_nullable=False,
            sample_values=["2024-01-15 10:30:00", "2024-01-16 14:22:15", "2024-01-17 09:45:30", "2024-01-18 16:12:45", "2024-01-19 11:55:20"],
            detected_patterns=[]
        )
    ]
    
    table = TableProfile(
        name="healthcare_records",
        schema="public",
        table_type="BASE TABLE",
        table_comment="Healthcare records with obvious patterns",
        estimated_row_count=50000,
        total_columns=len(columns),
        columns=columns
    )
    
    schema = SchemaProfile(
        database_name="simple_healthcare_db",
        schema_name="public",
        database_type="postgresql", 
        profiling_timestamp="2025-08-21T16:00:00",
        total_tables=1,
        total_columns=7,
        tables=[table]
    )
    
    return schema


def demonstrate_simple_patterns():
    """Demonstrate simplified pattern recognition."""
    
    print("=" * 80)
    print("SIMPLIFIED PATTERN RECOGNITION DEMO")
    print("=" * 80)
    print()
    
    # Initialize simple pattern recognizer
    recognizer = SimplePatternRecognizer()
    
    print("🔧 SIMPLE PATTERN RECOGNIZER:")
    print(f"   Loaded patterns: {len(recognizer.patterns)}")
    print(f"   Detection approach: Obvious patterns only")
    print(f"   Min match ratio: {recognizer.min_match_ratio}")
    print(f"   Min sample size: {recognizer.min_sample_size}")
    print()
    
    # Create test data
    schema = create_simple_test_data()
    table = schema.tables[0]
    
    print(f"📋 TABLE: {table.name}")
    print(f"   Comment: {table.table_comment}")
    print()
    
    # Process each column
    for column in table.columns:
        print(f"   📝 COLUMN: {column.name} ({column.data_type})")
        
        # Detect patterns using simple recognizer
        detected = recognizer.detect_patterns(column.sample_values, field_name=column.name)
        
        if detected:
            print(f"      ✅ DETECTED PATTERNS: {', '.join(detected)}")
            column.detected_patterns = detected
            
            # Show pattern details
            for pattern in detected:
                pattern_info = recognizer.get_pattern_info(pattern)
                if pattern_info:
                    print(f"         - {pattern}:")
                    if 'regex' in pattern_info:
                        print(f"           Validation: Regex pattern")
                    elif 'valid_values' in pattern_info:
                        print(f"           Validation: Valid values {pattern_info['valid_values'][:3]}...")
                    else:
                        print(f"           Validation: Field name + pattern matching")
                    
                    if 'field_names' in pattern_info:
                        print(f"           Expected fields: {', '.join(pattern_info['field_names'][:3])}...")
        else:
            print(f"      ❌ NO OBVIOUS PATTERNS DETECTED")
            column.detected_patterns = []
        
        print(f"      📋 Sample Values: {', '.join(str(v) for v in column.sample_values[:3])}...")
        print()
    
    # Export simple schema profile
    output_file = "simple_schema_profile.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(asdict(schema), f, indent=2, default=str)
    
    file_size = Path(output_file).stat().st_size
    print(f"💾 SIMPLE SCHEMA PROFILE EXPORTED:")
    print(f"   File: {output_file}")
    print(f"   Size: {file_size:,} bytes")
    print()
    
    # Show detection summary
    total_patterns = sum(len(col.detected_patterns) for col in table.columns)
    columns_with_patterns = sum(1 for col in table.columns if col.detected_patterns)
    
    print("📊 DETECTION SUMMARY:")
    print(f"   Total columns: {len(table.columns)}")
    print(f"   Columns with obvious patterns: {columns_with_patterns}")
    print(f"   Total patterns detected: {total_patterns}")
    print(f"   Detection rate: {columns_with_patterns/len(table.columns)*100:.1f}%")
    print()
    
    # Show benefits of simplification
    print("🎯 BENEFITS OF SIMPLIFIED APPROACH:")
    print("   ✅ No complex confidence scoring - patterns are obvious or not")
    print("   ✅ No priority conflicts - clear pattern hierarchy")
    print("   ✅ No context complexity - field name + data validation")
    print("   ✅ Fast detection - minimal processing overhead")
    print("   ✅ Predictable results - same input always gives same output")
    print("   ✅ Easy to understand - transparent detection logic")
    print("   ✅ Fewer false positives - high confidence threshold")
    print()
    
    print("=" * 80)
    print("SIMPLIFIED PATTERN RECOGNITION DEMO COMPLETED")
    print("=" * 80)


if __name__ == "__main__":
    demonstrate_simple_patterns() 