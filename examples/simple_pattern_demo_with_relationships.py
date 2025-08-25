#!/usr/bin/env python3
"""
Simple Pattern Recognition Demo with Relationship Examples

This script demonstrates the simplified pattern recognizer with comprehensive
relationship examples including foreign keys, cross-table relationships,
potential relationships, and potential FK candidates.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from profiler.simple_pattern_recognizer import SimplePatternRecognizer
from profiler.schema_models import ColumnProfile, TableProfile, SchemaProfile
from dataclasses import asdict
import json


def create_comprehensive_test_data():
    """Create test data with obvious patterns and comprehensive relationship examples."""
    
    # Create multiple tables with relationships
    
    # 1. PATIENTS TABLE
    patient_columns = [
        ColumnProfile(
            name="patient_id",
            data_type="bigint",
            is_nullable=False,
            is_primary_key=True,
            sample_values=[100001, 100002, 100003, 100004, 100005],
            detected_patterns=[]
        ),
        ColumnProfile(
            name="primary_provider_id",
            data_type="int",
            is_nullable=True,
            is_foreign_key=True,
            is_indexed=True,
            sample_values=[1001, 1002, 1003, 1001, 1004],
            detected_patterns=[],
            foreign_key_reference={
                "referenced_table": "providers",
                "referenced_column": "provider_id",
                "constraint_name": "fk_patients_provider"
            }
        ),
        ColumnProfile(
            name="department_id",
            data_type="int",
            is_nullable=True,
            is_indexed=True,
            sample_values=[101, 102, 101, 103, 102],
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
        )
    ]
    
    patients_table = TableProfile(
        name="patients",
        schema="public",
        table_type="BASE TABLE",
        table_comment="Patient records with provider relationships",
        estimated_row_count=50000,
        total_columns=len(patient_columns),
        columns=patient_columns,
        primary_keys=["patient_id"],
        foreign_keys=[
            {
                "column_name": "primary_provider_id",
                "referenced_table": "providers",
                "referenced_column": "provider_id",
                "constraint_name": "fk_patients_provider"
            }
        ],
        indexes=[
            {"index_name": "idx_patients_provider", "column_name": "primary_provider_id", "is_unique": False},
            {"index_name": "idx_patients_department", "column_name": "department_id", "is_unique": False}
        ],
        potential_fk_candidates=[
            {
                "column_name": "department_id",
                "potential_referenced_table": "departments",
                "potential_referenced_column": "dept_id",
                "confidence": "high",
                "reason": "Column name pattern and data type match"
            }
        ]
    )
    
    # 2. PROVIDERS TABLE
    provider_columns = [
        ColumnProfile(
            name="provider_id",
            data_type="int",
            is_nullable=False,
            is_primary_key=True,
            sample_values=[1001, 1002, 1003, 1004, 1005],
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
            name="department_id",
            data_type="int",
            is_nullable=True,
            is_foreign_key=True,
            is_indexed=True,
            sample_values=[101, 102, 103, 101, 104],
            detected_patterns=[],
            foreign_key_reference={
                "referenced_table": "departments",
                "referenced_column": "dept_id",
                "constraint_name": "fk_providers_department"
            }
        ),
        ColumnProfile(
            name="supervisor_id",
            data_type="int",
            is_nullable=True,
            is_foreign_key=True,
            is_indexed=True,
            sample_values=[1001, 1002, None, 1001, 1002],
            detected_patterns=[],
            foreign_key_reference={
                "referenced_table": "providers",
                "referenced_column": "provider_id",
                "constraint_name": "fk_providers_supervisor"
            }
        )
    ]
    
    providers_table = TableProfile(
        name="providers",
        schema="public",
        table_type="BASE TABLE",
        table_comment="Healthcare providers with NPI identifiers",
        estimated_row_count=1500,
        total_columns=len(provider_columns),
        columns=provider_columns,
        primary_keys=["provider_id"],
        foreign_keys=[
            {
                "column_name": "department_id",
                "referenced_table": "departments",
                "referenced_column": "dept_id",
                "constraint_name": "fk_providers_department"
            },
            {
                "column_name": "supervisor_id",
                "referenced_table": "providers",
                "referenced_column": "provider_id",
                "constraint_name": "fk_providers_supervisor"
            }
        ],
        indexes=[
            {"index_name": "idx_providers_npi", "column_name": "npi", "is_unique": True},
            {"index_name": "idx_providers_department", "column_name": "department_id", "is_unique": False},
            {"index_name": "idx_providers_supervisor", "column_name": "supervisor_id", "is_unique": False}
        ],
        self_referencing_columns=["supervisor_id"]
    )
    
    # 3. DEPARTMENTS TABLE
    department_columns = [
        ColumnProfile(
            name="dept_id",
            data_type="int",
            is_nullable=False,
            is_primary_key=True,
            sample_values=[101, 102, 103, 104, 105],
            detected_patterns=[]
        ),
        ColumnProfile(
            name="dept_name",
            data_type="varchar",
            max_length=100,
            is_nullable=False,
            sample_values=["Cardiology", "Neurology", "Emergency", "Radiology", "Pediatrics"],
            detected_patterns=[]
        ),
        ColumnProfile(
            name="manager_id",
            data_type="int",
            is_nullable=True,
            is_indexed=True,
            sample_values=[1001, 1003, 1005, 1002, 1004],
            detected_patterns=[]
        )
    ]
    
    departments_table = TableProfile(
        name="departments",
        schema="public",
        table_type="BASE TABLE",
        table_comment="Hospital departments",
        estimated_row_count=25,
        total_columns=len(department_columns),
        columns=department_columns,
        primary_keys=["dept_id"],
        foreign_keys=[],
        indexes=[
            {"index_name": "idx_departments_manager", "column_name": "manager_id", "is_unique": False}
        ],
        potential_fk_candidates=[
            {
                "column_name": "manager_id",
                "potential_referenced_table": "providers",
                "potential_referenced_column": "provider_id",
                "confidence": "high",
                "reason": "Column name pattern suggests provider relationship"
            }
        ]
    )
    
    # Create comprehensive schema profile
    schema_profile = SchemaProfile(
        database_name="comprehensive_healthcare_db",
        schema_name="public",
        database_type="postgresql", 
        profiling_timestamp="2025-08-21T16:30:00",
        total_tables=3,
        total_columns=13,
        tables=[patients_table, providers_table, departments_table],
        cross_table_relationships=[
            {
                "type": "foreign_key",
                "from_table": "patients",
                "from_column": "primary_provider_id",
                "to_table": "providers",
                "to_column": "provider_id",
                "constraint_name": "fk_patients_provider"
            },
            {
                "type": "foreign_key",
                "from_table": "providers",
                "from_column": "department_id",
                "to_table": "departments",
                "to_column": "dept_id",
                "constraint_name": "fk_providers_department"
            },
            {
                "type": "self_reference",
                "from_table": "providers",
                "from_column": "supervisor_id",
                "to_table": "providers",
                "to_column": "provider_id",
                "constraint_name": "fk_providers_supervisor"
            }
        ],
        potential_relationships=[
            {
                "type": "potential_foreign_key",
                "from_table": "patients",
                "from_column": "department_id",
                "to_table": "departments",
                "to_column": "dept_id",
                "confidence": "high",
                "reason": "Column name pattern and data type match"
            },
            {
                "type": "potential_foreign_key",
                "from_table": "departments",
                "from_column": "manager_id",
                "to_table": "providers",
                "to_column": "provider_id",
                "confidence": "high",
                "reason": "Column name pattern suggests provider relationship"
            }
        ],
        pattern_summary={
            "npi_identifier": 1,
            "email_address": 1,
            "phone_number": 1,
            "status_field": 1
        }
    )
    
    return schema_profile


def demonstrate_comprehensive_patterns():
    """Demonstrate comprehensive pattern recognition with relationships."""
    
    print("=" * 80)
    print("COMPREHENSIVE PATTERN RECOGNITION WITH RELATIONSHIPS DEMO")
    print("=" * 80)
    print()
    
    # Initialize simple pattern recognizer
    recognizer = SimplePatternRecognizer()
    
    print("🔧 SIMPLE PATTERN RECOGNIZER:")
    print(f"   Loaded patterns: {len(recognizer.patterns)}")
    print(f"   Detection approach: Obvious patterns only")
    print()
    
    # Create comprehensive test data
    schema_profile = create_comprehensive_test_data()
    
    print(f"📊 COMPREHENSIVE SCHEMA: {schema_profile.database_name}")
    print(f"   Total Tables: {schema_profile.total_tables}")
    print(f"   Total Columns: {schema_profile.total_columns}")
    print(f"   Cross-table Relationships: {len(schema_profile.cross_table_relationships)}")
    print(f"   Potential Relationships: {len(schema_profile.potential_relationships)}")
    print()
    
    # Process each table and detect patterns
    for table in schema_profile.tables:
        print(f"📋 TABLE: {table.name}")
        print(f"   Comment: {table.table_comment}")
        print(f"   Columns: {table.total_columns}")
        print(f"   Foreign Keys: {len(table.foreign_keys)}")
        print(f"   Potential FK Candidates: {len(table.potential_fk_candidates)}")
        print(f"   Self-referencing: {len(table.self_referencing_columns) > 0}")
        print()
        
        # Process each column for pattern detection
        for column in table.columns:
            # Detect patterns
            detected = recognizer.detect_patterns(column.sample_values, field_name=column.name)
            column.detected_patterns = detected
            
            print(f"   📝 COLUMN: {column.name} ({column.data_type})")
            
            if column.is_foreign_key and column.foreign_key_reference:
                fk_ref = column.foreign_key_reference
                print(f"      🔗 FOREIGN KEY → {fk_ref['referenced_table']}.{fk_ref['referenced_column']}")
                print(f"         Constraint: {fk_ref['constraint_name']}")
            
            if detected:
                print(f"      ✅ PATTERNS: {', '.join(detected)}")
            else:
                print(f"      ❌ NO OBVIOUS PATTERNS")
            
            print(f"      📋 Sample: {', '.join(str(v) for v in column.sample_values[:3])}...")
            print()
        
        print()
    
    # Show relationship analysis
    print("🔗 CROSS-TABLE RELATIONSHIPS:")
    for rel in schema_profile.cross_table_relationships:
        rel_type = rel['type'].replace('_', ' ').title()
        print(f"   {rel_type}: {rel['from_table']}.{rel['from_column']} → {rel['to_table']}.{rel['to_column']}")
        if 'constraint_name' in rel:
            print(f"      Constraint: {rel['constraint_name']}")
    print()
    
    print("🔍 POTENTIAL RELATIONSHIPS:")
    for rel in schema_profile.potential_relationships:
        print(f"   Potential FK: {rel['from_table']}.{rel['from_column']} → {rel['to_table']}.{rel['to_column']}")
        print(f"      Confidence: {rel['confidence']}")
        print(f"      Reason: {rel['reason']}")
    print()
    
    # Export comprehensive schema profile
    output_file = "comprehensive_schema_profile.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(asdict(schema_profile), f, indent=2, default=str)
    
    file_size = Path(output_file).stat().st_size
    print(f"💾 COMPREHENSIVE SCHEMA PROFILE EXPORTED:")
    print(f"   File: {output_file}")
    print(f"   Size: {file_size:,} bytes")
    print()
    
    # Show comprehensive statistics
    total_patterns = sum(len(col.detected_patterns) for table in schema_profile.tables for col in table.columns)
    total_fks = sum(len(table.foreign_keys) for table in schema_profile.tables)
    total_potential_fks = sum(len(table.potential_fk_candidates) for table in schema_profile.tables)
    
    print("📊 COMPREHENSIVE STATISTICS:")
    print(f"   Tables: {schema_profile.total_tables}")
    print(f"   Columns: {schema_profile.total_columns}")
    print(f"   Detected Patterns: {total_patterns}")
    print(f"   Foreign Keys: {total_fks}")
    print(f"   Potential Foreign Keys: {total_potential_fks}")
    print(f"   Cross-table Relationships: {len(schema_profile.cross_table_relationships)}")
    print(f"   Self-referencing Tables: {sum(1 for t in schema_profile.tables if t.self_referencing_columns)}")
    print()
    
    print("🎯 RELATIONSHIP FEATURES DEMONSTRATED:")
    print("   ✅ Foreign key references with constraint names")
    print("   ✅ Cross-table relationship mapping")
    print("   ✅ Potential relationship identification")
    print("   ✅ Self-referencing relationships")
    print("   ✅ Potential FK candidates with confidence scores")
    print("   ✅ Comprehensive relationship metadata")
    print()
    
    print("=" * 80)
    print("COMPREHENSIVE PATTERN RECOGNITION WITH RELATIONSHIPS DEMO COMPLETED")
    print("=" * 80)


if __name__ == "__main__":
    demonstrate_comprehensive_patterns() 