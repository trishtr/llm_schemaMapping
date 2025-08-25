#!/usr/bin/env python3
"""
Corrected Pattern Recognition Demo with Proper Ordinal Positions

This script demonstrates proper ordinal_position values in ColumnProfile objects.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from profiler.simple_pattern_recognizer import SimplePatternRecognizer
from profiler.schema_models import ColumnProfile, TableProfile, SchemaProfile
from dataclasses import asdict
import json


def create_corrected_test_data():
    """Create test data with proper ordinal positions."""
    
    # 1. PATIENTS TABLE with correct ordinal positions
    patient_columns = [
        ColumnProfile(
            name="patient_id",
            data_type="bigint",
            is_nullable=False,
            is_primary_key=True,
            ordinal_position=1,  # Correct ordinal position
            sample_values=[100001, 100002, 100003, 100004, 100005],
            detected_patterns=[]
        ),
        ColumnProfile(
            name="primary_provider_id",
            data_type="int",
            is_nullable=True,
            is_foreign_key=True,
            is_indexed=True,
            ordinal_position=2,  # Correct ordinal position
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
            ordinal_position=3,  # Correct ordinal position
            sample_values=[101, 102, 101, 103, 102],
            detected_patterns=[]
        ),
        ColumnProfile(
            name="email_address", 
            data_type="varchar",
            max_length=255,
            is_nullable=True,
            ordinal_position=4,  # Correct ordinal position
            sample_values=["john@example.com", "jane@test.org", "bob@clinic.net", "alice@hospital.edu", "charlie@medical.com"],
            detected_patterns=[]
        ),
        ColumnProfile(
            name="phone_number",
            data_type="varchar", 
            max_length=20,
            is_nullable=True,
            ordinal_position=5,  # Correct ordinal position
            sample_values=["555-123-4567", "555-987-6543", "555-456-7890", "555-234-5678", "555-345-6789"],
            detected_patterns=[]
        ),
        ColumnProfile(
            name="status",
            data_type="varchar",
            max_length=20,
            is_nullable=False,
            ordinal_position=6,  # Correct ordinal position
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
    
    # 2. PROVIDERS TABLE with correct ordinal positions
    provider_columns = [
        ColumnProfile(
            name="provider_id",
            data_type="int",
            is_nullable=False,
            is_primary_key=True,
            ordinal_position=1,  # Correct ordinal position
            sample_values=[1001, 1002, 1003, 1004, 1005],
            detected_patterns=[]
        ),
        ColumnProfile(
            name="npi",
            data_type="varchar",
            max_length=10,
            is_nullable=False,
            is_unique=True,
            ordinal_position=2,  # Correct ordinal position
            sample_values=["1234567890", "9876543210", "5555666677", "1111222233", "9999888877"],
            detected_patterns=[]
        ),
        ColumnProfile(
            name="department_id",
            data_type="int",
            is_nullable=True,
            is_foreign_key=True,
            is_indexed=True,
            ordinal_position=3,  # Correct ordinal position
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
            ordinal_position=4,  # Correct ordinal position
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
    
    # 3. DEPARTMENTS TABLE with correct ordinal positions
    department_columns = [
        ColumnProfile(
            name="dept_id",
            data_type="int",
            is_nullable=False,
            is_primary_key=True,
            ordinal_position=1,  # Correct ordinal position
            sample_values=[101, 102, 103, 104, 105],
            detected_patterns=[]
        ),
        ColumnProfile(
            name="dept_name",
            data_type="varchar",
            max_length=100,
            is_nullable=False,
            ordinal_position=2,  # Correct ordinal position
            sample_values=["Cardiology", "Neurology", "Emergency", "Radiology", "Pediatrics"],
            detected_patterns=[]
        ),
        ColumnProfile(
            name="manager_id",
            data_type="int",
            is_nullable=True,
            is_indexed=True,
            ordinal_position=3,  # Correct ordinal position
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
        database_name="corrected_healthcare_db",
        schema_name="public",
        database_type="postgresql", 
        profiling_timestamp="2025-08-21T16:45:00",
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


def demonstrate_corrected_ordinals():
    """Demonstrate corrected ordinal positions."""
    
    print("=" * 80)
    print("CORRECTED ORDINAL POSITIONS DEMO")
    print("=" * 80)
    print()
    
    # Initialize simple pattern recognizer
    recognizer = SimplePatternRecognizer()
    
    # Create corrected test data
    schema_profile = create_corrected_test_data()
    
    print(f"📊 SCHEMA: {schema_profile.database_name}")
    print()
    
    # Process each table and show ordinal positions
    for table in schema_profile.tables:
        print(f"📋 TABLE: {table.name}")
        print(f"   Total Columns: {table.total_columns}")
        print()
        
        # Sort columns by ordinal position to verify ordering
        sorted_columns = sorted(table.columns, key=lambda x: x.ordinal_position)
        
        for column in sorted_columns:
            # Detect patterns
            detected = recognizer.detect_patterns(column.sample_values, field_name=column.name)
            column.detected_patterns = detected
            
            print(f"   📝 COLUMN #{column.ordinal_position}: {column.name} ({column.data_type})")
            
            if column.is_foreign_key and column.foreign_key_reference:
                fk_ref = column.foreign_key_reference
                print(f"      🔗 FOREIGN KEY → {fk_ref['referenced_table']}.{fk_ref['referenced_column']}")
            
            if detected:
                print(f"      ✅ PATTERNS: {', '.join(detected)}")
            else:
                print(f"      ❌ NO PATTERNS")
            
            print(f"      📋 Sample: {', '.join(str(v) for v in column.sample_values[:3])}...")
            print()
        
        print()
    
    # Export corrected schema profile
    output_file = "corrected_ordinal_schema.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(asdict(schema_profile), f, indent=2, default=str)
    
    file_size = Path(output_file).stat().st_size
    print(f"💾 CORRECTED SCHEMA PROFILE EXPORTED:")
    print(f"   File: {output_file}")
    print(f"   Size: {file_size:,} bytes")
    print()
    
    print("🎯 ORDINAL POSITION VERIFICATION:")
    for table in schema_profile.tables:
        ordinals = [col.ordinal_position for col in table.columns]
        expected = list(range(1, len(table.columns) + 1))
        is_correct = ordinals == expected
        
        print(f"   Table '{table.name}': {ordinals} → {'✅ CORRECT' if is_correct else '❌ INCORRECT'}")
    
    print()
    print("=" * 80)
    print("CORRECTED ORDINAL POSITIONS DEMO COMPLETED")
    print("=" * 80)


if __name__ == "__main__":
    demonstrate_corrected_ordinals() 