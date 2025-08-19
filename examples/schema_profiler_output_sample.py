"""
Schema Profiler Output Sample

This script demonstrates the comprehensive output structure of the UnifiedProfiler
with realistic sample data, showing all the information that gets collected during
schema profiling for LLM processing.
"""

import sys
import os
import json
from datetime import datetime
from dataclasses import asdict

# Add the src directory to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from profiler import (
    SchemaProfile, 
    TableProfile, 
    ColumnProfile, 
    ProfilingStrategy
)


def create_sample_schema_profile():
    """Create a comprehensive sample schema profile with realistic data."""
    
    # Sample Column Profiles for users table
    users_columns = [
        ColumnProfile(
            name="user_id",
            data_type="int",
            is_nullable=False,
            is_primary_key=True,
            is_foreign_key=False,
            is_unique=True,
            is_indexed=True,
            max_length=None,
            default_value=None,
            column_comment="Primary key for users table",
            ordinal_position=1,
            detected_patterns=["provider_identifiers.npi"],
            sample_values=[1001, 1002, 1003, 1004, 1005],
            foreign_key_reference=None
        ),
        ColumnProfile(
            name="email",
            data_type="varchar",
            is_nullable=False,
            is_primary_key=False,
            is_foreign_key=False,
            is_unique=True,
            is_indexed=True,
            max_length=255,
            default_value=None,
            column_comment="User email address",
            ordinal_position=2,
            detected_patterns=["contact_information.email"],
            sample_values=[
                "john.doe@hospital.org", 
                "jane.smith@clinic.com", 
                "bob.wilson@healthcare.net",
                "alice.brown@medical.org",
                "charlie.davis@health.com"
            ],
            foreign_key_reference=None
        ),
        ColumnProfile(
            name="first_name",
            data_type="varchar",
            is_nullable=False,
            is_primary_key=False,
            is_foreign_key=False,
            is_unique=False,
            is_indexed=False,
            max_length=100,
            default_value=None,
            column_comment="User first name",
            ordinal_position=3,
            detected_patterns=[],
            sample_values=["John", "Jane", "Bob", "Alice", "Charlie"],
            foreign_key_reference=None
        ),
        ColumnProfile(
            name="last_name",
            data_type="varchar",
            is_nullable=False,
            is_primary_key=False,
            is_foreign_key=False,
            is_unique=False,
            is_indexed=True,
            max_length=100,
            default_value=None,
            column_comment="User last name",
            ordinal_position=4,
            detected_patterns=[],
            sample_values=["Doe", "Smith", "Wilson", "Brown", "Davis"],
            foreign_key_reference=None
        ),
        ColumnProfile(
            name="department_id",
            data_type="int",
            is_nullable=True,
            is_primary_key=False,
            is_foreign_key=True,
            is_unique=False,
            is_indexed=True,
            max_length=None,
            default_value=None,
            column_comment="Reference to department",
            ordinal_position=5,
            detected_patterns=[],
            sample_values=[101, 102, 101, 103, 102],
            foreign_key_reference={
                "table": "departments",
                "column": "dept_id",
                "constraint": "fk_users_department"
            }
        ),
        ColumnProfile(
            name="created_at",
            data_type="timestamp",
            is_nullable=False,
            is_primary_key=False,
            is_foreign_key=False,
            is_unique=False,
            is_indexed=True,
            max_length=None,
            default_value="CURRENT_TIMESTAMP",
            column_comment="Record creation timestamp",
            ordinal_position=6,
            detected_patterns=[],
            sample_values=[
                "2024-01-15 10:30:00",
                "2024-01-16 14:22:15",
                "2024-01-17 09:45:30",
                "2024-01-18 16:18:45",
                "2024-01-19 11:55:20"
            ],
            foreign_key_reference=None
        )
    ]
    
    # Sample users table profile
    users_table = TableProfile(
        name="users",
        schema="public",
        table_type="BASE TABLE",
        table_comment="System users and healthcare providers",
        estimated_row_count=15420,
        columns=users_columns,
        primary_keys=["user_id"],
        foreign_keys=[
            {
                "column_name": "department_id",
                "referenced_table": "departments",
                "referenced_column": "dept_id",
                "constraint_name": "fk_users_department"
            }
        ],
        indexes=[
            {
                "index_name": "PRIMARY",
                "column_name": "user_id",
                "is_unique": True
            },
            {
                "index_name": "idx_users_email",
                "column_name": "email",
                "is_unique": True
            },
            {
                "index_name": "idx_users_last_name",
                "column_name": "last_name",
                "is_unique": False
            },
            {
                "index_name": "idx_users_department",
                "column_name": "department_id",
                "is_unique": False
            },
            {
                "index_name": "idx_users_created",
                "column_name": "created_at",
                "is_unique": False
            }
        ],
        sample_data=[
            {
                "user_id": 1001,
                "email": "john.doe@hospital.org",
                "first_name": "John",
                "last_name": "Doe",
                "department_id": 101,
                "created_at": "2024-01-15 10:30:00"
            },
            {
                "user_id": 1002,
                "email": "jane.smith@clinic.com",
                "first_name": "Jane",
                "last_name": "Smith",
                "department_id": 102,
                "created_at": "2024-01-16 14:22:15"
            },
            {
                "user_id": 1003,
                "email": "bob.wilson@healthcare.net",
                "first_name": "Bob",
                "last_name": "Wilson",
                "department_id": 101,
                "created_at": "2024-01-17 09:45:30"
            },
            {
                "user_id": 1004,
                "email": "alice.brown@medical.org",
                "first_name": "Alice",
                "last_name": "Brown",
                "department_id": 103,
                "created_at": "2024-01-18 16:18:45"
            },
            {
                "user_id": 1005,
                "email": "charlie.davis@health.com",
                "first_name": "Charlie",
                "last_name": "Davis",
                "department_id": 102,
                "created_at": "2024-01-19 11:55:20"
            }
        ],
        self_referencing_columns=[],
        potential_fk_candidates=[
            {
                "column_name": "department_id",
                "data_type": "int",
                "reason": "Matches pattern: .*_id$"
            }
        ]
    )
    
    # Sample departments table
    departments_columns = [
        ColumnProfile(
            name="dept_id",
            data_type="int",
            is_nullable=False,
            is_primary_key=True,
            is_unique=True,
            is_indexed=True,
            ordinal_position=1,
            sample_values=[101, 102, 103, 104, 105]
        ),
        ColumnProfile(
            name="dept_name",
            data_type="varchar",
            is_nullable=False,
            max_length=200,
            ordinal_position=2,
            sample_values=["Cardiology", "Neurology", "Emergency", "Pediatrics", "Radiology"]
        ),
        ColumnProfile(
            name="manager_id",
            data_type="int",
            is_nullable=True,
            is_foreign_key=True,
            is_indexed=True,
            ordinal_position=3,
            sample_values=[1001, 1003, 1002, 1004, None],
            foreign_key_reference={
                "table": "users",
                "column": "user_id",
                "constraint": "fk_dept_manager"
            }
        )
    ]
    
    departments_table = TableProfile(
        name="departments",
        schema="public",
        table_type="BASE TABLE",
        table_comment="Hospital departments",
        estimated_row_count=25,
        columns=departments_columns,
        primary_keys=["dept_id"],
        foreign_keys=[
            {
                "column_name": "manager_id",
                "referenced_table": "users",
                "referenced_column": "user_id",
                "constraint_name": "fk_dept_manager"
            }
        ],
        indexes=[
            {
                "index_name": "PRIMARY",
                "column_name": "dept_id",
                "is_unique": True
            }
        ],
        sample_data=[
            {"dept_id": 101, "dept_name": "Cardiology", "manager_id": 1001},
            {"dept_id": 102, "dept_name": "Neurology", "manager_id": 1003},
            {"dept_id": 103, "dept_name": "Emergency", "manager_id": 1002},
            {"dept_id": 104, "dept_name": "Pediatrics", "manager_id": 1004},
            {"dept_id": 105, "dept_name": "Radiology", "manager_id": None}
        ],
        self_referencing_columns=[],
        potential_fk_candidates=[
            {
                "column_name": "manager_id",
                "data_type": "int",
                "reason": "Matches pattern: .*_id$"
            }
        ]
    )
    
    # Sample appointments table
    appointments_columns = [
        ColumnProfile(
            name="appointment_id",
            data_type="int",
            is_nullable=False,
            is_primary_key=True,
            is_unique=True,
            is_indexed=True,
            ordinal_position=1,
            sample_values=[2001, 2002, 2003, 2004, 2005]
        ),
        ColumnProfile(
            name="patient_id",
            data_type="int",
            is_nullable=False,
            is_foreign_key=True,
            is_indexed=True,
            ordinal_position=2,
            sample_values=[3001, 3002, 3003, 3004, 3005],
            foreign_key_reference={
                "table": "patients",
                "column": "patient_id",
                "constraint": "fk_appointments_patient"
            }
        ),
        ColumnProfile(
            name="provider_id",
            data_type="int",
            is_nullable=False,
            is_foreign_key=True,
            is_indexed=True,
            ordinal_position=3,
            sample_values=[1001, 1002, 1003, 1001, 1004],
            foreign_key_reference={
                "table": "users",
                "column": "user_id",
                "constraint": "fk_appointments_provider"
            }
        ),
        ColumnProfile(
            name="appointment_date",
            data_type="date",
            is_nullable=False,
            is_indexed=True,
            ordinal_position=4,
            sample_values=["2024-02-15", "2024-02-16", "2024-02-17", "2024-02-18", "2024-02-19"]
        ),
        ColumnProfile(
            name="status",
            data_type="varchar",
            is_nullable=False,
            max_length=50,
            default_value="scheduled",
            ordinal_position=5,
            sample_values=["scheduled", "completed", "cancelled", "scheduled", "in_progress"]
        )
    ]
    
    appointments_table = TableProfile(
        name="appointments",
        schema="public",
        table_type="BASE TABLE",
        table_comment="Patient appointments",
        estimated_row_count=45680,
        columns=appointments_columns,
        primary_keys=["appointment_id"],
        foreign_keys=[
            {
                "column_name": "patient_id",
                "referenced_table": "patients",
                "referenced_column": "patient_id",
                "constraint_name": "fk_appointments_patient"
            },
            {
                "column_name": "provider_id",
                "referenced_table": "users",
                "referenced_column": "user_id",
                "constraint_name": "fk_appointments_provider"
            }
        ],
        indexes=[
            {
                "index_name": "PRIMARY",
                "column_name": "appointment_id",
                "is_unique": True
            },
            {
                "index_name": "idx_appointments_patient",
                "column_name": "patient_id",
                "is_unique": False
            },
            {
                "index_name": "idx_appointments_provider",
                "column_name": "provider_id",
                "is_unique": False
            },
            {
                "index_name": "idx_appointments_date",
                "column_name": "appointment_date",
                "is_unique": False
            }
        ],
        sample_data=[
            {
                "appointment_id": 2001,
                "patient_id": 3001,
                "provider_id": 1001,
                "appointment_date": "2024-02-15",
                "status": "scheduled"
            },
            {
                "appointment_id": 2002,
                "patient_id": 3002,
                "provider_id": 1002,
                "appointment_date": "2024-02-16",
                "status": "completed"
            },
            {
                "appointment_id": 2003,
                "patient_id": 3003,
                "provider_id": 1003,
                "appointment_date": "2024-02-17",
                "status": "cancelled"
            },
            {
                "appointment_id": 2004,
                "patient_id": 3004,
                "provider_id": 1001,
                "appointment_date": "2024-02-18",
                "status": "scheduled"
            },
            {
                "appointment_id": 2005,
                "patient_id": 3005,
                "provider_id": 1004,
                "appointment_date": "2024-02-19",
                "status": "in_progress"
            }
        ],
        self_referencing_columns=[],
        potential_fk_candidates=[
            {
                "column_name": "patient_id",
                "data_type": "int",
                "reason": "Matches pattern: .*_id$"
            },
            {
                "column_name": "provider_id",
                "data_type": "int",
                "reason": "Matches pattern: .*_id$"
            }
        ]
    )
    
    # Create comprehensive schema profile
    schema_profile = SchemaProfile(
        database_name="healthcare_db",
        schema_name="public",
        database_type="postgresql",
        profiling_timestamp=datetime.now().isoformat(),
        total_tables=3,
        total_columns=14,
        tables=[users_table, departments_table, appointments_table],
        cross_table_relationships=[
            {
                "type": "foreign_key",
                "from_table": "users",
                "from_column": "department_id",
                "to_table": "departments",
                "to_column": "dept_id",
                "constraint_name": "fk_users_department"
            },
            {
                "type": "foreign_key",
                "from_table": "departments",
                "from_column": "manager_id",
                "to_table": "users",
                "to_column": "user_id",
                "constraint_name": "fk_dept_manager"
            },
            {
                "type": "foreign_key",
                "from_table": "appointments",
                "from_column": "patient_id",
                "to_table": "patients",
                "to_column": "patient_id",
                "constraint_name": "fk_appointments_patient"
            },
            {
                "type": "foreign_key",
                "from_table": "appointments",
                "from_column": "provider_id",
                "to_table": "users",
                "to_column": "user_id",
                "constraint_name": "fk_appointments_provider"
            }
        ],
        potential_relationships=[
            {
                "type": "potential_foreign_key",
                "from_table": "users",
                "from_column": "department_id",
                "to_table": "departments",
                "to_column": "dept_id",
                "reason": "Column name pattern and data type match",
                "confidence": "high"
            },
            {
                "type": "potential_foreign_key",
                "from_table": "appointments",
                "from_column": "patient_id",
                "to_table": "patients",
                "to_column": "patient_id",
                "reason": "Column name pattern and data type match",
                "confidence": "high"
            }
        ],
        pattern_summary={
            "provider_identifiers.npi": 1,
            "contact_information.email": 1
        }
    )
    
    return schema_profile


def print_schema_profile_summary(schema_profile: SchemaProfile):
    """Print a human-readable summary of the schema profile."""
    
    print("=" * 80)
    print("SCHEMA PROFILER OUTPUT SAMPLE")
    print("=" * 80)
    
    # Database Information
    print(f"\n📊 DATABASE INFORMATION:")
    print(f"   Database Name: {schema_profile.database_name}")
    print(f"   Schema Name: {schema_profile.schema_name}")
    print(f"   Database Type: {schema_profile.database_type}")
    print(f"   Profiling Timestamp: {schema_profile.profiling_timestamp}")
    print(f"   Total Tables: {schema_profile.total_tables}")
    print(f"   Total Columns: {schema_profile.total_columns}")
    
    # Table Details
    print(f"\n📋 TABLE DETAILS:")
    for i, table in enumerate(schema_profile.tables, 1):
        print(f"\n   {i}. Table: {table.name}")
        print(f"      Schema: {table.schema}")
        print(f"      Type: {table.table_type}")
        print(f"      Comment: {table.table_comment or 'N/A'}")
        print(f"      Estimated Rows: {table.estimated_row_count:,}")
        print(f"      Columns: {len(table.columns)}")
        print(f"      Primary Keys: {table.primary_keys}")
        print(f"      Foreign Keys: {len(table.foreign_keys)}")
        print(f"      Indexes: {len(table.indexes)}")
        print(f"      Self-Referencing: {table.self_referencing_columns}")
        print(f"      Potential FK Candidates: {len(table.potential_fk_candidates)}")
        
        # Column details
        print(f"      \n      📝 COLUMNS:")
        for col in table.columns:
            flags = []
            if col.is_primary_key: flags.append("PK")
            if col.is_foreign_key: flags.append("FK")
            if col.is_unique: flags.append("UNIQUE")
            if col.is_indexed: flags.append("INDEXED")
            if not col.is_nullable: flags.append("NOT NULL")
            
            flag_str = f" [{', '.join(flags)}]" if flags else ""
            max_len_str = f"({col.max_length})" if col.max_length else ""
            
            print(f"         • {col.name}: {col.data_type}{max_len_str}{flag_str}")
            
            if col.column_comment:
                print(f"           Comment: {col.column_comment}")
            
            if col.detected_patterns:
                print(f"           Patterns: {', '.join(col.detected_patterns)}")
            
            if col.sample_values:
                sample_str = ', '.join([str(v)[:30] for v in col.sample_values[:3] if v is not None])
                print(f"           Sample: {sample_str}...")
            
            if col.foreign_key_reference:
                ref = col.foreign_key_reference
                print(f"           References: {ref['table']}.{ref['column']} ({ref['constraint']})")
        
        # Sample data
        if table.sample_data:
            print(f"      \n      📄 SAMPLE DATA (first 2 rows):")
            for i, row in enumerate(table.sample_data[:2], 1):
                print(f"         Row {i}: {dict(list(row.items())[:4])}...")
    
    # Relationships
    print(f"\n🔗 CROSS-TABLE RELATIONSHIPS:")
    if schema_profile.cross_table_relationships:
        for i, rel in enumerate(schema_profile.cross_table_relationships, 1):
            print(f"   {i}. {rel['from_table']}.{rel['from_column']} → {rel['to_table']}.{rel['to_column']}")
            print(f"      Type: {rel['type']}")
            print(f"      Constraint: {rel['constraint_name']}")
    else:
        print("   No explicit relationships found")
    
    # Potential Relationships
    print(f"\n🔍 POTENTIAL RELATIONSHIPS:")
    if schema_profile.potential_relationships:
        for i, rel in enumerate(schema_profile.potential_relationships, 1):
            print(f"   {i}. {rel['from_table']}.{rel['from_column']} → {rel['to_table']}.{rel['to_column']}")
            print(f"      Type: {rel['type']}")
            print(f"      Reason: {rel['reason']}")
            print(f"      Confidence: {rel['confidence']}")
    else:
        print("   No potential relationships identified")
    
    # Pattern Summary
    print(f"\n🎯 DETECTED PATTERNS:")
    if schema_profile.pattern_summary:
        for pattern, count in schema_profile.pattern_summary.items():
            print(f"   • {pattern}: {count} occurrence(s)")
    else:
        print("   No patterns detected")


def export_schema_profile_json(schema_profile: SchemaProfile, filename: str = "sample_schema_profile.json"):
    """Export the schema profile to JSON for LLM processing."""
    
    # Convert to dictionary
    profile_dict = asdict(schema_profile)
    
    # Save to JSON file
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(profile_dict, f, indent=2, ensure_ascii=False, default=str)
    
    print(f"\n💾 EXPORT INFORMATION:")
    print(f"   JSON file: {filename}")
    print(f"   File size: {os.path.getsize(filename):,} bytes")
    print(f"   JSON structure: {len(json.dumps(profile_dict))} characters")
    
    return profile_dict


def analyze_schema_profile_structure(schema_profile: SchemaProfile):
    """Analyze the structure and content of the schema profile."""
    
    print(f"\n🔬 SCHEMA PROFILE ANALYSIS:")
    
    # Data model analysis
    total_constraints = sum(
        len(table.primary_keys) + len(table.foreign_keys) + len(table.indexes)
        for table in schema_profile.tables
    )
    
    total_patterns = sum(schema_profile.pattern_summary.values()) if schema_profile.pattern_summary else 0
    
    total_sample_rows = sum(
        len(table.sample_data) for table in schema_profile.tables
    )
    
    print(f"   📊 Statistics:")
    print(f"      Tables: {schema_profile.total_tables}")
    print(f"      Columns: {schema_profile.total_columns}")
    print(f"      Constraints: {total_constraints}")
    print(f"      Relationships: {len(schema_profile.cross_table_relationships)}")
    print(f"      Potential Relationships: {len(schema_profile.potential_relationships)}")
    print(f"      Pattern Detections: {total_patterns}")
    print(f"      Sample Data Rows: {total_sample_rows}")
    
    print(f"   \n📋 Data Quality:")
    tables_with_comments = sum(1 for table in schema_profile.tables if table.table_comment)
    columns_with_comments = sum(
        sum(1 for col in table.columns if col.column_comment)
        for table in schema_profile.tables
    )
    columns_with_patterns = sum(
        sum(1 for col in table.columns if col.detected_patterns)
        for table in schema_profile.tables
    )
    
    print(f"      Tables with comments: {tables_with_comments}/{schema_profile.total_tables}")
    print(f"      Columns with comments: {columns_with_comments}/{schema_profile.total_columns}")
    print(f"      Columns with patterns: {columns_with_patterns}/{schema_profile.total_columns}")
    
    print(f"   \n🎯 LLM Processing Readiness:")
    print(f"      ✓ Structured data models")
    print(f"      ✓ Sample data for context")
    print(f"      ✓ Relationship mapping")
    print(f"      ✓ Pattern recognition")
    print(f"      ✓ Metadata enrichment")
    print(f"      ✓ JSON serializable")


def main():
    """Main function to demonstrate schema profiler output."""
    
    try:
        # Create sample schema profile
        schema_profile = create_sample_schema_profile()
        
        # Print comprehensive summary
        print_schema_profile_summary(schema_profile)
        
        # Export to JSON
        profile_dict = export_schema_profile_json(schema_profile)
        
        # Analyze structure
        analyze_schema_profile_structure(schema_profile)
        
        print(f"\n{'='*80}")
        print("SCHEMA PROFILER OUTPUT SAMPLE COMPLETED")
        print(f"{'='*80}")
        
        print(f"\n🎯 KEY FEATURES DEMONSTRATED:")
        print(f"✓ Complete table metadata (columns, types, constraints)")
        print(f"✓ Primary and foreign key relationships")
        print(f"✓ Index information and performance hints")
        print(f"✓ Sample data for context understanding")
        print(f"✓ Pattern detection (healthcare, email, etc.)")
        print(f"✓ Self-referencing relationship detection")
        print(f"✓ Potential foreign key candidate identification")
        print(f"✓ Cross-table relationship analysis")
        print(f"✓ Rich metadata with comments and descriptions")
        print(f"✓ JSON export for LLM processing")
        print(f"✓ Comprehensive data quality metrics")
        
        print(f"\n📝 USAGE FOR LLM PROCESSING:")
        print(f"• Load the JSON file into your LLM pipeline")
        print(f"• Use table/column metadata for schema understanding")
        print(f"• Leverage sample data for data type inference")
        print(f"• Apply relationship info for join optimization")
        print(f"• Use pattern detection for data validation")
        print(f"• Reference comments for business context")
        
    except Exception as e:
        print(f"✗ Error creating schema profile sample: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main() 