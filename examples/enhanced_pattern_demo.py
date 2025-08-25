#!/usr/bin/env python3
"""
Enhanced Pattern Recognition Demo

This script demonstrates the enhanced pattern recognition capabilities
with the new field_patterns.json structure, showing detailed pattern
detection with confidence scores, priorities, and sensitivity levels.
"""

import json
from pathlib import Path
import sys
from dataclasses import asdict

# Add the src directory to the path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from profiler.pattern_recognizer import FieldPatternRecognizer
from profiler.schema_models import ColumnProfile, TableProfile, SchemaProfile


def create_enhanced_sample_data():
    """Create sample data that showcases the new pattern recognition features."""
    
    # Create sample schema with healthcare-focused data
    schema_profile = SchemaProfile(
        database_name="enhanced_healthcare_db",
        schema_name="public",
        database_type="postgresql",
        profiling_timestamp="2025-08-21T10:00:00",
        total_tables=2,
        total_columns=12
    )
    
    # Patient table with comprehensive healthcare patterns
    patient_columns = [
        ColumnProfile(
            name="patient_id",
            data_type="int",
            is_nullable=False,
            is_primary_key=True,
            sample_values=[12345, 67890, 11111, 22222, 33333],
            detected_patterns=[]
        ),
        ColumnProfile(
            name="mrn",
            data_type="varchar",
            max_length=20,
            is_nullable=False,
            is_unique=True,
            sample_values=["MRN001234", "MRN005678", "MRN009012", "MRN003456", "MRN007890"],
            detected_patterns=[]
        ),
        ColumnProfile(
            name="first_name",
            data_type="varchar",
            max_length=50,
            is_nullable=False,
            sample_values=["John", "Maria", "David", "Sarah", "Michael"],
            detected_patterns=[]
        ),
        ColumnProfile(
            name="last_name",
            data_type="varchar",
            max_length=50,
            is_nullable=False,
            sample_values=["Smith", "Garcia", "Johnson", "Williams", "Brown"],
            detected_patterns=[]
        ),
        ColumnProfile(
            name="date_of_birth",
            data_type="date",
            is_nullable=False,
            sample_values=["1985-03-15", "1992-07-22", "1978-11-08", "1995-01-30", "1983-09-12"],
            detected_patterns=[]
        ),
        ColumnProfile(
            name="email_address",
            data_type="varchar",
            max_length=255,
            is_nullable=True,
            sample_values=["john.smith@email.com", "maria.garcia@healthcare.org", "david.j@clinic.net", "sarah.w@hospital.edu", "michael.b@medical.com"],
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
            name="insurance_member_id",
            data_type="varchar",
            max_length=30,
            is_nullable=True,
            sample_values=["INS123456789", "MEM987654321", "POL456789123", "PLN789123456", "COV321654987"],
            detected_patterns=[]
        )
    ]
    
    # Provider table with NPI and healthcare identifiers
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
            name="appointment_date",
            data_type="timestamp",
            is_nullable=False,
            sample_values=["2024-08-21 09:30:00", "2024-08-21 14:15:00", "2024-08-22 10:45:00", "2024-08-22 16:30:00", "2024-08-23 08:00:00"],
            detected_patterns=[]
        ),
        ColumnProfile(
            name="status",
            data_type="varchar",
            max_length=20,
            is_nullable=False,
            sample_values=["active", "inactive", "pending", "completed", "cancelled"],
            detected_patterns=[]
        )
    ]
    
    # Create table profiles
    patient_table = TableProfile(
        name="patients",
        schema="public",
        table_type="BASE TABLE",
        table_comment="Patient demographic and contact information",
        estimated_row_count=50000,
        total_columns=len(patient_columns),
        columns=patient_columns
    )
    
    provider_table = TableProfile(
        name="providers",
        schema="public",
        table_type="BASE TABLE",
        table_comment="Healthcare provider information with NPI identifiers",
        estimated_row_count=1500,
        total_columns=len(provider_columns),
        columns=provider_columns
    )
    
    schema_profile.tables = [patient_table, provider_table]
    
    return schema_profile


def demonstrate_enhanced_patterns():
    """Demonstrate the enhanced pattern recognition capabilities."""
    
    print("=" * 80)
    print("ENHANCED PATTERN RECOGNITION DEMO")
    print("=" * 80)
    print()
    
    # Initialize pattern recognizer
    recognizer = FieldPatternRecognizer()
    
    print("🔧 PATTERN RECOGNIZER CONFIGURATION:")
    print(f"   Loaded patterns: {len(recognizer.patterns)}")
    print(f"   Available categories: {sorted(recognizer.get_pattern_categories())}")
    print()
    
    # Show sensitivity levels
    sensitivity_levels = recognizer.get_sensitivity_levels()
    print("🔒 SENSITIVITY LEVELS:")
    for level, info in sensitivity_levels.items():
        print(f"   • {level}: {info['description']}")
        print(f"     Examples: {', '.join(info['examples'][:3])}...")
        print(f"     Access Control: {info['access_control']}")
    print()
    
    # Create sample data
    schema_profile = create_enhanced_sample_data()
    
    print("📊 ENHANCED PATTERN DETECTION RESULTS:")
    print()
    
    # Process each table
    for table in schema_profile.tables:
        print(f"📋 TABLE: {table.name}")
        print(f"   Comment: {table.table_comment}")
        print()
        
        # Process each column
        for column in table.columns:
            print(f"   📝 COLUMN: {column.name} ({column.data_type})")
            
            # Detect patterns with confidence
            detected = recognizer.detect_patterns_with_confidence(
                column.sample_values, 
                field_name=column.name
            )
            
            if detected:
                print(f"      🎯 DETECTED PATTERNS:")
                for i, pattern in enumerate(detected, 1):
                    print(f"         {i}. {pattern['pattern_key']}")
                    print(f"            Confidence: {pattern['confidence']:.3f}")
                    print(f"            Priority: {pattern['priority']}")
                    print(f"            Category: {pattern['semantic_category']}")
                    print(f"            Sensitivity: {pattern['sensitivity_level']}")
                    print(f"            Data Types: {', '.join(pattern['data_types'])}")
                    print(f"            Field Match: {pattern['field_name_match']}")
                    print(f"            Match Ratio: {pattern['match_ratio']:.3f} ({pattern['matches']}/{pattern['sample_size']})")
                    if i < len(detected):
                        print()
                
                # Update column with detected patterns
                column.detected_patterns = [p['pattern_key'] for p in detected]
            else:
                print(f"      ❌ NO PATTERNS DETECTED")
            
            print(f"      📋 Sample Values: {', '.join(str(v) for v in column.sample_values[:3])}...")
            print()
        
        print()
    
    # Show pattern statistics by sensitivity level
    print("📈 PATTERN STATISTICS BY SENSITIVITY LEVEL:")
    for level in ['phi_high', 'phi_limited', 'confidential', 'internal', 'public']:
        patterns = recognizer.get_pattern_by_sensitivity(level)
        if patterns:
            print(f"   • {level}: {len(patterns)} patterns")
            for pattern_key in patterns[:3]:  # Show first 3
                pattern_info = recognizer.get_pattern_info(pattern_key)
                category = pattern_info.get('semantic_category', 'unknown')
                print(f"     - {pattern_key} ({category})")
            if len(patterns) > 3:
                print(f"     ... and {len(patterns) - 3} more")
    print()
    
    # Export enhanced schema profile
    output_file = "enhanced_schema_profile.json"
    from dataclasses import asdict
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(asdict(schema_profile), f, indent=2, default=str)
    
    file_size = Path(output_file).stat().st_size
    print(f"💾 ENHANCED SCHEMA PROFILE EXPORTED:")
    print(f"   File: {output_file}")
    print(f"   Size: {file_size:,} bytes")
    print()
    
    # Show key improvements
    print("🚀 KEY IMPROVEMENTS WITH NEW PATTERN STRUCTURE:")
    print("   ✓ Priority-based pattern matching")
    print("   ✓ Confidence scoring with base confidence + match ratio")
    print("   ✓ Sensitivity level classification")
    print("   ✓ Semantic category grouping")
    print("   ✓ Wildcard pattern matching (*pattern*)")
    print("   ✓ Valid values matching for status fields")
    print("   ✓ Enhanced field name matching")
    print("   ✓ Data type awareness")
    print("   ✓ Healthcare-specific pattern library")
    print("   ✓ Entity relationship context")
    print()
    
    print("=" * 80)
    print("ENHANCED PATTERN RECOGNITION DEMO COMPLETED")
    print("=" * 80)


if __name__ == "__main__":
    demonstrate_enhanced_patterns() 